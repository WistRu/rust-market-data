use anyhow::{Context, Result, anyhow, ensure};
use futures::{SinkExt, StreamExt};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcManagedRuntimePolicy,
    MexcPublicRuntimeConfig,
};
use serde::Serialize;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[derive(Clone)]
struct ProxyControl {
    close_generation: Arc<AtomicUsize>,
    active_connections: Arc<AtomicUsize>,
}

impl ProxyControl {
    fn new() -> Self {
        Self {
            close_generation: Arc::new(AtomicUsize::new(0)),
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn trigger_close_all(&self) {
        self.close_generation.fetch_add(1, Ordering::SeqCst);
    }

    fn generation(&self) -> usize {
        self.close_generation.load(Ordering::SeqCst)
    }

    fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::SeqCst)
    }
}

async fn spawn_ws_proxy(remote_url: &'static str) -> Result<(String, ProxyControl)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind local proxy listener")?;
    let local_addr = listener.local_addr().context("get local proxy addr")?;
    let endpoint = format!("ws://{}", local_addr);
    let control = ProxyControl::new();
    let control_task = control.clone();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(value) => value,
                Err(_) => break,
            };
            let control = control_task.clone();
            tokio::spawn(async move {
                let _ = proxy_connection(stream, remote_url, control).await;
            });
        }
    });

    Ok((endpoint, control))
}

async fn proxy_connection(
    stream: tokio::net::TcpStream,
    remote_url: &'static str,
    control: ProxyControl,
) -> Result<()> {
    let local_ws = accept_async(stream)
        .await
        .context("accept local websocket proxy connection")?;
    let (remote_ws, _) = connect_async(remote_url)
        .await
        .with_context(|| format!("connect remote websocket {remote_url}"))?;

    control.active_connections.fetch_add(1, Ordering::SeqCst);
    let close_generation = control.generation();

    let (mut local_sink, mut local_stream) = local_ws.split();
    let (mut remote_sink, mut remote_stream) = remote_ws.split();

    loop {
        if control.generation() != close_generation {
            let _ = local_sink.send(Message::Close(None)).await;
            let _ = remote_sink.send(Message::Close(None)).await;
            break;
        }

        tokio::select! {
            maybe_local = local_stream.next() => {
                match maybe_local {
                    Some(Ok(message)) => {
                        remote_sink.send(message).await.context("forward local->remote frame")?;
                    }
                    Some(Err(error)) => return Err(anyhow!(error).context("read local websocket frame")),
                    None => break,
                }
            }
            maybe_remote = remote_stream.next() => {
                match maybe_remote {
                    Some(Ok(message)) => {
                        local_sink.send(message).await.context("forward remote->local frame")?;
                    }
                    Some(Err(error)) => return Err(anyhow!(error).context("read remote websocket frame")),
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    control.active_connections.fetch_sub(1, Ordering::SeqCst);
    Ok(())
}

#[derive(Debug, Serialize)]
struct ForcedReconnectReport {
    unix_started_at_s: u64,
    cut_after_s: u64,
    watch_after_cut_s: u64,
    initial_startup_ready: bool,
    initial_startup_seen_count: usize,
    initial_startup_expected_count: usize,
    spot_connections_before_cut: usize,
    futures_connections_before_cut: usize,
    spot_resets_after_cut: usize,
    futures_resets_after_cut: usize,
    recovered_after_cut: bool,
    total_rebuilds: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cut_after_s = env_u64("MEXC_FORCE_RECONNECT_CUT_AFTER_SECONDS", 5).max(1);
    let watch_after_cut_s = env_u64("MEXC_FORCE_RECONNECT_WATCH_AFTER_CUT_SECONDS", 40).max(1);
    let report_path = std::env::var("MEXC_FORCE_RECONNECT_REPORT_PATH").ok();
    let unix_started_at_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs();

    let (spot_endpoint, spot_control) = spawn_ws_proxy("wss://wbs-api.mexc.com/ws").await?;
    let (futures_endpoint, futures_control) =
        spawn_ws_proxy("wss://contract.mexc.com/edge").await?;

    let connector = MexcConnector {
        rest: mexc::MexcPublicRestClient::default(),
        spot_ws: mexc::MexcSpotWsClient::default().with_endpoint(spot_endpoint),
        futures_ws: mexc::MexcFuturesWsClient::default().with_endpoint(futures_endpoint),
    };

    let mut runtime = connector
        .public_runtime_builder()
        .connect_managed_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await
        .context("connect managed runtime through local proxies")?;

    let startup = runtime
        .await_balanced_startup(Duration::from_secs(45))
        .await
        .context("await initial balanced startup")?;
    ensure!(
        startup.is_ready(),
        "initial startup did not reach readiness"
    );

    let spot_connections_before_cut = spot_control.active_connections();
    let futures_connections_before_cut = futures_control.active_connections();

    tokio::time::sleep(Duration::from_secs(cut_after_s)).await;
    spot_control.trigger_close_all();
    futures_control.trigger_close_all();

    let mut policy = MexcManagedRuntimePolicy::balanced_defaults();
    policy.auto_heal = false;
    let started_cut_watch = Instant::now();
    let deadline = started_cut_watch + Duration::from_secs(watch_after_cut_s);

    let mut spot_resets_after_cut = 0usize;
    let mut futures_resets_after_cut = 0usize;
    let mut recovered_after_cut = false;

    while Instant::now() < deadline {
        let step = tokio::time::timeout(Duration::from_secs(10), runtime.next_step(&policy))
            .await
            .context("wait for reconnect step")?
            .transpose()?
            .context("managed runtime ended unexpectedly during reconnect watch")?;

        if let Some(reset) = &step.pending_reset {
            if reset.spot_resets_detected > 0 {
                spot_resets_after_cut += reset.spot_resets_detected;
            }
            if reset.futures_resets_detected > 0 {
                futures_resets_after_cut += reset.futures_resets_detected;
            }
        }

        if spot_resets_after_cut > 0
            && futures_resets_after_cut > 0
            && step.health_alerts.health.is_healthy()
        {
            recovered_after_cut = true;
            break;
        }
    }

    let report = ForcedReconnectReport {
        unix_started_at_s,
        cut_after_s,
        watch_after_cut_s,
        initial_startup_ready: startup.is_ready(),
        initial_startup_seen_count: startup.seen_count(),
        initial_startup_expected_count: startup.expected_count(),
        spot_connections_before_cut,
        futures_connections_before_cut,
        spot_resets_after_cut,
        futures_resets_after_cut,
        recovered_after_cut,
        total_rebuilds: runtime.total_rebuilds(),
    };

    let json = serde_json::to_string_pretty(&report).context("serialize reconnect report")?;
    if let Some(path) = report_path {
        std::fs::write(&path, json).with_context(|| format!("write reconnect report to {path}"))?;
        println!("wrote reconnect report to {path}");
    } else {
        println!("{json}");
    }

    Ok(())
}
