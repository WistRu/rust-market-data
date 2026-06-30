use anyhow::{Context, Result, ensure};
use mexc::{MexcConnector, MexcPublicEventKind, MexcPublicRuntimeConfig};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let mut runtime = connector
        .public_runtime_builder()
        .connect_stateful_with_snapshot(MexcPublicRuntimeConfig::balanced())
        .await
        .context("connect stateful with snapshot")?;

    println!(
        "manifest spot_connections={} futures_connections={} spot_subscriptions={} futures_subscriptions={}",
        runtime.manifest.spot_connection_count,
        runtime.manifest.futures_connection_count,
        runtime.manifest.spot_subscription_count,
        runtime.manifest.futures_subscription_count
    );

    let spot_kind = runtime
        .wait_for_live_event_kind(MexcPublicEventKind::SpotAggDepth, Duration::from_secs(20))
        .await
        .context("wait for spot aggDepth")?
        .context("spot aggDepth did not arrive within timeout")?;
    println!(
        "spot_wait_kind={}",
        spot_kind.kind().expect("spot kind").as_str()
    );

    let futures_kind = runtime
        .wait_for_live_event_kind(
            MexcPublicEventKind::FuturesIndexPrice,
            Duration::from_secs(20),
        )
        .await
        .context("wait for futures index price")?
        .context("futures index price did not arrive within timeout")?;
    println!(
        "futures_wait_kind={}",
        futures_kind.kind().expect("futures kind").as_str()
    );

    let contract_kind = runtime
        .wait_for_live_event_kind(
            MexcPublicEventKind::FuturesContract,
            Duration::from_secs(10),
        )
        .await
        .context("wait for futures contract")?;
    match contract_kind {
        Some(event) => println!(
            "contract_wait_kind={}",
            event.kind().expect("contract kind").as_str()
        ),
        None => println!("contract_wait_kind=timeout"),
    }

    ensure!(
        runtime.state().futures_symbols.contains_key("BTC_USDT"),
        "state lost BTC_USDT after wait flow"
    );
    Ok(())
}
