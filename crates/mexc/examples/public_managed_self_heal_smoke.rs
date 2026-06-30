use anyhow::{Context, Result, ensure};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcPublicRuntimeConfig,
};
use tokio::time::{Duration, Instant};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    let force_max_age_seconds = env_u64("MEXC_FORCE_HEAL_MAX_AGE_SECONDS", 1).max(1);
    let force_min_stale_seconds = env_u64("MEXC_FORCE_HEAL_MIN_STALE_SECONDS", 0);
    let startup_wait_seconds = env_u64("MEXC_FORCE_HEAL_STARTUP_WAIT_SECONDS", 30).max(1);
    let window_seconds = env_u64("MEXC_FORCE_HEAL_WINDOW_SECONDS", 30).max(1);

    let connector = MexcConnector::default();
    let mut runtime = connector
        .public_runtime_builder()
        .connect_managed_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await
        .context("connect managed stateful with deep snapshot")?;

    println!(
        "manifest spot_connections={} futures_connections={} spot_subscriptions={} futures_subscriptions={}",
        runtime.manifest().spot_connection_count,
        runtime.manifest().futures_connection_count,
        runtime.manifest().spot_subscription_count,
        runtime.manifest().futures_subscription_count
    );

    let startup = runtime
        .await_balanced_startup(Duration::from_secs(startup_wait_seconds))
        .await
        .context("await initial balanced startup")?;
    ensure!(
        startup.is_ready(),
        "initial startup did not reach readiness"
    );
    println!(
        "startup ready={} coverage={}/{} rebuilds={}",
        startup.is_ready(),
        startup.seen_count(),
        startup.expected_count(),
        runtime.total_rebuilds()
    );

    let deadline = Instant::now() + Duration::from_secs(window_seconds);
    while Instant::now() < deadline {
        let _event = tokio::time::timeout(Duration::from_secs(10), runtime.next_event())
            .await
            .context("wait for managed runtime event")?
            .transpose()?
            .context("managed runtime stream ended unexpectedly")?;

        if let Some(healed) = runtime
            .heal_if_persistent_balanced_stale(
                Duration::from_secs(force_max_age_seconds),
                Duration::from_secs(force_min_stale_seconds),
                Duration::from_secs(startup_wait_seconds),
            )
            .await
            .context("trigger managed self-heal")?
        {
            println!(
                "self_heal_triggered rebuilds={} startup_ready={} coverage={}/{} persistent_stale={}",
                healed.total_rebuilds,
                healed.startup_after_heal.is_ready(),
                healed.startup_after_heal.seen_count(),
                healed.startup_after_heal.expected_count(),
                healed
                    .alerts_before_heal
                    .persistent_stale
                    .iter()
                    .map(|item| item.kind.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            ensure!(
                healed.startup_after_heal.is_ready(),
                "self-healed runtime did not regain balanced startup"
            );
            ensure!(
                runtime.total_rebuilds() >= 1,
                "rebuild count did not increase"
            );
            if let Some(symbol) = runtime.state().futures_symbols.get("BTC_USDT") {
                println!(
                    "post_heal BTC_USDT has_contract={} has_ticker={} has_index={} has_fair={} has_funding_snapshot={}",
                    symbol.contract.is_some(),
                    symbol.ticker.is_some(),
                    symbol.index_price.is_some(),
                    symbol.fair_price.is_some(),
                    symbol.funding_rate_snapshot.is_some()
                );
            }
            return Ok(());
        }
    }

    anyhow::bail!(
        "managed self-heal was not triggered within {} seconds",
        window_seconds
    )
}
