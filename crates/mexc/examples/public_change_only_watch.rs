use anyhow::{Context, Result};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcPublicEventKind,
    MexcPublicRuntimeConfig,
};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let mut runtime = connector
        .public_runtime_builder()
        .connect_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await
        .context("connect stateful with deep snapshot")?;

    let watched = [
        MexcPublicEventKind::FuturesContract,
        MexcPublicEventKind::FuturesEventContract,
    ];
    let report = runtime
        .observe_live_event_kinds(&watched, Duration::from_secs(30))
        .await
        .context("observe change-only kinds")?;

    if let Some(deep_report) = &runtime.deep_report {
        println!(
            "reference_modes index={} fair={} funding={}",
            deep_report.index_price_mode.as_str(),
            deep_report.fair_price_mode.as_str(),
            deep_report.funding_rate_mode.as_str()
        );
    }

    println!(
        "observed_live_events={} window_ms={}",
        report.observed_live_events, report.window_ms
    );
    for kind in watched {
        let item = report
            .observations
            .get(&kind)
            .expect("observation entry should exist");
        println!(
            "kind={} count={} first_seen_after_ms={:?} last_seen_after_ms={:?} profile={:?}",
            kind.as_str(),
            item.count,
            item.first_seen_after_ms,
            item.last_seen_after_ms,
            kind.delivery_profile()
        );
    }
    let missing = report
        .missing
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",");
    println!("missing_kinds={missing}");

    Ok(())
}
