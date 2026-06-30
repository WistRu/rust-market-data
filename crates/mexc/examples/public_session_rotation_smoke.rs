use anyhow::{Result, ensure};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcFuturesWsClient,
    MexcPublicRuntimeBuilder, MexcPublicRuntimeConfig, MexcSpotWsClient,
};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let rotate_after_seconds = std::env::var("MEXC_ROTATE_AFTER_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5);
    let recovery_wait_seconds = std::env::var("MEXC_ROTATION_RECOVERY_WAIT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20);

    let connector = MexcConnector::default();
    let builder = MexcPublicRuntimeBuilder::new(
        connector.rest.clone(),
        MexcSpotWsClient::default()
            .with_session_max_age(Duration::from_secs(rotate_after_seconds))
            .without_session_rotation_spread(),
        MexcFuturesWsClient::default()
            .with_session_max_age(Duration::from_secs(rotate_after_seconds)),
    );

    let mut runtime = builder
        .connect_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await?;

    let startup = runtime
        .wait_for_live_event_kinds(
            mexc::BALANCED_STARTUP_KINDS,
            Duration::from_secs(recovery_wait_seconds),
        )
        .await?;
    ensure!(startup.is_ready(), "initial startup did not become ready");
    println!(
        "startup ready={} coverage={}/{}",
        startup.is_ready(),
        startup.seen_count(),
        startup.expected_count()
    );

    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(rotate_after_seconds * 3 + recovery_wait_seconds);
    let mut saw_reset = false;
    let mut saw_spot_reset = false;
    let mut saw_futures_reset = false;
    let mut recovery_ready = false;

    while tokio::time::Instant::now() < deadline {
        let Some(event) = runtime.next_live_event().await else {
            break;
        };
        let _ = event?;
        let session = runtime.session_status();
        if session.pending_spot_resets > 0 || session.pending_futures_resets > 0 {
            let Some(recovery) = runtime
                .await_recovery_after_pending_reset(Duration::from_secs(recovery_wait_seconds))
                .await?
            else {
                continue;
            };
            let reset = &recovery.reset;
            saw_reset = true;
            if reset.spot_resets_detected > 0 {
                saw_spot_reset = true;
            }
            if reset.futures_resets_detected > 0 {
                saw_futures_reset = true;
            }
            println!(
                "rotation_reset spot_resets={} futures_resets={}",
                reset.spot_resets_detected, reset.futures_resets_detected
            );
            recovery_ready = recovery.readiness.is_ready();
            println!(
                "rotation_recovery ready={} coverage={}/{} observed_live_events={}",
                recovery.readiness.is_ready(),
                recovery.readiness.seen_count(),
                recovery.readiness.expected_count(),
                recovery.readiness.observed_live_events
            );
            if recovery_ready && saw_spot_reset && saw_futures_reset {
                break;
            }
        }
    }

    ensure!(
        saw_reset,
        "planned session rotation did not trigger any reset"
    );
    ensure!(
        saw_spot_reset,
        "planned spot session rotation was not observed"
    );
    ensure!(
        saw_futures_reset,
        "planned futures session rotation was not observed"
    );
    ensure!(
        recovery_ready,
        "runtime did not recover after planned rotation"
    );

    Ok(())
}
