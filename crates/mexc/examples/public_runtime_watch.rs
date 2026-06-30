use anyhow::{Context, Result};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcManagedRuntimePolicy,
    MexcPublicEventKind, MexcPublicRuntimeConfig,
};
use std::collections::BTreeMap;
use tokio::time::{Duration, Instant};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn top_kinds(kind_counts: &BTreeMap<MexcPublicEventKind, usize>, limit: usize) -> String {
    let mut items = kind_counts
        .iter()
        .map(|(kind, count)| (*kind, *count))
        .collect::<Vec<_>>();
    items.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    items
        .into_iter()
        .take(limit)
        .map(|(kind, count)| format!("{}={count}", kind.as_str()))
        .collect::<Vec<_>>()
        .join(",")
}

fn join_kinds(kinds: &[MexcPublicEventKind]) -> String {
    kinds
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn describe_alert_kinds(alerts: &[mexc::MexcLiveKindHealthAlert]) -> String {
    alerts
        .iter()
        .map(|alert| {
            format!(
                "{}:stale_for_ms={}:last_seen_ago_ms={}:max_age_ms={}",
                alert.kind.as_str(),
                alert.stale_for_ms,
                alert
                    .last_seen_ago_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                alert.max_age_ms
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn describe_recoveries(recoveries: &[mexc::MexcLiveKindRecovery]) -> String {
    recoveries
        .iter()
        .map(|item| format!("{}:stale_for_ms={}", item.kind.as_str(), item.stale_for_ms))
        .collect::<Vec<_>>()
        .join(",")
}

#[tokio::main]
async fn main() -> Result<()> {
    let watch_seconds = env_u64("MEXC_WATCH_SECONDS", 60);
    let summary_every_seconds = env_u64("MEXC_WATCH_SUMMARY_EVERY_SECONDS", 10).max(1);
    let health_max_age_seconds = env_u64("MEXC_HEALTH_MAX_AGE_SECONDS", 5).max(1);
    let health_min_stale_seconds = env_u64("MEXC_HEALTH_MIN_STALE_SECONDS", 5);
    let self_heal_stale_seconds = env_u64("MEXC_SELF_HEAL_STALE_SECONDS", 0);
    let self_heal_startup_wait_seconds = env_u64("MEXC_SELF_HEAL_STARTUP_WAIT_SECONDS", 30).max(1);
    let self_heal_cooldown_seconds = env_u64("MEXC_SELF_HEAL_COOLDOWN_SECONDS", 60);
    let public_metadata_refresh_interval_seconds =
        env_u64("MEXC_PUBLIC_METADATA_REFRESH_INTERVAL_SECONDS", 0);
    let contract_refresh_interval_seconds = env_u64("MEXC_CONTRACT_REFRESH_INTERVAL_SECONDS", 0);
    let contract_event_refresh = env_u64("MEXC_CONTRACT_EVENT_REFRESH", 1) > 0;
    let contract_event_refresh_cooldown_seconds =
        env_u64("MEXC_CONTRACT_EVENT_REFRESH_COOLDOWN_SECONDS", 2);
    let contract_gap_refresh = env_u64("MEXC_CONTRACT_GAP_REFRESH", 1) > 0;
    let contract_gap_refresh_cooldown_seconds =
        env_u64("MEXC_CONTRACT_GAP_REFRESH_COOLDOWN_SECONDS", 30);
    let contract_gap_refresh_batch_size =
        env_u64("MEXC_CONTRACT_GAP_REFRESH_BATCH_SIZE", 16) as usize;
    let use_default_health_policy = std::env::var("MEXC_USE_DEFAULT_HEALTH_POLICY")
        .map(|value| value != "0")
        .unwrap_or(true);

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
    if let Some(report) = runtime.deep_report() {
        println!(
            "reference_modes index={} fair={} funding={}",
            report.index_price_mode.as_str(),
            report.fair_price_mode.as_str(),
            report.funding_rate_mode.as_str()
        );
    }

    let startup = runtime
        .await_balanced_startup(Duration::from_secs(30))
        .await
        .context("await balanced startup")?;
    println!(
        "startup_ready={} coverage={}/{} observed_live_events={}",
        startup.is_ready(),
        startup.seen_count(),
        startup.expected_count(),
        startup.observed_live_events
    );

    let started_at = Instant::now();
    let deadline = started_at + Duration::from_secs(watch_seconds);
    let mut next_summary_at = started_at + Duration::from_secs(summary_every_seconds);
    let mut total_live_events = 0usize;
    let mut kind_counts = BTreeMap::<MexcPublicEventKind, usize>::new();
    let mut reset_reports = 0usize;
    let mut latest_alerts = runtime
        .poll_balanced_health_alerts_with_defaults(Duration::from_secs(health_min_stale_seconds));
    let mut policy = MexcManagedRuntimePolicy::balanced_defaults();
    policy.use_default_health_policy = use_default_health_policy;
    policy.health_max_age = Duration::from_secs(health_max_age_seconds);
    policy.health_min_stale_for = Duration::from_secs(health_min_stale_seconds);
    policy.contract_event_refresh = contract_event_refresh;
    policy.contract_event_refresh_cooldown =
        Duration::from_secs(contract_event_refresh_cooldown_seconds);
    policy.contract_gap_refresh = contract_gap_refresh;
    policy.contract_gap_refresh_cooldown =
        Duration::from_secs(contract_gap_refresh_cooldown_seconds);
    policy.contract_gap_refresh_batch_size = contract_gap_refresh_batch_size;
    policy.public_metadata_refresh_interval = if public_metadata_refresh_interval_seconds > 0 {
        Some(Duration::from_secs(
            public_metadata_refresh_interval_seconds,
        ))
    } else {
        None
    };
    policy.contract_refresh_interval = if contract_refresh_interval_seconds > 0 {
        Some(Duration::from_secs(contract_refresh_interval_seconds))
    } else {
        None
    };

    while Instant::now() < deadline {
        let step = tokio::time::timeout(Duration::from_secs(10), runtime.next_step(&policy))
            .await
            .context("wait for managed runtime step")?
            .transpose()?
            .context("managed runtime ended unexpectedly")?;
        let event = step.event;
        latest_alerts = step.health_alerts.clone();

        if let Some(kind) = event.kind() {
            total_live_events += 1;
            *kind_counts.entry(kind).or_insert(0) += 1;
        }

        if let Some(reset) = step.pending_reset {
            reset_reports += 1;
            println!(
                "reset#{} spot_resets={} futures_resets={}",
                reset_reports, reset.spot_resets_detected, reset.futures_resets_detected
            );
        }

        if let Some(refresh) = step.public_metadata_refresh {
            println!(
                "public_metadata_refresh spot_server_time_changed={} spot_default_symbols_changed={} spot_default_symbol_count={}=>{} spot_offline_symbols_changed={} spot_offline_symbol_count={}=>{} spot_exchange_info_changed={} spot_exchange_symbol_count={}=>{} futures_server_time_changed={} futures_transferable_currencies_changed={} futures_transferable_currency_count={}=>{} futures_insurance_balances_changed={} futures_insurance_balance_count={}=>{}",
                refresh.spot_server_time_changed,
                refresh.spot_default_symbols_changed,
                refresh.spot_default_symbol_count_before,
                refresh.spot_default_symbol_count_after,
                refresh.spot_offline_symbols_changed,
                refresh.spot_offline_symbol_count_before,
                refresh.spot_offline_symbol_count_after,
                refresh.spot_exchange_info_changed,
                refresh.spot_exchange_symbol_count_before,
                refresh.spot_exchange_symbol_count_after,
                refresh.futures_server_time_changed,
                refresh.futures_transferable_currencies_changed,
                refresh.futures_transferable_currency_count_before,
                refresh.futures_transferable_currency_count_after,
                refresh.futures_insurance_balances_changed,
                refresh.futures_insurance_balance_count_before,
                refresh.futures_insurance_balance_count_after
            );
        }

        if let Some(refresh) = step.contract_refresh {
            println!(
                "contract_refresh cause={:?} requested_symbols={:?} unresolved_requested_symbols={:?} used_full_snapshot_fallback={} refreshed_contracts={} added={} updated={} removed={} unchanged={} added_symbols={:?} updated_symbols={:?} removed_symbols={:?} changes={:?}",
                refresh.cause,
                refresh.requested_symbols,
                refresh.unresolved_requested_symbols,
                refresh.used_full_snapshot_fallback,
                refresh.refreshed_contracts,
                refresh.added,
                refresh.updated,
                refresh.removed,
                refresh.unchanged,
                refresh.added_symbols,
                refresh.updated_symbols,
                refresh.removed_symbols,
                refresh.changes
            );
        }

        if Instant::now() >= next_summary_at {
            let session = runtime.session_status();
            let alerts = latest_alerts.clone();
            if !alerts.newly_stale.is_empty() {
                println!(
                    "alert newly_stale={}",
                    describe_alert_kinds(&alerts.newly_stale)
                );
            }
            if !alerts.persistent_stale.is_empty() {
                println!(
                    "alert persistent_stale={}",
                    describe_alert_kinds(&alerts.persistent_stale)
                );
            }
            if !alerts.recovered.is_empty() {
                println!("alert recovered={}", describe_recoveries(&alerts.recovered));
            }

            if self_heal_stale_seconds > 0 {
                match runtime
                    .heal_if_persistent_balanced_stale_with_defaults_and_cooldown(
                        Duration::from_secs(self_heal_stale_seconds),
                        Duration::from_secs(self_heal_startup_wait_seconds),
                        Duration::from_secs(self_heal_cooldown_seconds),
                    )
                    .await
                    .context("self-heal persistent balanced stale")?
                {
                    mexc::MexcManagedHealOutcome::Healthy => {}
                    mexc::MexcManagedHealOutcome::Suppressed {
                        alerts_before_heal,
                        cooldown_remaining_ms,
                        total_rebuilds,
                    } => {
                        println!(
                            "self_heal_suppressed rebuilds={} cooldown_remaining_ms={} persistent_stale={}",
                            total_rebuilds,
                            cooldown_remaining_ms,
                            describe_alert_kinds(&alerts_before_heal.persistent_stale)
                        );
                    }
                    mexc::MexcManagedHealOutcome::Healed(healed) => {
                        println!(
                            "self_heal rebuilds={} persistent_stale={} startup_ready={} coverage={}/{}",
                            healed.total_rebuilds,
                            describe_alert_kinds(&healed.alerts_before_heal.persistent_stale),
                            healed.startup_after_heal.is_ready(),
                            healed.startup_after_heal.seen_count(),
                            healed.startup_after_heal.expected_count()
                        );
                    }
                }
            }
            println!(
                "summary elapsed_s={} total_live_events={} reset_reports={} total_spot_resets={} total_futures_resets={} pending_contract_refresh_symbols={:?} health={}/{} stale_kinds={} top_kinds={}",
                started_at.elapsed().as_secs(),
                total_live_events,
                reset_reports,
                session.total_spot_resets,
                session.total_futures_resets,
                runtime.pending_contract_refresh_symbols(),
                alerts.health.healthy_count(),
                alerts.health.expected_count(),
                join_kinds(&alerts.health.stale),
                top_kinds(&kind_counts, 8)
            );
            next_summary_at += Duration::from_secs(summary_every_seconds);
        }
    }

    let session = runtime.session_status();
    let alerts = latest_alerts;
    println!(
        "final elapsed_s={} total_live_events={} reset_reports={} total_spot_resets={} total_futures_resets={} pending_contract_refresh_symbols={:?} health={}/{} stale_kinds={} top_kinds={}",
        started_at.elapsed().as_secs(),
        total_live_events,
        reset_reports,
        session.total_spot_resets,
        session.total_futures_resets,
        runtime.pending_contract_refresh_symbols(),
        alerts.health.healthy_count(),
        alerts.health.expected_count(),
        join_kinds(&alerts.health.stale),
        top_kinds(&kind_counts, 12)
    );

    Ok(())
}
