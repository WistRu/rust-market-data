use crate::{
    FuturesContractInfo, FuturesFairPrice, FuturesFundingRate, FuturesIndexPrice,
    FuturesInsuranceBalance, FuturesTicker, MexcFuturesContractChange, MexcFuturesCoverageConfig,
    MexcFuturesSubscription, MexcFuturesWsClient, MexcFuturesWsMessage,
    MexcPublicMetadataRefreshReport, MexcPublicRestClient, MexcPublicState,
    MexcPublicStateHandoffReport, MexcSpotCoverageConfig, MexcSpotSubscription, MexcSpotWsClient,
    MexcSpotWsMessage, OneOrMany, SpotBookTicker, SpotDefaultSymbolsResponse, SpotExchangeInfo,
    SpotOfflineSymbolsResponse, SpotPriceTicker, SpotServerTime, SpotTicker24Hr,
    build_futures_public_subscriptions, build_spot_public_subscriptions,
};
use anyhow::{Result, anyhow};
use futures::StreamExt;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, timeout};
use tokio_stream::wrappers::ReceiverStream;

pub const DEFAULT_FUTURES_SUBSCRIPTIONS_PER_CONNECTION: usize = 200;
pub const DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY: usize = 8;
const PUBLIC_RUNTIME_EVENT_CHANNEL_CAPACITY: usize = 1024;

pub const BALANCED_STARTUP_KINDS: &[MexcPublicEventKind] = &[
    MexcPublicEventKind::SpotAggDepth,
    MexcPublicEventKind::FuturesIndexPrice,
    MexcPublicEventKind::FuturesFairPrice,
];

pub const SPOT_RECOVERY_KINDS: &[MexcPublicEventKind] = &[MexcPublicEventKind::SpotAggDepth];
pub const FUTURES_RECOVERY_KINDS: &[MexcPublicEventKind] = &[
    MexcPublicEventKind::FuturesIndexPrice,
    MexcPublicEventKind::FuturesFairPrice,
];
pub const BALANCED_HEALTH_KINDS: &[MexcPublicEventKind] = &[
    MexcPublicEventKind::SpotAggDepth,
    MexcPublicEventKind::SpotBookTickerBatch,
    MexcPublicEventKind::FuturesDepth,
    MexcPublicEventKind::FuturesTicker,
    MexcPublicEventKind::FuturesIndexPrice,
    MexcPublicEventKind::FuturesFairPrice,
];

#[derive(Debug, Clone)]
pub struct MexcPublicRuntimeConfig {
    pub spot_coverage: MexcSpotCoverageConfig,
    pub futures_coverage: MexcFuturesCoverageConfig,
    pub futures_subscriptions_per_connection: usize,
}

impl MexcPublicRuntimeConfig {
    pub fn balanced() -> Self {
        Self {
            spot_coverage: MexcSpotCoverageConfig::balanced(),
            futures_coverage: MexcFuturesCoverageConfig::balanced(),
            futures_subscriptions_per_connection: DEFAULT_FUTURES_SUBSCRIPTIONS_PER_CONNECTION,
        }
    }

    pub fn exhaustive() -> Self {
        Self {
            spot_coverage: MexcSpotCoverageConfig::exhaustive(),
            futures_coverage: MexcFuturesCoverageConfig::exhaustive(),
            futures_subscriptions_per_connection: DEFAULT_FUTURES_SUBSCRIPTIONS_PER_CONNECTION,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MexcPublicRuntimeManifest {
    pub spot_symbol_count: usize,
    pub futures_symbol_count: usize,
    pub spot_subscription_count: usize,
    pub futures_subscription_count: usize,
    pub spot_connection_count: usize,
    pub futures_connection_count: usize,
}

#[derive(Debug, Clone)]
pub struct MexcSpotBootstrapSnapshot {
    pub server_time: SpotServerTime,
    pub default_symbols: SpotDefaultSymbolsResponse,
    pub offline_symbols: SpotOfflineSymbolsResponse,
    pub exchange_info: SpotExchangeInfo,
    pub ticker_24hr: Vec<SpotTicker24Hr>,
    pub price_tickers: Vec<SpotPriceTicker>,
    pub book_tickers: Vec<SpotBookTicker>,
}

#[derive(Debug, Clone)]
pub struct MexcFuturesBootstrapSnapshot {
    pub server_time: u64,
    pub contracts: Vec<FuturesContractInfo>,
    pub tickers: Vec<FuturesTicker>,
    pub transferable_currencies: Vec<String>,
    pub insurance_balances: Vec<FuturesInsuranceBalance>,
}

#[derive(Debug, Clone)]
pub struct MexcPublicBootstrapSnapshot {
    pub spot: MexcSpotBootstrapSnapshot,
    pub futures: MexcFuturesBootstrapSnapshot,
}

#[derive(Debug, Clone)]
pub struct MexcPublicMetadataRefreshSnapshot {
    pub spot_server_time: SpotServerTime,
    pub spot_default_symbols: SpotDefaultSymbolsResponse,
    pub spot_offline_symbols: SpotOfflineSymbolsResponse,
    pub spot_exchange_info: SpotExchangeInfo,
    pub futures_server_time: u64,
    pub futures_transferable_currencies: Vec<String>,
    pub futures_insurance_balances: Vec<FuturesInsuranceBalance>,
}

#[derive(Debug, Clone)]
pub struct MexcFuturesReferenceSnapshot {
    pub index_prices: Vec<FuturesIndexPrice>,
    pub fair_prices: Vec<FuturesFairPrice>,
    pub funding_rates: Vec<FuturesFundingRate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MexcBootstrapFetchMode {
    BulkOnly,
    Parallel { concurrency: usize },
    SequentialRequested,
    SequentialFallback { requested_concurrency: usize },
}

impl MexcBootstrapFetchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BulkOnly => "bulk_only",
            Self::Parallel { .. } => "parallel",
            Self::SequentialRequested => "sequential",
            Self::SequentialFallback { .. } => "sequential_fallback",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MexcFuturesReferenceBootstrapReport {
    pub symbol_count: usize,
    pub index_price_count: usize,
    pub fair_price_count: usize,
    pub funding_rate_count: usize,
    pub index_price_bulk_count: usize,
    pub fair_price_bulk_count: usize,
    pub funding_rate_bulk_count: usize,
    pub index_price_endpoint_count: usize,
    pub fair_price_endpoint_count: usize,
    pub funding_rate_endpoint_count: usize,
    pub index_price_mode: MexcBootstrapFetchMode,
    pub fair_price_mode: MexcBootstrapFetchMode,
    pub funding_rate_mode: MexcBootstrapFetchMode,
}

#[derive(Debug, Clone)]
pub struct MexcPublicDeepBootstrapSnapshot {
    pub base: MexcPublicBootstrapSnapshot,
    pub futures_reference: MexcFuturesReferenceSnapshot,
    pub report: MexcFuturesReferenceBootstrapReport,
}

#[derive(Debug, Clone)]
pub enum MexcPublicEvent {
    Spot(MexcSpotWsMessage),
    Futures(MexcFuturesWsMessage),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MexcPublicEventKind {
    SpotAggTrades,
    SpotIncreaseDepth,
    SpotIncreaseDepthBatch,
    SpotAggDepth,
    SpotLimitDepth,
    SpotBookTicker,
    SpotBookTickerBatch,
    SpotAggBookTicker,
    SpotKline,
    SpotMiniTicker,
    SpotMiniTickers,
    FuturesTickers,
    FuturesTicker,
    FuturesDeals,
    FuturesDepth,
    FuturesDepthStep,
    FuturesDepthFull,
    FuturesFundingRate,
    FuturesIndexPrice,
    FuturesFairPrice,
    FuturesKline,
    FuturesContract,
    FuturesEventContract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MexcPublicEventDeliveryProfile {
    AlwaysOn,
    ChangeOnly,
    Opportunistic,
}

#[derive(Debug, Clone)]
pub struct MexcLiveKindWaitReport {
    pub expected: Vec<MexcPublicEventKind>,
    pub seen: BTreeSet<MexcPublicEventKind>,
    pub missing: Vec<MexcPublicEventKind>,
    pub observed_live_events: usize,
}

impl MexcLiveKindWaitReport {
    pub fn is_ready(&self) -> bool {
        self.missing.is_empty()
    }

    pub fn seen_count(&self) -> usize {
        self.seen.len()
    }

    pub fn expected_count(&self) -> usize {
        self.expected.len()
    }
}

#[derive(Debug, Clone, Default)]
pub struct MexcLiveKindObservation {
    pub count: usize,
    pub first_seen_after_ms: Option<u128>,
    pub last_seen_after_ms: Option<u128>,
}

#[derive(Debug, Clone)]
pub struct MexcLiveKindObservationReport {
    pub expected: Vec<MexcPublicEventKind>,
    pub observations: BTreeMap<MexcPublicEventKind, MexcLiveKindObservation>,
    pub missing: Vec<MexcPublicEventKind>,
    pub observed_live_events: usize,
    pub window_ms: u128,
}

impl MexcPublicEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SpotAggTrades => "spot.aggTrades",
            Self::SpotIncreaseDepth => "spot.increaseDepth",
            Self::SpotIncreaseDepthBatch => "spot.increaseDepthBatch",
            Self::SpotAggDepth => "spot.aggDepth",
            Self::SpotLimitDepth => "spot.limitDepth",
            Self::SpotBookTicker => "spot.bookTickerRaw",
            Self::SpotBookTickerBatch => "spot.bookTickerBatch",
            Self::SpotAggBookTicker => "spot.bookTicker",
            Self::SpotKline => "spot.kline",
            Self::SpotMiniTicker => "spot.miniTicker",
            Self::SpotMiniTickers => "spot.miniTickers",
            Self::FuturesTickers => "futures.tickers",
            Self::FuturesTicker => "futures.ticker",
            Self::FuturesDeals => "futures.deals",
            Self::FuturesDepth => "futures.depth",
            Self::FuturesDepthStep => "futures.depthStep",
            Self::FuturesDepthFull => "futures.depthFull",
            Self::FuturesFundingRate => "futures.fundingRate",
            Self::FuturesIndexPrice => "futures.indexPrice",
            Self::FuturesFairPrice => "futures.fairPrice",
            Self::FuturesKline => "futures.kline",
            Self::FuturesContract => "futures.contract",
            Self::FuturesEventContract => "futures.eventContract",
        }
    }

    pub fn delivery_profile(self) -> MexcPublicEventDeliveryProfile {
        match self {
            Self::SpotAggTrades
            | Self::SpotAggDepth
            | Self::SpotKline
            | Self::SpotMiniTicker
            | Self::SpotMiniTickers
            | Self::FuturesTickers
            | Self::FuturesTicker
            | Self::FuturesDeals
            | Self::FuturesDepth
            | Self::FuturesDepthStep
            | Self::FuturesFundingRate
            | Self::FuturesIndexPrice
            | Self::FuturesFairPrice
            | Self::FuturesKline => MexcPublicEventDeliveryProfile::AlwaysOn,
            Self::FuturesContract | Self::FuturesEventContract => {
                MexcPublicEventDeliveryProfile::ChangeOnly
            }
            Self::SpotIncreaseDepth
            | Self::SpotIncreaseDepthBatch
            | Self::SpotLimitDepth
            | Self::SpotBookTicker
            | Self::SpotBookTickerBatch
            | Self::SpotAggBookTicker
            | Self::FuturesDepthFull => MexcPublicEventDeliveryProfile::Opportunistic,
        }
    }

    pub fn is_recommended_for_balanced_startup(self) -> bool {
        BALANCED_STARTUP_KINDS.contains(&self)
    }

    pub fn is_spot(self) -> bool {
        matches!(
            self,
            Self::SpotAggTrades
                | Self::SpotIncreaseDepth
                | Self::SpotIncreaseDepthBatch
                | Self::SpotAggDepth
                | Self::SpotLimitDepth
                | Self::SpotBookTicker
                | Self::SpotBookTickerBatch
                | Self::SpotAggBookTicker
                | Self::SpotKline
                | Self::SpotMiniTicker
                | Self::SpotMiniTickers
        )
    }

    pub fn is_futures(self) -> bool {
        !self.is_spot()
    }

    pub fn recommended_max_silence(self) -> Duration {
        match self {
            Self::SpotAggDepth => Duration::from_secs(2),
            Self::SpotBookTickerBatch => Duration::from_secs(5),
            Self::FuturesDepth => Duration::from_secs(2),
            Self::FuturesTicker => Duration::from_secs(5),
            Self::FuturesIndexPrice | Self::FuturesFairPrice => Duration::from_secs(10),
            _ => Duration::from_secs(10),
        }
    }
}

impl MexcPublicEvent {
    pub fn kind(&self) -> Option<MexcPublicEventKind> {
        match self {
            Self::Spot(message) => match message {
                MexcSpotWsMessage::SessionStart(_)
                | MexcSpotWsMessage::Ack(_)
                | MexcSpotWsMessage::RawText(_) => None,
                MexcSpotWsMessage::AggTrades(_) => Some(MexcPublicEventKind::SpotAggTrades),
                MexcSpotWsMessage::IncreaseDepth(_) => Some(MexcPublicEventKind::SpotIncreaseDepth),
                MexcSpotWsMessage::IncreaseDepthBatch(_) => {
                    Some(MexcPublicEventKind::SpotIncreaseDepthBatch)
                }
                MexcSpotWsMessage::AggDepth(_) => Some(MexcPublicEventKind::SpotAggDepth),
                MexcSpotWsMessage::LimitDepth(_) => Some(MexcPublicEventKind::SpotLimitDepth),
                MexcSpotWsMessage::BookTicker(_) => Some(MexcPublicEventKind::SpotBookTicker),
                MexcSpotWsMessage::BookTickerBatch(_) => {
                    Some(MexcPublicEventKind::SpotBookTickerBatch)
                }
                MexcSpotWsMessage::AggBookTicker(_) => Some(MexcPublicEventKind::SpotAggBookTicker),
                MexcSpotWsMessage::Kline(_) => Some(MexcPublicEventKind::SpotKline),
                MexcSpotWsMessage::MiniTicker(_) => Some(MexcPublicEventKind::SpotMiniTicker),
                MexcSpotWsMessage::MiniTickers(_) => Some(MexcPublicEventKind::SpotMiniTickers),
            },
            Self::Futures(message) => match message {
                MexcFuturesWsMessage::SessionStart(_)
                | MexcFuturesWsMessage::Ack(_)
                | MexcFuturesWsMessage::Raw(_) => None,
                MexcFuturesWsMessage::Tickers(_) => Some(MexcPublicEventKind::FuturesTickers),
                MexcFuturesWsMessage::Ticker(_) => Some(MexcPublicEventKind::FuturesTicker),
                MexcFuturesWsMessage::Deals(_) => Some(MexcPublicEventKind::FuturesDeals),
                MexcFuturesWsMessage::Depth(_) => Some(MexcPublicEventKind::FuturesDepth),
                MexcFuturesWsMessage::DepthStep(_) => Some(MexcPublicEventKind::FuturesDepthStep),
                MexcFuturesWsMessage::DepthFull(_) => Some(MexcPublicEventKind::FuturesDepthFull),
                MexcFuturesWsMessage::FundingRate(_) => {
                    Some(MexcPublicEventKind::FuturesFundingRate)
                }
                MexcFuturesWsMessage::IndexPrice(_) => Some(MexcPublicEventKind::FuturesIndexPrice),
                MexcFuturesWsMessage::FairPrice(_) => Some(MexcPublicEventKind::FuturesFairPrice),
                MexcFuturesWsMessage::Kline(_) => Some(MexcPublicEventKind::FuturesKline),
                MexcFuturesWsMessage::Contract(_) => Some(MexcPublicEventKind::FuturesContract),
                MexcFuturesWsMessage::EventContract(_) => {
                    Some(MexcPublicEventKind::FuturesEventContract)
                }
            },
        }
    }

    pub fn is_live_payload(&self) -> bool {
        self.kind().is_some()
    }
}

#[derive(Debug)]
pub struct MexcPublicRuntime {
    pub manifest: MexcPublicRuntimeManifest,
    pub stream: ReceiverStream<Result<MexcPublicEvent>>,
}

#[derive(Debug)]
pub struct MexcPublicBootstrapRuntime {
    pub manifest: MexcPublicRuntimeManifest,
    pub snapshot: MexcPublicBootstrapSnapshot,
    pub stream: ReceiverStream<Result<MexcPublicEvent>>,
}

#[derive(Debug)]
pub struct MexcPublicDeepBootstrapRuntime {
    pub manifest: MexcPublicRuntimeManifest,
    pub snapshot: MexcPublicDeepBootstrapSnapshot,
    pub stream: ReceiverStream<Result<MexcPublicEvent>>,
}

pub struct MexcManagedReadyRuntime {
    pub runtime: MexcManagedStatefulPublicRuntime,
    pub startup: MexcLiveKindWaitReport,
    pub contract_gap_warmup: MexcManagedContractGapWarmupReport,
}

impl MexcManagedReadyRuntime {
    pub fn is_ready(&self) -> bool {
        self.startup.is_ready()
    }

    pub fn into_parts(self) -> (MexcManagedStatefulPublicRuntime, MexcLiveKindWaitReport) {
        (self.runtime, self.startup)
    }
}

#[derive(Debug)]
pub struct MexcStatefulPublicRuntime {
    pub manifest: MexcPublicRuntimeManifest,
    pub state: MexcPublicState,
    pub deep_report: Option<MexcFuturesReferenceBootstrapReport>,
    spot_session: Option<crate::MexcWsSessionStart>,
    futures_session: Option<crate::MexcWsSessionStart>,
    pending_spot_resets: usize,
    pending_futures_resets: usize,
    total_spot_resets: usize,
    total_futures_resets: usize,
    spot_recovery_seen: BTreeSet<MexcPublicEventKind>,
    futures_recovery_seen: BTreeSet<MexcPublicEventKind>,
    last_seen_at_by_kind: BTreeMap<MexcPublicEventKind, Instant>,
    stale_since_by_kind: BTreeMap<MexcPublicEventKind, Instant>,
    stream: ReceiverStream<Result<MexcPublicEvent>>,
}

#[derive(Debug, Clone, Copy)]
enum MexcManagedSnapshotMode {
    Snapshot,
    DeepSnapshot { concurrency: usize },
}

pub struct MexcManagedStatefulPublicRuntime {
    builder: MexcPublicRuntimeBuilder,
    config: MexcPublicRuntimeConfig,
    snapshot_mode: MexcManagedSnapshotMode,
    total_rebuilds: usize,
    last_rebuild_at: Option<Instant>,
    last_contract_refresh_at: Option<Instant>,
    last_public_metadata_refresh_at: Option<Instant>,
    pending_contract_refresh_symbols: BTreeSet<String>,
    inner: MexcStatefulPublicRuntime,
}

#[derive(Debug, Clone)]
pub struct MexcRuntimeSessionStatus {
    pub spot_session: Option<crate::MexcWsSessionStart>,
    pub futures_session: Option<crate::MexcWsSessionStart>,
    pub pending_spot_resets: usize,
    pub pending_futures_resets: usize,
    pub total_spot_resets: usize,
    pub total_futures_resets: usize,
}

#[derive(Debug, Clone)]
pub struct MexcRuntimeResetReport {
    pub spot_session: Option<crate::MexcWsSessionStart>,
    pub futures_session: Option<crate::MexcWsSessionStart>,
    pub spot_resets_detected: usize,
    pub futures_resets_detected: usize,
}

#[derive(Debug, Clone)]
pub struct MexcRuntimeRecoveryReport {
    pub reset: MexcRuntimeResetReport,
    pub readiness: MexcLiveKindWaitReport,
}

#[derive(Debug, Clone)]
pub struct MexcManagedHealReport {
    pub alerts_before_heal: MexcLiveKindAlertReport,
    pub startup_after_heal: MexcLiveKindWaitReport,
    pub total_rebuilds: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct MexcManagedContractRefreshReport {
    pub cause: MexcManagedContractRefreshCause,
    pub requested_symbols: Vec<String>,
    pub unresolved_requested_symbols: Vec<String>,
    pub used_full_snapshot_fallback: bool,
    pub refreshed_contracts: usize,
    pub added: usize,
    pub added_symbols: Vec<String>,
    pub updated: usize,
    pub updated_symbols: Vec<String>,
    pub removed: usize,
    pub removed_symbols: Vec<String>,
    pub unchanged: usize,
    pub changes: Vec<MexcFuturesContractChange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MexcManagedContractGapWarmupStopReason {
    NoGaps,
    Converged,
    Stalled,
    MaxPasses,
}

#[derive(Debug, Clone, Serialize)]
pub struct MexcManagedContractGapWarmupReport {
    pub initial_gap_symbols: Vec<String>,
    pub final_gap_symbols: Vec<String>,
    pub passes: usize,
    pub refreshes: Vec<MexcManagedContractRefreshReport>,
    pub stop_reason: MexcManagedContractGapWarmupStopReason,
}

impl MexcManagedContractGapWarmupReport {
    pub fn initial_gap_count(&self) -> usize {
        self.initial_gap_symbols.len()
    }

    pub fn final_gap_count(&self) -> usize {
        self.final_gap_symbols.len()
    }

    pub fn resolved_gap_count(&self) -> usize {
        self.initial_gap_count()
            .saturating_sub(self.final_gap_count())
    }

    pub fn is_fully_resolved(&self) -> bool {
        self.final_gap_symbols.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MexcManagedContractRefreshCause {
    Interval,
    Targeted,
    GapBackfill,
}

impl MexcManagedContractRefreshReport {
    pub fn has_changes(&self) -> bool {
        self.added > 0 || self.updated > 0 || self.removed > 0
    }

    pub fn is_noop(&self) -> bool {
        !self.has_changes()
    }

    pub fn change_count(&self) -> usize {
        self.changes.len()
    }
}

#[derive(Debug, Clone)]
pub enum MexcManagedHealOutcome {
    Healthy,
    Suppressed {
        alerts_before_heal: MexcLiveKindAlertReport,
        cooldown_remaining_ms: u128,
        total_rebuilds: usize,
    },
    Healed(MexcManagedHealReport),
}

#[derive(Debug, Clone)]
pub struct MexcManagedRuntimePolicy {
    pub recovery_wait_for: Duration,
    pub use_default_health_policy: bool,
    pub health_max_age: Duration,
    pub health_min_stale_for: Duration,
    pub public_metadata_refresh_interval: Option<Duration>,
    pub contract_refresh_interval: Option<Duration>,
    pub contract_event_refresh: bool,
    pub contract_event_refresh_cooldown: Duration,
    pub contract_gap_refresh: bool,
    pub contract_gap_refresh_cooldown: Duration,
    pub contract_gap_refresh_batch_size: usize,
    pub heal_startup_wait_for: Duration,
    pub heal_cooldown: Duration,
    pub auto_heal: bool,
}

impl MexcManagedRuntimePolicy {
    pub fn balanced_defaults() -> Self {
        Self {
            recovery_wait_for: Duration::from_secs(30),
            use_default_health_policy: true,
            health_max_age: Duration::from_secs(5),
            health_min_stale_for: Duration::from_secs(5),
            public_metadata_refresh_interval: Some(Duration::from_secs(15 * 60)),
            contract_refresh_interval: Some(Duration::from_secs(15 * 60)),
            contract_event_refresh: true,
            contract_event_refresh_cooldown: Duration::from_secs(2),
            contract_gap_refresh: true,
            contract_gap_refresh_cooldown: Duration::from_secs(30),
            contract_gap_refresh_batch_size: 16,
            heal_startup_wait_for: Duration::from_secs(30),
            heal_cooldown: Duration::from_secs(60),
            auto_heal: false,
        }
    }
}

const READY_CONTRACT_GAP_WARMUP_MAX_PASSES: usize = 8;
const READY_CONTRACT_GAP_WARMUP_BATCH_SIZE: usize = 32;

#[derive(Debug, Clone)]
pub struct MexcManagedRuntimeStep {
    pub event: MexcPublicEvent,
    pub pending_reset: Option<MexcRuntimeResetReport>,
    pub health_alerts: MexcLiveKindAlertReport,
    pub public_metadata_refresh: Option<MexcPublicMetadataRefreshReport>,
    pub contract_refresh: Option<MexcManagedContractRefreshReport>,
    pub heal_outcome: Option<MexcManagedHealOutcome>,
}

#[derive(Debug, Clone)]
pub struct MexcLiveKindHealth {
    pub last_seen_ago_ms: Option<u128>,
    pub max_age_ms: u128,
    pub stale: bool,
}

#[derive(Debug, Clone)]
pub struct MexcLiveKindHealthReport {
    pub expected: Vec<MexcPublicEventKind>,
    pub health: BTreeMap<MexcPublicEventKind, MexcLiveKindHealth>,
    pub stale: Vec<MexcPublicEventKind>,
}

impl MexcLiveKindHealthReport {
    pub fn is_healthy(&self) -> bool {
        self.stale.is_empty()
    }

    pub fn healthy_count(&self) -> usize {
        self.expected.len().saturating_sub(self.stale.len())
    }

    pub fn expected_count(&self) -> usize {
        self.expected.len()
    }
}

#[derive(Debug, Clone)]
pub struct MexcLiveKindHealthAlert {
    pub kind: MexcPublicEventKind,
    pub stale_for_ms: u128,
    pub last_seen_ago_ms: Option<u128>,
    pub max_age_ms: u128,
}

#[derive(Debug, Clone)]
pub struct MexcLiveKindRecovery {
    pub kind: MexcPublicEventKind,
    pub stale_for_ms: u128,
}

#[derive(Debug, Clone)]
pub struct MexcLiveKindAlertReport {
    pub health: MexcLiveKindHealthReport,
    pub newly_stale: Vec<MexcLiveKindHealthAlert>,
    pub persistent_stale: Vec<MexcLiveKindHealthAlert>,
    pub recovered: Vec<MexcLiveKindRecovery>,
    pub min_stale_for_ms: u128,
}

impl MexcStatefulPublicRuntime {
    pub async fn next_event(&mut self) -> Option<Result<MexcPublicEvent>> {
        match self.stream.next().await {
            Some(Ok(event)) => {
                match &event {
                    MexcPublicEvent::Spot(MexcSpotWsMessage::SessionStart(session))
                        if session.resumed =>
                    {
                        self.spot_session = Some(*session);
                        self.pending_spot_resets += 1;
                        self.total_spot_resets += 1;
                        self.spot_recovery_seen.clear();
                        self.last_seen_at_by_kind.retain(|kind, _| !kind.is_spot());
                        self.stale_since_by_kind.retain(|kind, _| !kind.is_spot());
                        self.state.reset_after_spot_session_restart();
                    }
                    MexcPublicEvent::Futures(MexcFuturesWsMessage::SessionStart(session))
                        if session.resumed =>
                    {
                        self.futures_session = Some(*session);
                        self.pending_futures_resets += 1;
                        self.total_futures_resets += 1;
                        self.futures_recovery_seen.clear();
                        self.last_seen_at_by_kind
                            .retain(|kind, _| !kind.is_futures());
                        self.stale_since_by_kind
                            .retain(|kind, _| !kind.is_futures());
                        self.state.reset_after_futures_session_restart();
                    }
                    MexcPublicEvent::Spot(MexcSpotWsMessage::SessionStart(session)) => {
                        self.spot_session = Some(*session);
                        self.spot_recovery_seen.clear();
                        self.last_seen_at_by_kind.retain(|kind, _| !kind.is_spot());
                        self.stale_since_by_kind.retain(|kind, _| !kind.is_spot());
                    }
                    MexcPublicEvent::Futures(MexcFuturesWsMessage::SessionStart(session)) => {
                        self.futures_session = Some(*session);
                        self.futures_recovery_seen.clear();
                        self.last_seen_at_by_kind
                            .retain(|kind, _| !kind.is_futures());
                        self.stale_since_by_kind
                            .retain(|kind, _| !kind.is_futures());
                    }
                    _ => {}
                }
                self.state.apply_event(&event);
                if let Some(kind) = event.kind() {
                    self.last_seen_at_by_kind.insert(kind, Instant::now());
                    if SPOT_RECOVERY_KINDS.contains(&kind) {
                        self.spot_recovery_seen.insert(kind);
                    }
                    if FUTURES_RECOVERY_KINDS.contains(&kind) {
                        self.futures_recovery_seen.insert(kind);
                    }
                }
                Some(Ok(event))
            }
            Some(Err(error)) => Some(Err(error)),
            None => None,
        }
    }

    pub async fn next_live_event(&mut self) -> Option<Result<MexcPublicEvent>> {
        loop {
            match self.next_event().await {
                Some(Ok(event)) if event.is_live_payload() => return Some(Ok(event)),
                Some(Ok(_)) => continue,
                Some(Err(error)) => return Some(Err(error)),
                None => return None,
            }
        }
    }

    pub async fn wait_for_live_event_kind(
        &mut self,
        kind: MexcPublicEventKind,
        wait_for: Duration,
    ) -> Result<Option<MexcPublicEvent>> {
        let deadline = Instant::now() + wait_for;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }

            let remaining = deadline.saturating_duration_since(now);
            match timeout(remaining, self.next_live_event()).await {
                Err(_) => return Ok(None),
                Ok(Some(Ok(event))) if event.kind() == Some(kind) => return Ok(Some(event)),
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(error))) => return Err(error),
                Ok(None) => return Ok(None),
            }
        }
    }

    pub async fn wait_for_live_event_kinds(
        &mut self,
        kinds: &[MexcPublicEventKind],
        wait_for: Duration,
    ) -> Result<MexcLiveKindWaitReport> {
        let deadline = Instant::now() + wait_for;
        let expected = kinds.to_vec();
        let expected_set = expected.iter().copied().collect::<BTreeSet<_>>();
        let mut seen = BTreeSet::new();
        let mut observed_live_events = 0usize;

        while Instant::now() < deadline && seen.len() < expected.len() {
            let now = Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            match timeout(remaining, self.next_live_event()).await {
                Err(_) => break,
                Ok(Some(Ok(event))) => {
                    observed_live_events += 1;
                    if let Some(kind) = event.kind() {
                        if expected_set.contains(&kind) {
                            seen.insert(kind);
                        }
                    }
                }
                Ok(Some(Err(error))) => return Err(error),
                Ok(None) => break,
            }
        }

        let missing = expected
            .iter()
            .copied()
            .filter(|kind| !seen.contains(kind))
            .collect::<Vec<_>>();

        Ok(MexcLiveKindWaitReport {
            expected,
            seen,
            missing,
            observed_live_events,
        })
    }

    pub async fn await_balanced_startup(
        &mut self,
        wait_for: Duration,
    ) -> Result<MexcLiveKindWaitReport> {
        self.wait_for_live_event_kinds(BALANCED_STARTUP_KINDS, wait_for)
            .await
    }

    pub async fn await_recovery_after_pending_reset(
        &mut self,
        wait_for: Duration,
    ) -> Result<Option<MexcRuntimeRecoveryReport>> {
        let Some(reset) = self.take_pending_reset_report() else {
            return Ok(None);
        };

        let mut expected = Vec::new();
        let mut already_seen = BTreeSet::new();
        if reset.spot_resets_detected > 0 {
            expected.extend_from_slice(SPOT_RECOVERY_KINDS);
            already_seen.extend(self.spot_recovery_seen.iter().copied());
        }
        if reset.futures_resets_detected > 0 {
            expected.extend_from_slice(FUTURES_RECOVERY_KINDS);
            already_seen.extend(self.futures_recovery_seen.iter().copied());
        }

        let mut dedup = BTreeSet::new();
        expected.retain(|kind| dedup.insert(*kind));

        let missing_before_wait = expected
            .iter()
            .copied()
            .filter(|kind| !already_seen.contains(kind))
            .collect::<Vec<_>>();

        let waited = if missing_before_wait.is_empty() {
            MexcLiveKindWaitReport {
                expected: expected.clone(),
                seen: already_seen.clone(),
                missing: Vec::new(),
                observed_live_events: 0,
            }
        } else {
            self.wait_for_live_event_kinds(&missing_before_wait, wait_for)
                .await?
        };

        let mut seen = already_seen;
        seen.extend(waited.seen.iter().copied());
        let missing = expected
            .iter()
            .copied()
            .filter(|kind| !seen.contains(kind))
            .collect::<Vec<_>>();
        let readiness = MexcLiveKindWaitReport {
            expected,
            seen,
            missing,
            observed_live_events: waited.observed_live_events,
        };
        Ok(Some(MexcRuntimeRecoveryReport { reset, readiness }))
    }

    pub async fn observe_live_event_kinds(
        &mut self,
        kinds: &[MexcPublicEventKind],
        window: Duration,
    ) -> Result<MexcLiveKindObservationReport> {
        let started_at = Instant::now();
        let deadline = started_at + window;
        let expected = kinds.to_vec();
        let expected_set = expected.iter().copied().collect::<BTreeSet<_>>();
        let mut observations = expected
            .iter()
            .copied()
            .map(|kind| (kind, MexcLiveKindObservation::default()))
            .collect::<BTreeMap<_, _>>();
        let mut observed_live_events = 0usize;

        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match timeout(remaining, self.next_live_event()).await {
                Err(_) => break,
                Ok(Some(Ok(event))) => {
                    observed_live_events += 1;
                    let Some(kind) = event.kind() else {
                        continue;
                    };
                    if !expected_set.contains(&kind) {
                        continue;
                    }

                    let since_start_ms = started_at.elapsed().as_millis();
                    let entry = observations
                        .get_mut(&kind)
                        .expect("expected observation entry to exist");
                    entry.count += 1;
                    if entry.first_seen_after_ms.is_none() {
                        entry.first_seen_after_ms = Some(since_start_ms);
                    }
                    entry.last_seen_after_ms = Some(since_start_ms);
                }
                Ok(Some(Err(error))) => return Err(error),
                Ok(None) => break,
            }
        }

        let missing = expected
            .iter()
            .copied()
            .filter(|kind| observations.get(kind).map(|item| item.count).unwrap_or(0) == 0)
            .collect::<Vec<_>>();

        Ok(MexcLiveKindObservationReport {
            expected,
            observations,
            missing,
            observed_live_events,
            window_ms: window.as_millis(),
        })
    }

    pub fn state(&self) -> &MexcPublicState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut MexcPublicState {
        &mut self.state
    }

    pub fn session_status(&self) -> MexcRuntimeSessionStatus {
        MexcRuntimeSessionStatus {
            spot_session: self.spot_session,
            futures_session: self.futures_session,
            pending_spot_resets: self.pending_spot_resets,
            pending_futures_resets: self.pending_futures_resets,
            total_spot_resets: self.total_spot_resets,
            total_futures_resets: self.total_futures_resets,
        }
    }

    pub fn take_pending_reset_report(&mut self) -> Option<MexcRuntimeResetReport> {
        if self.pending_spot_resets == 0 && self.pending_futures_resets == 0 {
            return None;
        }

        let report = MexcRuntimeResetReport {
            spot_session: self.spot_session,
            futures_session: self.futures_session,
            spot_resets_detected: self.pending_spot_resets,
            futures_resets_detected: self.pending_futures_resets,
        };
        self.pending_spot_resets = 0;
        self.pending_futures_resets = 0;
        Some(report)
    }

    pub fn live_kind_health_status(
        &self,
        kinds: &[MexcPublicEventKind],
        max_age: Duration,
    ) -> MexcLiveKindHealthReport {
        let mut expected = kinds.to_vec();
        let mut dedup = BTreeSet::new();
        expected.retain(|kind| dedup.insert(*kind));

        let now = Instant::now();
        let mut health = BTreeMap::new();
        let mut stale = Vec::new();
        for kind in &expected {
            let last_seen_ago_ms = self
                .last_seen_at_by_kind
                .get(kind)
                .map(|seen_at| now.saturating_duration_since(*seen_at).as_millis());
            let is_stale = last_seen_ago_ms
                .map(|age_ms| age_ms > max_age.as_millis())
                .unwrap_or(true);
            if is_stale {
                stale.push(*kind);
            }
            health.insert(
                *kind,
                MexcLiveKindHealth {
                    last_seen_ago_ms,
                    max_age_ms: max_age.as_millis(),
                    stale: is_stale,
                },
            );
        }

        MexcLiveKindHealthReport {
            expected,
            health,
            stale,
        }
    }

    pub fn balanced_health_status(&self, max_age: Duration) -> MexcLiveKindHealthReport {
        self.live_kind_health_status(BALANCED_HEALTH_KINDS, max_age)
    }

    pub fn balanced_health_status_with_defaults(&self) -> MexcLiveKindHealthReport {
        let mut expected = BALANCED_HEALTH_KINDS.to_vec();
        let mut dedup = BTreeSet::new();
        expected.retain(|kind| dedup.insert(*kind));

        let now = Instant::now();
        let mut health = BTreeMap::new();
        let mut stale = Vec::new();
        for kind in &expected {
            let max_age = kind.recommended_max_silence();
            let last_seen_ago_ms = self
                .last_seen_at_by_kind
                .get(kind)
                .map(|seen_at| now.saturating_duration_since(*seen_at).as_millis());
            let is_stale = last_seen_ago_ms
                .map(|age_ms| age_ms > max_age.as_millis())
                .unwrap_or(true);
            if is_stale {
                stale.push(*kind);
            }
            health.insert(
                *kind,
                MexcLiveKindHealth {
                    last_seen_ago_ms,
                    max_age_ms: max_age.as_millis(),
                    stale: is_stale,
                },
            );
        }

        MexcLiveKindHealthReport {
            expected,
            health,
            stale,
        }
    }

    fn poll_alerts_from_health(
        &mut self,
        health: MexcLiveKindHealthReport,
        min_stale_for: Duration,
    ) -> MexcLiveKindAlertReport {
        let now = Instant::now();
        let mut newly_stale = Vec::new();
        let mut persistent_stale = Vec::new();
        let mut recovered = Vec::new();

        let stale_set = health.stale.iter().copied().collect::<BTreeSet<_>>();
        for kind in &health.expected {
            let entry = health
                .health
                .get(kind)
                .expect("health entry to exist for expected kind");
            if entry.stale {
                let stale_since = match self.stale_since_by_kind.entry(*kind) {
                    Entry::Occupied(occupied) => *occupied.get(),
                    Entry::Vacant(vacant) => {
                        newly_stale.push(MexcLiveKindHealthAlert {
                            kind: *kind,
                            stale_for_ms: 0,
                            last_seen_ago_ms: entry.last_seen_ago_ms,
                            max_age_ms: entry.max_age_ms,
                        });
                        *vacant.insert(now)
                    }
                };
                let stale_for_ms = now.saturating_duration_since(stale_since).as_millis();
                if stale_for_ms >= min_stale_for.as_millis() {
                    persistent_stale.push(MexcLiveKindHealthAlert {
                        kind: *kind,
                        stale_for_ms,
                        last_seen_ago_ms: entry.last_seen_ago_ms,
                        max_age_ms: entry.max_age_ms,
                    });
                }
            } else if let Some(stale_since) = self.stale_since_by_kind.remove(kind) {
                recovered.push(MexcLiveKindRecovery {
                    kind: *kind,
                    stale_for_ms: now.saturating_duration_since(stale_since).as_millis(),
                });
            }
        }

        self.stale_since_by_kind
            .retain(|kind, _| stale_set.contains(kind));

        MexcLiveKindAlertReport {
            health,
            newly_stale,
            persistent_stale,
            recovered,
            min_stale_for_ms: min_stale_for.as_millis(),
        }
    }

    pub fn poll_live_kind_health_alerts(
        &mut self,
        kinds: &[MexcPublicEventKind],
        max_age: Duration,
        min_stale_for: Duration,
    ) -> MexcLiveKindAlertReport {
        let health = self.live_kind_health_status(kinds, max_age);
        self.poll_alerts_from_health(health, min_stale_for)
    }

    pub fn poll_balanced_health_alerts(
        &mut self,
        max_age: Duration,
        min_stale_for: Duration,
    ) -> MexcLiveKindAlertReport {
        let health = self.balanced_health_status(max_age);
        self.poll_alerts_from_health(health, min_stale_for)
    }

    pub fn poll_balanced_health_alerts_with_defaults(
        &mut self,
        min_stale_for: Duration,
    ) -> MexcLiveKindAlertReport {
        let health = self.balanced_health_status_with_defaults();
        self.poll_alerts_from_health(health, min_stale_for)
    }
}

impl MexcManagedStatefulPublicRuntime {
    pub fn manifest(&self) -> &MexcPublicRuntimeManifest {
        &self.inner.manifest
    }

    pub fn state(&self) -> &MexcPublicState {
        self.inner.state()
    }

    pub fn state_handoff_report(&self) -> MexcPublicStateHandoffReport {
        self.inner.state().handoff_report()
    }

    pub fn state_mut(&mut self) -> &mut MexcPublicState {
        self.inner.state_mut()
    }

    pub fn deep_report(&self) -> Option<&MexcFuturesReferenceBootstrapReport> {
        self.inner.deep_report.as_ref()
    }

    pub fn session_status(&self) -> MexcRuntimeSessionStatus {
        self.inner.session_status()
    }

    pub fn total_rebuilds(&self) -> usize {
        self.total_rebuilds
    }

    pub fn pending_contract_refresh_symbols(&self) -> Vec<String> {
        self.pending_contract_refresh_symbols
            .iter()
            .cloned()
            .collect()
    }

    pub fn last_rebuild_ago(&self) -> Option<Duration> {
        self.last_rebuild_at
            .map(|rebuilt_at| Instant::now().saturating_duration_since(rebuilt_at))
    }

    pub fn last_contract_refresh_ago(&self) -> Option<Duration> {
        self.last_contract_refresh_at
            .map(|refreshed_at| Instant::now().saturating_duration_since(refreshed_at))
    }

    pub fn last_public_metadata_refresh_ago(&self) -> Option<Duration> {
        self.last_public_metadata_refresh_at
            .map(|refreshed_at| Instant::now().saturating_duration_since(refreshed_at))
    }

    pub fn take_pending_reset_report(&mut self) -> Option<MexcRuntimeResetReport> {
        self.inner.take_pending_reset_report()
    }

    pub fn balanced_health_status_with_defaults(&self) -> MexcLiveKindHealthReport {
        self.inner.balanced_health_status_with_defaults()
    }

    pub fn poll_balanced_health_alerts_with_defaults(
        &mut self,
        min_stale_for: Duration,
    ) -> MexcLiveKindAlertReport {
        self.inner
            .poll_balanced_health_alerts_with_defaults(min_stale_for)
    }

    pub fn poll_balanced_health_alerts(
        &mut self,
        max_age: Duration,
        min_stale_for: Duration,
    ) -> MexcLiveKindAlertReport {
        self.inner
            .poll_balanced_health_alerts(max_age, min_stale_for)
    }

    pub async fn next_event(&mut self) -> Option<Result<MexcPublicEvent>> {
        self.inner.next_event().await
    }

    pub async fn next_live_event(&mut self) -> Option<Result<MexcPublicEvent>> {
        self.inner.next_live_event().await
    }

    pub async fn await_balanced_startup(
        &mut self,
        wait_for: Duration,
    ) -> Result<MexcLiveKindWaitReport> {
        self.inner.await_balanced_startup(wait_for).await
    }

    pub async fn warm_futures_contract_gaps(
        &mut self,
        max_passes: usize,
        batch_size: usize,
    ) -> Result<MexcManagedContractGapWarmupReport> {
        let initial_gap_symbols = self.contract_gap_symbols();
        if initial_gap_symbols.is_empty() {
            return Ok(MexcManagedContractGapWarmupReport {
                initial_gap_symbols,
                final_gap_symbols: Vec::new(),
                passes: 0,
                refreshes: Vec::new(),
                stop_reason: MexcManagedContractGapWarmupStopReason::NoGaps,
            });
        }

        let batch_size = batch_size.max(1);
        let mut previous_gap_count = initial_gap_symbols.len();
        let mut refreshes = Vec::new();
        let mut stop_reason = MexcManagedContractGapWarmupStopReason::MaxPasses;

        for _ in 0..max_passes {
            let Some(report) = self
                .refresh_futures_contract_gap_symbols_with_report(batch_size)
                .await?
            else {
                stop_reason = MexcManagedContractGapWarmupStopReason::NoGaps;
                break;
            };

            let current_gap_count = self.contract_gap_symbols().len();
            let made_progress = current_gap_count < previous_gap_count;
            refreshes.push(report);

            if current_gap_count == 0 {
                stop_reason = MexcManagedContractGapWarmupStopReason::Converged;
                break;
            }
            if !made_progress {
                stop_reason = MexcManagedContractGapWarmupStopReason::Stalled;
                break;
            }

            previous_gap_count = current_gap_count;
        }

        let final_gap_symbols = self.contract_gap_symbols();
        Ok(MexcManagedContractGapWarmupReport {
            initial_gap_symbols,
            final_gap_symbols,
            passes: refreshes.len(),
            refreshes,
            stop_reason,
        })
    }

    pub async fn await_recovery_after_pending_reset(
        &mut self,
        wait_for: Duration,
    ) -> Result<Option<MexcRuntimeRecoveryReport>> {
        self.inner
            .await_recovery_after_pending_reset(wait_for)
            .await
    }

    pub async fn refresh_futures_contract_snapshot_with_report(
        &mut self,
    ) -> Result<MexcManagedContractRefreshReport> {
        let contracts = self.builder.refresh_futures_contract_snapshot().await?;
        let report = self
            .inner
            .state_mut()
            .hydrate_futures_contract_snapshot(&contracts);
        self.last_contract_refresh_at = Some(Instant::now());
        self.pending_contract_refresh_symbols.clear();
        Ok(MexcManagedContractRefreshReport {
            cause: MexcManagedContractRefreshCause::Interval,
            requested_symbols: Vec::new(),
            unresolved_requested_symbols: Vec::new(),
            used_full_snapshot_fallback: false,
            refreshed_contracts: report.refreshed_contracts,
            added: report.added,
            added_symbols: report.added_symbols,
            updated: report.updated,
            updated_symbols: report.updated_symbols,
            removed: report.removed,
            removed_symbols: report.removed_symbols,
            unchanged: report.unchanged,
            changes: report.changes,
        })
    }

    pub async fn refresh_futures_contract_snapshot(&mut self) -> Result<usize> {
        Ok(self
            .refresh_futures_contract_snapshot_with_report()
            .await?
            .refreshed_contracts)
    }

    pub async fn refresh_futures_contract_snapshot_if_due(
        &mut self,
        interval: Duration,
    ) -> Result<Option<MexcManagedContractRefreshReport>> {
        if let Some(last_refresh_ago) = self.last_contract_refresh_ago() {
            if last_refresh_ago < interval {
                return Ok(None);
            }
        }

        Ok(Some(
            self.refresh_futures_contract_snapshot_with_report().await?,
        ))
    }

    pub async fn refresh_futures_contract_symbols_with_report(
        &mut self,
        symbols: &[String],
    ) -> Result<MexcManagedContractRefreshReport> {
        self.refresh_futures_contract_symbols_with_report_for_cause(
            symbols,
            MexcManagedContractRefreshCause::Targeted,
            true,
        )
        .await
    }

    pub async fn refresh_futures_contract_gap_symbols_with_report(
        &mut self,
        max_symbols: usize,
    ) -> Result<Option<MexcManagedContractRefreshReport>> {
        let symbols = self
            .contract_gap_symbols()
            .into_iter()
            .take(max_symbols)
            .collect::<Vec<_>>();
        if symbols.is_empty() {
            return Ok(None);
        }
        Ok(Some(
            self.refresh_futures_contract_symbols_with_report_for_cause(
                &symbols,
                MexcManagedContractRefreshCause::GapBackfill,
                false,
            )
            .await?,
        ))
    }

    fn contract_gap_symbols(&self) -> Vec<String> {
        self.state_handoff_report()
            .futures
            .orphan_symbols_with_other_state_list
    }

    fn contract_gap_refresh_symbols_if_due(
        &self,
        cooldown: Duration,
        max_symbols: usize,
    ) -> Option<Vec<String>> {
        if max_symbols == 0 {
            return None;
        }
        if let Some(last_refresh_ago) = self.last_contract_refresh_ago() {
            if last_refresh_ago < cooldown {
                return None;
            }
        }
        let symbols = self
            .contract_gap_symbols()
            .into_iter()
            .take(max_symbols)
            .collect::<Vec<_>>();
        if symbols.is_empty() {
            None
        } else {
            Some(symbols)
        }
    }

    async fn refresh_futures_contract_symbols_with_report_for_cause(
        &mut self,
        symbols: &[String],
        cause: MexcManagedContractRefreshCause,
        allow_full_snapshot_fallback: bool,
    ) -> Result<MexcManagedContractRefreshReport> {
        if symbols.is_empty() {
            return Err(anyhow!(
                "at least one symbol is required for targeted futures contract refresh"
            ));
        }

        let requested_symbols = symbols
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let targeted_contracts = self
            .builder
            .refresh_futures_contract_symbols(&requested_symbols)
            .await;

        let (report, used_full_snapshot_fallback) = match targeted_contracts {
            Ok(contracts) => {
                let refreshed_symbols = contracts
                    .iter()
                    .map(|item| item.symbol.clone())
                    .collect::<BTreeSet<_>>();
                if allow_full_snapshot_fallback
                    && (contracts.is_empty()
                        || refreshed_symbols.len() != requested_symbols.len()
                        || !requested_symbols
                            .iter()
                            .all(|symbol| refreshed_symbols.contains(symbol)))
                {
                    let snapshot = self.builder.refresh_futures_contract_snapshot().await?;
                    (
                        self.inner
                            .state_mut()
                            .hydrate_futures_contract_snapshot(&snapshot),
                        true,
                    )
                } else {
                    (
                        self.inner
                            .state_mut()
                            .hydrate_futures_contract_updates(&contracts),
                        false,
                    )
                }
            }
            Err(error) => {
                let snapshot = self.builder.refresh_futures_contract_snapshot().await.map_err(
                    |snapshot_error| {
                        anyhow!(
                            "targeted futures contract refresh failed for {:?}: {error:#}; full snapshot fallback also failed: {snapshot_error:#}",
                            requested_symbols
                        )
                    },
                )?;
                (
                    self.inner
                        .state_mut()
                        .hydrate_futures_contract_snapshot(&snapshot),
                    true,
                )
            }
        };

        let unresolved_requested_symbols = requested_symbols
            .iter()
            .filter(|symbol| {
                self.inner
                    .state()
                    .futures_symbols
                    .get(*symbol)
                    .and_then(|item| item.contract.as_ref())
                    .is_none()
            })
            .cloned()
            .collect::<Vec<_>>();

        self.last_contract_refresh_at = Some(Instant::now());
        if used_full_snapshot_fallback {
            self.pending_contract_refresh_symbols.clear();
        } else {
            for symbol in &requested_symbols {
                self.pending_contract_refresh_symbols.remove(symbol);
            }
        }

        Ok(MexcManagedContractRefreshReport {
            cause,
            requested_symbols,
            unresolved_requested_symbols,
            used_full_snapshot_fallback,
            refreshed_contracts: report.refreshed_contracts,
            added: report.added,
            added_symbols: report.added_symbols,
            updated: report.updated,
            updated_symbols: report.updated_symbols,
            removed: report.removed,
            removed_symbols: report.removed_symbols,
            unchanged: report.unchanged,
            changes: report.changes,
        })
    }

    pub async fn refresh_public_metadata_snapshot_with_report(
        &mut self,
    ) -> Result<MexcPublicMetadataRefreshReport> {
        let snapshot = self.builder.refresh_public_metadata_snapshot().await?;
        let report = self
            .inner
            .state_mut()
            .hydrate_public_metadata_snapshot(&snapshot);
        self.last_public_metadata_refresh_at = Some(Instant::now());
        Ok(report)
    }

    pub async fn refresh_public_metadata_snapshot(
        &mut self,
    ) -> Result<MexcPublicMetadataRefreshReport> {
        self.refresh_public_metadata_snapshot_with_report().await
    }

    pub async fn refresh_public_metadata_snapshot_if_due(
        &mut self,
        interval: Duration,
    ) -> Result<Option<MexcPublicMetadataRefreshReport>> {
        if let Some(last_refresh_ago) = self.last_public_metadata_refresh_ago() {
            if last_refresh_ago < interval {
                return Ok(None);
            }
        }

        Ok(Some(
            self.refresh_public_metadata_snapshot_with_report().await?,
        ))
    }

    async fn rebuild_inner(&self) -> Result<MexcStatefulPublicRuntime> {
        match self.snapshot_mode {
            MexcManagedSnapshotMode::Snapshot => {
                self.builder
                    .connect_stateful_with_snapshot(self.config.clone())
                    .await
            }
            MexcManagedSnapshotMode::DeepSnapshot { concurrency } => {
                self.builder
                    .connect_stateful_with_deep_snapshot(self.config.clone(), concurrency)
                    .await
            }
        }
    }

    pub async fn rebuild_and_await_balanced_startup(
        &mut self,
        wait_for: Duration,
    ) -> Result<(MexcLiveKindWaitReport, MexcManagedContractGapWarmupReport)> {
        let mut rebuilt = self.rebuild_inner().await?;
        let startup = rebuilt.await_balanced_startup(wait_for).await?;
        self.inner = rebuilt;
        self.total_rebuilds += 1;
        let now = Instant::now();
        self.last_rebuild_at = Some(now);
        self.last_contract_refresh_at = Some(now);
        self.pending_contract_refresh_symbols.clear();
        let contract_gap_warmup = self
            .warm_futures_contract_gaps(
                READY_CONTRACT_GAP_WARMUP_MAX_PASSES,
                READY_CONTRACT_GAP_WARMUP_BATCH_SIZE,
            )
            .await?;
        Ok((startup, contract_gap_warmup))
    }

    fn queue_contract_refresh_signal_from_event(&mut self, event: &MexcPublicEvent) {
        if let Some(symbol) = contract_refresh_signal_symbol(event) {
            self.pending_contract_refresh_symbols.insert(symbol);
        }
    }

    fn pending_contract_refresh_symbols_if_due(&self, cooldown: Duration) -> Option<Vec<String>> {
        if self.pending_contract_refresh_symbols.is_empty() {
            return None;
        }
        if let Some(last_refresh_ago) = self.last_contract_refresh_ago() {
            if last_refresh_ago < cooldown {
                return None;
            }
        }
        Some(
            self.pending_contract_refresh_symbols
                .iter()
                .cloned()
                .collect(),
        )
    }

    async fn heal_from_alerts_with_cooldown(
        &mut self,
        alerts: MexcLiveKindAlertReport,
        startup_wait_for: Duration,
        rebuild_cooldown: Duration,
    ) -> Result<MexcManagedHealOutcome> {
        if alerts.persistent_stale.is_empty() {
            return Ok(MexcManagedHealOutcome::Healthy);
        }

        if let Some(last_rebuild_ago) = self.last_rebuild_ago() {
            if last_rebuild_ago < rebuild_cooldown {
                return Ok(MexcManagedHealOutcome::Suppressed {
                    alerts_before_heal: alerts,
                    cooldown_remaining_ms: rebuild_cooldown
                        .saturating_sub(last_rebuild_ago)
                        .as_millis(),
                    total_rebuilds: self.total_rebuilds,
                });
            }
        }

        let startup_after_heal = self
            .rebuild_and_await_balanced_startup(startup_wait_for)
            .await?;
        Ok(MexcManagedHealOutcome::Healed(MexcManagedHealReport {
            alerts_before_heal: alerts,
            startup_after_heal: startup_after_heal.0,
            total_rebuilds: self.total_rebuilds,
        }))
    }

    pub async fn heal_if_persistent_balanced_stale_with_defaults(
        &mut self,
        min_stale_for: Duration,
        startup_wait_for: Duration,
    ) -> Result<Option<MexcManagedHealReport>> {
        let alerts = self.poll_balanced_health_alerts_with_defaults(min_stale_for);
        if alerts.persistent_stale.is_empty() {
            return Ok(None);
        }

        let startup_after_heal = self
            .rebuild_and_await_balanced_startup(startup_wait_for)
            .await?;
        Ok(Some(MexcManagedHealReport {
            alerts_before_heal: alerts,
            startup_after_heal: startup_after_heal.0,
            total_rebuilds: self.total_rebuilds,
        }))
    }

    pub async fn heal_if_persistent_balanced_stale(
        &mut self,
        max_age: Duration,
        min_stale_for: Duration,
        startup_wait_for: Duration,
    ) -> Result<Option<MexcManagedHealReport>> {
        let alerts = self.poll_balanced_health_alerts(max_age, min_stale_for);
        if alerts.persistent_stale.is_empty() {
            return Ok(None);
        }

        let startup_after_heal = self
            .rebuild_and_await_balanced_startup(startup_wait_for)
            .await?;
        Ok(Some(MexcManagedHealReport {
            alerts_before_heal: alerts,
            startup_after_heal: startup_after_heal.0,
            total_rebuilds: self.total_rebuilds,
        }))
    }

    pub async fn heal_if_persistent_balanced_stale_with_defaults_and_cooldown(
        &mut self,
        min_stale_for: Duration,
        startup_wait_for: Duration,
        rebuild_cooldown: Duration,
    ) -> Result<MexcManagedHealOutcome> {
        let alerts = self.poll_balanced_health_alerts_with_defaults(min_stale_for);
        self.heal_from_alerts_with_cooldown(alerts, startup_wait_for, rebuild_cooldown)
            .await
    }

    pub async fn heal_if_persistent_balanced_stale_and_cooldown(
        &mut self,
        max_age: Duration,
        min_stale_for: Duration,
        startup_wait_for: Duration,
        rebuild_cooldown: Duration,
    ) -> Result<MexcManagedHealOutcome> {
        let alerts = self.poll_balanced_health_alerts(max_age, min_stale_for);
        self.heal_from_alerts_with_cooldown(alerts, startup_wait_for, rebuild_cooldown)
            .await
    }

    pub async fn next_step(
        &mut self,
        policy: &MexcManagedRuntimePolicy,
    ) -> Option<Result<MexcManagedRuntimeStep>> {
        let event = match self.next_event().await {
            Some(Ok(event)) => event,
            Some(Err(error)) => return Some(Err(error)),
            None => return None,
        };

        if policy.contract_event_refresh {
            self.queue_contract_refresh_signal_from_event(&event);
        }

        let pending_reset = self.take_pending_reset_report();
        let health_alerts = if policy.use_default_health_policy {
            self.poll_balanced_health_alerts_with_defaults(policy.health_min_stale_for)
        } else {
            self.poll_balanced_health_alerts(policy.health_max_age, policy.health_min_stale_for)
        };
        let heal_outcome = if policy.auto_heal {
            let outcome = self
                .heal_from_alerts_with_cooldown(
                    health_alerts.clone(),
                    policy.heal_startup_wait_for,
                    policy.heal_cooldown,
                )
                .await;
            match outcome {
                Ok(MexcManagedHealOutcome::Healthy) => None,
                Ok(other) => Some(other),
                Err(error) => return Some(Err(error)),
            }
        } else {
            None
        };
        let public_metadata_refresh = if heal_outcome.is_some() {
            None
        } else if let Some(interval) = policy.public_metadata_refresh_interval {
            match self.refresh_public_metadata_snapshot_if_due(interval).await {
                Ok(report) => report,
                Err(error) => return Some(Err(error)),
            }
        } else {
            None
        };
        let contract_refresh = if heal_outcome.is_some() {
            None
        } else if policy.contract_event_refresh {
            if let Some(symbols) =
                self.pending_contract_refresh_symbols_if_due(policy.contract_event_refresh_cooldown)
            {
                match self
                    .refresh_futures_contract_symbols_with_report(&symbols)
                    .await
                {
                    Ok(report) => Some(report),
                    Err(error) => return Some(Err(error)),
                }
            } else if policy.contract_gap_refresh {
                if let Some(symbols) = self.contract_gap_refresh_symbols_if_due(
                    policy.contract_gap_refresh_cooldown,
                    policy.contract_gap_refresh_batch_size,
                ) {
                    match self
                        .refresh_futures_contract_symbols_with_report_for_cause(
                            &symbols,
                            MexcManagedContractRefreshCause::GapBackfill,
                            false,
                        )
                        .await
                    {
                        Ok(report) => Some(report),
                        Err(error) => return Some(Err(error)),
                    }
                } else if let Some(interval) = policy.contract_refresh_interval {
                    match self
                        .refresh_futures_contract_snapshot_if_due(interval)
                        .await
                    {
                        Ok(report) => report,
                        Err(error) => return Some(Err(error)),
                    }
                } else {
                    None
                }
            } else if let Some(interval) = policy.contract_refresh_interval {
                match self
                    .refresh_futures_contract_snapshot_if_due(interval)
                    .await
                {
                    Ok(report) => report,
                    Err(error) => return Some(Err(error)),
                }
            } else {
                None
            }
        } else if policy.contract_gap_refresh {
            if let Some(symbols) = self.contract_gap_refresh_symbols_if_due(
                policy.contract_gap_refresh_cooldown,
                policy.contract_gap_refresh_batch_size,
            ) {
                match self
                    .refresh_futures_contract_symbols_with_report_for_cause(
                        &symbols,
                        MexcManagedContractRefreshCause::GapBackfill,
                        false,
                    )
                    .await
                {
                    Ok(report) => Some(report),
                    Err(error) => return Some(Err(error)),
                }
            } else if let Some(interval) = policy.contract_refresh_interval {
                match self
                    .refresh_futures_contract_snapshot_if_due(interval)
                    .await
                {
                    Ok(report) => report,
                    Err(error) => return Some(Err(error)),
                }
            } else {
                None
            }
        } else if let Some(interval) = policy.contract_refresh_interval {
            match self
                .refresh_futures_contract_snapshot_if_due(interval)
                .await
            {
                Ok(report) => report,
                Err(error) => return Some(Err(error)),
            }
        } else {
            None
        };

        Some(Ok(MexcManagedRuntimeStep {
            event,
            pending_reset,
            health_alerts,
            public_metadata_refresh,
            contract_refresh,
            heal_outcome,
        }))
    }
}

fn contract_refresh_signal_symbol(event: &MexcPublicEvent) -> Option<String> {
    match event {
        MexcPublicEvent::Futures(MexcFuturesWsMessage::Contract(message)) => message
            .symbol
            .clone()
            .or_else(|| Some(message.data.symbol.clone())),
        MexcPublicEvent::Futures(MexcFuturesWsMessage::EventContract(message)) => {
            Some(message.data.symbol.clone())
        }
        MexcPublicEvent::Futures(MexcFuturesWsMessage::Raw(raw)) => {
            let channel = raw.get("channel").and_then(serde_json::Value::as_str)?;
            match channel {
                "push.contract" | "push.event.contract" => raw_contract_signal_symbol(raw),
                _ => None,
            }
        }
        _ => None,
    }
}

fn raw_contract_signal_symbol(raw: &serde_json::Value) -> Option<String> {
    raw.get("symbol")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            raw.get("data")
                .and_then(serde_json::Value::as_object)
                .and_then(|data| data.get("symbol"))
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spot_proto::PublicAggreDealsV3Api;
    use crate::{MexcSpotEnvelope, MexcWsAck, MexcWsSessionStart};
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    #[test]
    fn bootstrap_fetch_mode_strings_are_stable() {
        assert_eq!(MexcBootstrapFetchMode::BulkOnly.as_str(), "bulk_only");
        assert_eq!(
            MexcBootstrapFetchMode::Parallel { concurrency: 8 }.as_str(),
            "parallel"
        );
        assert_eq!(
            MexcBootstrapFetchMode::SequentialRequested.as_str(),
            "sequential"
        );
        assert_eq!(
            MexcBootstrapFetchMode::SequentialFallback {
                requested_concurrency: 8
            }
            .as_str(),
            "sequential_fallback"
        );
    }

    #[test]
    fn delivery_profiles_capture_change_only_channels() {
        assert_eq!(
            MexcPublicEventKind::FuturesContract.delivery_profile(),
            MexcPublicEventDeliveryProfile::ChangeOnly
        );
        assert_eq!(
            MexcPublicEventKind::FuturesIndexPrice.delivery_profile(),
            MexcPublicEventDeliveryProfile::AlwaysOn
        );
        assert_eq!(
            MexcPublicEventKind::SpotBookTickerBatch.delivery_profile(),
            MexcPublicEventDeliveryProfile::Opportunistic
        );
    }

    #[test]
    fn balanced_startup_set_is_stable() {
        assert!(MexcPublicEventKind::SpotAggDepth.is_recommended_for_balanced_startup());
        assert!(MexcPublicEventKind::FuturesIndexPrice.is_recommended_for_balanced_startup());
        assert!(MexcPublicEventKind::FuturesFairPrice.is_recommended_for_balanced_startup());
        assert!(!MexcPublicEventKind::FuturesContract.is_recommended_for_balanced_startup());
        assert_eq!(SPOT_RECOVERY_KINDS, &[MexcPublicEventKind::SpotAggDepth]);
        assert_eq!(
            FUTURES_RECOVERY_KINDS,
            &[
                MexcPublicEventKind::FuturesIndexPrice,
                MexcPublicEventKind::FuturesFairPrice
            ]
        );
        assert!(BALANCED_HEALTH_KINDS.contains(&MexcPublicEventKind::SpotBookTickerBatch));
        assert!(BALANCED_HEALTH_KINDS.contains(&MexcPublicEventKind::FuturesDepth));
        assert_eq!(
            MexcPublicEventKind::SpotAggDepth.recommended_max_silence(),
            Duration::from_secs(2)
        );
        assert_eq!(
            MexcPublicEventKind::FuturesTicker.recommended_max_silence(),
            Duration::from_secs(5)
        );
        assert_eq!(
            MexcPublicEventKind::FuturesIndexPrice.recommended_max_silence(),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn live_kind_wait_report_ready_when_no_missing() {
        let report = MexcLiveKindWaitReport {
            expected: vec![MexcPublicEventKind::SpotAggDepth],
            seen: BTreeSet::from([MexcPublicEventKind::SpotAggDepth]),
            missing: Vec::new(),
            observed_live_events: 1,
        };
        assert!(report.is_ready());
        assert_eq!(report.seen_count(), 1);
        assert_eq!(report.expected_count(), 1);
    }

    #[test]
    fn live_kind_observation_defaults_to_zero_counts() {
        let report = MexcLiveKindObservationReport {
            expected: vec![MexcPublicEventKind::FuturesContract],
            observations: BTreeMap::from([(
                MexcPublicEventKind::FuturesContract,
                MexcLiveKindObservation::default(),
            )]),
            missing: vec![MexcPublicEventKind::FuturesContract],
            observed_live_events: 0,
            window_ms: 1000,
        };
        let observation = report
            .observations
            .get(&MexcPublicEventKind::FuturesContract)
            .expect("observation");
        assert_eq!(observation.count, 0);
        assert_eq!(observation.first_seen_after_ms, None);
        assert_eq!(observation.last_seen_after_ms, None);
    }

    #[tokio::test]
    async fn warm_futures_contract_gaps_returns_no_gaps_without_network_work() {
        let mut runtime = MexcManagedStatefulPublicRuntime {
            builder: MexcPublicRuntimeBuilder::new(
                crate::MexcPublicRestClient::default(),
                crate::MexcSpotWsClient::default(),
                crate::MexcFuturesWsClient::default(),
            ),
            config: MexcPublicRuntimeConfig::balanced(),
            snapshot_mode: MexcManagedSnapshotMode::DeepSnapshot { concurrency: 4 },
            total_rebuilds: 0,
            last_rebuild_at: None,
            last_contract_refresh_at: Some(Instant::now()),
            last_public_metadata_refresh_at: Some(Instant::now()),
            pending_contract_refresh_symbols: BTreeSet::new(),
            inner: MexcStatefulPublicRuntime {
                manifest: MexcPublicRuntimeManifest {
                    spot_symbol_count: 0,
                    futures_symbol_count: 0,
                    spot_subscription_count: 0,
                    futures_subscription_count: 0,
                    spot_connection_count: 0,
                    futures_connection_count: 0,
                },
                state: MexcPublicState::default(),
                deep_report: None,
                spot_session: None,
                futures_session: None,
                pending_spot_resets: 0,
                pending_futures_resets: 0,
                total_spot_resets: 0,
                total_futures_resets: 0,
                spot_recovery_seen: BTreeSet::new(),
                futures_recovery_seen: BTreeSet::new(),
                last_seen_at_by_kind: BTreeMap::new(),
                stale_since_by_kind: BTreeMap::new(),
                stream: ReceiverStream::new(mpsc::channel(1).1),
            },
        };

        let report = runtime
            .warm_futures_contract_gaps(4, 16)
            .await
            .expect("warmup");
        assert_eq!(
            report.stop_reason,
            MexcManagedContractGapWarmupStopReason::NoGaps
        );
        assert_eq!(report.passes, 0);
        assert_eq!(report.initial_gap_count(), 0);
        assert_eq!(report.final_gap_count(), 0);
        assert!(report.is_fully_resolved());
    }

    #[test]
    fn public_event_kind_ignores_ack_frames() {
        let event = MexcPublicEvent::Spot(MexcSpotWsMessage::Ack(MexcWsAck {
            id: Some(1),
            code: Some(0),
            msg: Some("ok".to_string()),
            channel: None,
            data: None,
        }));
        assert_eq!(event.kind(), None);
        assert!(!event.is_live_payload());
    }

    #[test]
    fn public_event_kind_ignores_session_start_frames() {
        let event =
            MexcPublicEvent::Futures(MexcFuturesWsMessage::SessionStart(MexcWsSessionStart {
                session_id: 2,
                resumed: true,
                subscription_count: 200,
            }));
        assert_eq!(event.kind(), None);
        assert!(!event.is_live_payload());
    }

    #[test]
    fn public_event_kind_maps_spot_payloads() {
        let event = MexcPublicEvent::Spot(MexcSpotWsMessage::AggTrades(MexcSpotEnvelope {
            channel: "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT".to_string(),
            symbol: Some("BTCUSDT".to_string()),
            create_time: None,
            send_time: Some(1),
            data: PublicAggreDealsV3Api {
                deals: Vec::new(),
                event_type: "spot@public.aggre.deals.v3.api.pb@100ms".to_string(),
            },
        }));
        assert_eq!(event.kind(), Some(MexcPublicEventKind::SpotAggTrades));
        assert_eq!(event.kind().expect("kind").as_str(), "spot.aggTrades");
        assert!(event.is_live_payload());
    }

    #[test]
    fn public_event_kind_maps_futures_raw_as_non_live() {
        let event = MexcPublicEvent::Futures(MexcFuturesWsMessage::Raw(json!({"foo":"bar"})));
        assert_eq!(event.kind(), None);
        assert!(!event.is_live_payload());
    }

    #[tokio::test]
    async fn stateful_runtime_clears_futures_depth_on_resumed_session() {
        let mut state = MexcPublicState::default();
        state
            .futures_symbols
            .entry("BTC_USDT".to_string())
            .or_default()
            .depth = Some(crate::FuturesDepthSnapshot {
            cts: None,
            asks: Vec::new(),
            bids: Vec::new(),
            version: 7,
        });

        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(MexcPublicEvent::Futures(
            MexcFuturesWsMessage::SessionStart(MexcWsSessionStart {
                session_id: 2,
                resumed: true,
                subscription_count: 1,
            }),
        )))
        .await
        .expect("send session start");
        drop(tx);

        let mut runtime = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 1,
                spot_subscription_count: 0,
                futures_subscription_count: 0,
                spot_connection_count: 0,
                futures_connection_count: 1,
            },
            state,
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(rx),
        };

        let event = runtime
            .next_event()
            .await
            .expect("event")
            .expect("ok event");
        assert!(matches!(
            event,
            MexcPublicEvent::Futures(MexcFuturesWsMessage::SessionStart(_))
        ));
        assert!(
            runtime
                .state
                .futures_symbols
                .get("BTC_USDT")
                .expect("symbol state")
                .depth
                .is_none()
        );
        let report = runtime
            .take_pending_reset_report()
            .expect("pending reset report");
        assert_eq!(report.spot_resets_detected, 0);
        assert_eq!(report.futures_resets_detected, 1);
        assert_eq!(
            runtime.session_status().pending_futures_resets,
            0,
            "taking the report should clear pending counters"
        );
    }

    #[tokio::test]
    async fn next_live_event_keeps_pending_reset_report_visible() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(MexcPublicEvent::Futures(
            MexcFuturesWsMessage::SessionStart(MexcWsSessionStart {
                session_id: 2,
                resumed: true,
                subscription_count: 1,
            }),
        )))
        .await
        .expect("send session start");
        tx.send(Ok(MexcPublicEvent::Futures(
            MexcFuturesWsMessage::IndexPrice(crate::MexcFuturesEnvelope {
                channel: "push.index.price".to_string(),
                symbol: Some("BTC_USDT".to_string()),
                data: crate::FuturesWsPricePoint {
                    symbol: "BTC_USDT".to_string(),
                    price: 60000.0,
                },
                ts: Some(1),
            }),
        )))
        .await
        .expect("send index price");
        drop(tx);

        let mut runtime = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 1,
                spot_subscription_count: 0,
                futures_subscription_count: 1,
                spot_connection_count: 0,
                futures_connection_count: 1,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(rx),
        };

        let event = runtime
            .next_live_event()
            .await
            .expect("event")
            .expect("ok event");
        assert!(matches!(
            event,
            MexcPublicEvent::Futures(MexcFuturesWsMessage::IndexPrice(_))
        ));
        let report = runtime
            .take_pending_reset_report()
            .expect("pending reset report");
        assert_eq!(report.futures_resets_detected, 1);
        assert_eq!(
            report.futures_session.expect("futures session").session_id,
            2
        );
    }

    #[tokio::test]
    async fn await_recovery_after_pending_reset_waits_for_expected_kinds() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(Ok(MexcPublicEvent::Spot(MexcSpotWsMessage::SessionStart(
            MexcWsSessionStart {
                session_id: 2,
                resumed: true,
                subscription_count: 1,
            },
        ))))
        .await
        .expect("send spot session start");
        tx.send(Ok(MexcPublicEvent::Futures(
            MexcFuturesWsMessage::SessionStart(MexcWsSessionStart {
                session_id: 3,
                resumed: true,
                subscription_count: 1,
            }),
        )))
        .await
        .expect("send futures session start");
        tx.send(Ok(MexcPublicEvent::Spot(MexcSpotWsMessage::AggDepth(
            crate::MexcSpotEnvelope {
                channel: "spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT".to_string(),
                symbol: Some("BTCUSDT".to_string()),
                create_time: None,
                send_time: Some(1),
                data: crate::spot_proto::PublicAggreDepthsV3Api::default(),
            },
        ))))
        .await
        .expect("send spot recovery event");
        tx.send(Ok(MexcPublicEvent::Futures(
            MexcFuturesWsMessage::IndexPrice(crate::MexcFuturesEnvelope {
                channel: "push.index.price".to_string(),
                symbol: Some("BTC_USDT".to_string()),
                data: crate::FuturesWsPricePoint {
                    symbol: "BTC_USDT".to_string(),
                    price: 60000.0,
                },
                ts: Some(1),
            }),
        )))
        .await
        .expect("send futures index price");
        tx.send(Ok(MexcPublicEvent::Futures(
            MexcFuturesWsMessage::FairPrice(crate::MexcFuturesEnvelope {
                channel: "push.fair.price".to_string(),
                symbol: Some("BTC_USDT".to_string()),
                data: crate::FuturesWsPricePoint {
                    symbol: "BTC_USDT".to_string(),
                    price: 60001.0,
                },
                ts: Some(2),
            }),
        )))
        .await
        .expect("send futures fair price");
        drop(tx);

        let mut runtime = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 1,
                futures_symbol_count: 1,
                spot_subscription_count: 1,
                futures_subscription_count: 1,
                spot_connection_count: 1,
                futures_connection_count: 1,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(rx),
        };

        let first_live = runtime
            .next_live_event()
            .await
            .expect("first live event")
            .expect("ok first live event");
        assert!(matches!(
            first_live,
            MexcPublicEvent::Spot(MexcSpotWsMessage::AggDepth(_))
        ));
        let recovery = runtime
            .await_recovery_after_pending_reset(Duration::from_secs(1))
            .await
            .expect("recovery result")
            .expect("pending recovery report");
        assert_eq!(recovery.reset.spot_resets_detected, 1);
        assert_eq!(recovery.reset.futures_resets_detected, 1);
        assert!(recovery.readiness.is_ready());
        assert_eq!(recovery.readiness.expected_count(), 3);
        assert_eq!(runtime.session_status().pending_spot_resets, 0);
        assert_eq!(runtime.session_status().pending_futures_resets, 0);
    }

    #[tokio::test]
    async fn await_recovery_after_pending_reset_returns_none_without_resets() {
        let (_tx, rx) = mpsc::channel(1);
        let mut runtime = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 0,
                spot_subscription_count: 0,
                futures_subscription_count: 0,
                spot_connection_count: 0,
                futures_connection_count: 0,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(rx),
        };

        let recovery = runtime
            .await_recovery_after_pending_reset(Duration::from_millis(10))
            .await
            .expect("recovery result");
        assert!(recovery.is_none());
    }

    #[test]
    fn balanced_health_status_is_stale_before_any_live_events() {
        let runtime = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 0,
                spot_subscription_count: 0,
                futures_subscription_count: 0,
                spot_connection_count: 0,
                futures_connection_count: 0,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(mpsc::channel(1).1),
        };

        let health = runtime.balanced_health_status(Duration::from_secs(5));
        assert!(!health.is_healthy());
        assert_eq!(health.healthy_count(), 0);
        assert_eq!(health.expected_count(), BALANCED_HEALTH_KINDS.len());
        assert_eq!(health.stale.len(), BALANCED_HEALTH_KINDS.len());
        assert_eq!(
            health
                .health
                .get(&MexcPublicEventKind::SpotAggDepth)
                .expect("spot aggDepth health")
                .max_age_ms,
            5_000
        );
    }

    #[tokio::test]
    async fn spot_session_restart_clears_spot_health_freshness() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(MexcPublicEvent::Spot(MexcSpotWsMessage::AggDepth(
            crate::MexcSpotEnvelope {
                channel: "spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT".to_string(),
                symbol: Some("BTCUSDT".to_string()),
                create_time: None,
                send_time: Some(1),
                data: crate::spot_proto::PublicAggreDepthsV3Api::default(),
            },
        ))))
        .await
        .expect("send first aggDepth");
        tx.send(Ok(MexcPublicEvent::Spot(MexcSpotWsMessage::SessionStart(
            MexcWsSessionStart {
                session_id: 2,
                resumed: true,
                subscription_count: 1,
            },
        ))))
        .await
        .expect("send session restart");
        drop(tx);

        let mut runtime = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 1,
                futures_symbol_count: 0,
                spot_subscription_count: 1,
                futures_subscription_count: 0,
                spot_connection_count: 1,
                futures_connection_count: 0,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(rx),
        };

        runtime
            .next_event()
            .await
            .expect("first event")
            .expect("ok");
        let fresh = runtime
            .live_kind_health_status(&[MexcPublicEventKind::SpotAggDepth], Duration::from_secs(5));
        assert!(fresh.is_healthy());

        runtime.next_event().await.expect("restart").expect("ok");
        let stale = runtime
            .live_kind_health_status(&[MexcPublicEventKind::SpotAggDepth], Duration::from_secs(5));
        assert!(!stale.is_healthy());
        assert_eq!(stale.stale, vec![MexcPublicEventKind::SpotAggDepth]);
    }

    #[test]
    fn balanced_health_status_with_defaults_uses_kind_specific_thresholds() {
        let runtime = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 0,
                spot_subscription_count: 0,
                futures_subscription_count: 0,
                spot_connection_count: 0,
                futures_connection_count: 0,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(mpsc::channel(1).1),
        };

        let health = runtime.balanced_health_status_with_defaults();
        assert_eq!(
            health
                .health
                .get(&MexcPublicEventKind::SpotAggDepth)
                .expect("spot health")
                .max_age_ms,
            2_000
        );
        assert_eq!(
            health
                .health
                .get(&MexcPublicEventKind::FuturesTicker)
                .expect("futures ticker health")
                .max_age_ms,
            5_000
        );
        assert_eq!(
            health
                .health
                .get(&MexcPublicEventKind::FuturesFairPrice)
                .expect("futures fair health")
                .max_age_ms,
            10_000
        );
    }

    #[test]
    fn health_alerts_track_new_persistent_and_recovered_states() {
        let now = Instant::now();
        let mut runtime = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 0,
                spot_subscription_count: 0,
                futures_subscription_count: 0,
                spot_connection_count: 0,
                futures_connection_count: 0,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::from([(
                MexcPublicEventKind::FuturesTicker,
                now - Duration::from_secs(20),
            )]),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(mpsc::channel(1).1),
        };

        let first = runtime.poll_live_kind_health_alerts(
            &[MexcPublicEventKind::FuturesTicker],
            Duration::from_secs(5),
            Duration::ZERO,
        );
        assert_eq!(first.newly_stale.len(), 1);
        assert_eq!(first.persistent_stale.len(), 1);
        assert!(first.recovered.is_empty());

        let stale_since = now - Duration::from_secs(30);
        runtime
            .stale_since_by_kind
            .insert(MexcPublicEventKind::FuturesTicker, stale_since);
        let second = runtime.poll_live_kind_health_alerts(
            &[MexcPublicEventKind::FuturesTicker],
            Duration::from_secs(5),
            Duration::from_secs(10),
        );
        assert!(second.newly_stale.is_empty());
        assert_eq!(second.persistent_stale.len(), 1);
        assert!(second.persistent_stale[0].stale_for_ms >= 30_000);

        runtime.last_seen_at_by_kind.insert(
            MexcPublicEventKind::FuturesTicker,
            Instant::now() - Duration::from_secs(1),
        );
        let third = runtime.poll_live_kind_health_alerts(
            &[MexcPublicEventKind::FuturesTicker],
            Duration::from_secs(5),
            Duration::from_secs(10),
        );
        assert!(third.newly_stale.is_empty());
        assert!(third.persistent_stale.is_empty());
        assert_eq!(third.recovered.len(), 1);
        assert!(
            !runtime
                .stale_since_by_kind
                .contains_key(&MexcPublicEventKind::FuturesTicker)
        );
    }

    #[tokio::test]
    async fn managed_heal_can_be_suppressed_by_cooldown() {
        let inner = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 0,
                spot_subscription_count: 0,
                futures_subscription_count: 0,
                spot_connection_count: 0,
                futures_connection_count: 0,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::from([(
                MexcPublicEventKind::SpotAggDepth,
                Instant::now() - Duration::from_secs(10),
            )]),
            stream: ReceiverStream::new(mpsc::channel(1).1),
        };
        let mut managed = MexcManagedStatefulPublicRuntime {
            builder: MexcPublicRuntimeBuilder::new(
                crate::MexcPublicRestClient::default(),
                crate::MexcSpotWsClient::default(),
                crate::MexcFuturesWsClient::default(),
            ),
            config: MexcPublicRuntimeConfig::balanced(),
            snapshot_mode: MexcManagedSnapshotMode::DeepSnapshot { concurrency: 8 },
            total_rebuilds: 1,
            last_rebuild_at: Some(Instant::now()),
            last_contract_refresh_at: Some(Instant::now()),
            last_public_metadata_refresh_at: Some(Instant::now()),
            pending_contract_refresh_symbols: BTreeSet::new(),
            inner,
        };

        let outcome = managed
            .heal_if_persistent_balanced_stale_with_defaults_and_cooldown(
                Duration::from_secs(5),
                Duration::from_secs(30),
                Duration::from_secs(60),
            )
            .await
            .expect("cooldown outcome");

        match outcome {
            MexcManagedHealOutcome::Suppressed {
                alerts_before_heal,
                cooldown_remaining_ms,
                total_rebuilds,
            } => {
                assert!(!alerts_before_heal.persistent_stale.is_empty());
                assert!(cooldown_remaining_ms > 0);
                assert_eq!(total_rebuilds, 1);
            }
            other => panic!("expected suppressed outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn contract_refresh_if_due_skips_when_recent() {
        let inner = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 0,
                spot_subscription_count: 0,
                futures_subscription_count: 0,
                spot_connection_count: 0,
                futures_connection_count: 0,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(mpsc::channel(1).1),
        };
        let mut managed = MexcManagedStatefulPublicRuntime {
            builder: MexcPublicRuntimeBuilder::new(
                crate::MexcPublicRestClient::default(),
                crate::MexcSpotWsClient::default(),
                crate::MexcFuturesWsClient::default(),
            ),
            config: MexcPublicRuntimeConfig::balanced(),
            snapshot_mode: MexcManagedSnapshotMode::DeepSnapshot { concurrency: 8 },
            total_rebuilds: 0,
            last_rebuild_at: None,
            last_contract_refresh_at: Some(Instant::now()),
            last_public_metadata_refresh_at: Some(Instant::now()),
            pending_contract_refresh_symbols: BTreeSet::new(),
            inner,
        };

        let report = managed
            .refresh_futures_contract_snapshot_if_due(Duration::from_secs(60))
            .await
            .expect("refresh if due");
        assert!(report.is_none());
    }

    #[tokio::test]
    async fn public_metadata_refresh_if_due_skips_when_recent() {
        let inner = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 0,
                spot_subscription_count: 0,
                futures_subscription_count: 0,
                spot_connection_count: 0,
                futures_connection_count: 0,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(mpsc::channel(1).1),
        };
        let mut managed = MexcManagedStatefulPublicRuntime {
            builder: MexcPublicRuntimeBuilder::new(
                crate::MexcPublicRestClient::default(),
                crate::MexcSpotWsClient::default(),
                crate::MexcFuturesWsClient::default(),
            ),
            config: MexcPublicRuntimeConfig::balanced(),
            snapshot_mode: MexcManagedSnapshotMode::DeepSnapshot { concurrency: 8 },
            total_rebuilds: 0,
            last_rebuild_at: None,
            last_contract_refresh_at: Some(Instant::now()),
            last_public_metadata_refresh_at: Some(Instant::now()),
            pending_contract_refresh_symbols: BTreeSet::new(),
            inner,
        };

        let report = managed
            .refresh_public_metadata_snapshot_if_due(Duration::from_secs(60))
            .await
            .expect("public metadata refresh if due");
        assert!(report.is_none());
    }

    #[tokio::test]
    async fn next_step_skips_refreshes_when_intervals_not_due() {
        let (tx, rx) = mpsc::channel(2);
        tx.send(Ok(MexcPublicEvent::Futures(MexcFuturesWsMessage::Ticker(
            crate::MexcFuturesEnvelope {
                channel: "push.ticker".to_string(),
                symbol: Some("BTC_USDT".to_string()),
                data: crate::FuturesTicker {
                    contract_id: None,
                    symbol: "BTC_USDT".to_string(),
                    last_price: 1.0,
                    bid1: None,
                    ask1: None,
                    volume24: 1.0,
                    amount24: None,
                    hold_vol: None,
                    lower24_price: None,
                    high24_price: None,
                    rise_fall_rate: None,
                    rise_fall_value: None,
                    index_price: None,
                    fair_price: None,
                    funding_rate: None,
                    max_bid_price: None,
                    min_ask_price: None,
                    timestamp: None,
                    extra: BTreeMap::new(),
                },
                ts: Some(1),
            },
        ))))
        .await
        .expect("send ticker");
        drop(tx);

        let inner = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 1,
                spot_subscription_count: 0,
                futures_subscription_count: 1,
                spot_connection_count: 0,
                futures_connection_count: 1,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(rx),
        };
        let mut managed = MexcManagedStatefulPublicRuntime {
            builder: MexcPublicRuntimeBuilder::new(
                crate::MexcPublicRestClient::default(),
                crate::MexcSpotWsClient::default(),
                crate::MexcFuturesWsClient::default(),
            ),
            config: MexcPublicRuntimeConfig::balanced(),
            snapshot_mode: MexcManagedSnapshotMode::DeepSnapshot { concurrency: 8 },
            total_rebuilds: 0,
            last_rebuild_at: None,
            last_contract_refresh_at: Some(Instant::now()),
            last_public_metadata_refresh_at: Some(Instant::now()),
            pending_contract_refresh_symbols: BTreeSet::new(),
            inner,
        };
        let policy = MexcManagedRuntimePolicy {
            public_metadata_refresh_interval: Some(Duration::from_secs(60)),
            contract_refresh_interval: Some(Duration::from_secs(60)),
            ..MexcManagedRuntimePolicy::balanced_defaults()
        };

        let step = managed
            .next_step(&policy)
            .await
            .expect("step")
            .expect("ok step");
        assert!(step.public_metadata_refresh.is_none());
        assert!(step.contract_refresh.is_none());
    }

    #[test]
    fn contract_refresh_signal_symbol_extracts_from_typed_and_raw_events() {
        let typed = MexcPublicEvent::Futures(MexcFuturesWsMessage::EventContract(
            crate::MexcFuturesEnvelope {
                channel: "push.event.contract".to_string(),
                symbol: None,
                data: crate::FuturesEventContract {
                    contract_id: crate::MexcFlexibleString::String("1".to_string()),
                    symbol: "BTC_USDT".to_string(),
                    base_coin: "BTC".to_string(),
                    quote_coin: "USDT".to_string(),
                    base_coin_name: "Bitcoin".to_string(),
                    quote_coin_name: "Tether".to_string(),
                    settle_coin: "USDT".to_string(),
                    base_coin_icon_url: None,
                    invest_min_amount: None,
                    invest_max_amount: None,
                    amount_scale: None,
                    pay_rate_scale: None,
                    index_price_scale: None,
                    available_scale: None,
                    extra: BTreeMap::new(),
                },
                ts: Some(1),
            },
        ));
        let raw = MexcPublicEvent::Futures(MexcFuturesWsMessage::Raw(json!({
            "channel": "push.contract",
            "data": {
                "symbol": "ETH_USDT"
            }
        })));

        assert_eq!(
            contract_refresh_signal_symbol(&typed).as_deref(),
            Some("BTC_USDT")
        );
        assert_eq!(
            contract_refresh_signal_symbol(&raw).as_deref(),
            Some("ETH_USDT")
        );
    }

    #[test]
    fn pending_contract_refresh_symbols_wait_for_cooldown() {
        let inner = MexcStatefulPublicRuntime {
            manifest: MexcPublicRuntimeManifest {
                spot_symbol_count: 0,
                futures_symbol_count: 0,
                spot_subscription_count: 0,
                futures_subscription_count: 0,
                spot_connection_count: 0,
                futures_connection_count: 0,
            },
            state: MexcPublicState::default(),
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: ReceiverStream::new(mpsc::channel(1).1),
        };
        let mut managed = MexcManagedStatefulPublicRuntime {
            builder: MexcPublicRuntimeBuilder::new(
                crate::MexcPublicRestClient::default(),
                crate::MexcSpotWsClient::default(),
                crate::MexcFuturesWsClient::default(),
            ),
            config: MexcPublicRuntimeConfig::balanced(),
            snapshot_mode: MexcManagedSnapshotMode::DeepSnapshot { concurrency: 8 },
            total_rebuilds: 0,
            last_rebuild_at: None,
            last_contract_refresh_at: Some(Instant::now()),
            last_public_metadata_refresh_at: Some(Instant::now()),
            pending_contract_refresh_symbols: BTreeSet::new(),
            inner,
        };
        managed
            .pending_contract_refresh_symbols
            .insert("BTC_USDT".to_string());

        assert!(
            managed
                .pending_contract_refresh_symbols_if_due(Duration::from_secs(60))
                .is_none()
        );
        assert_eq!(
            managed
                .pending_contract_refresh_symbols_if_due(Duration::from_secs(0))
                .unwrap(),
            vec!["BTC_USDT".to_string()]
        );
    }
}

#[derive(Clone)]
pub struct MexcPublicRuntimeBuilder {
    pub rest: MexcPublicRestClient,
    pub spot_ws: MexcSpotWsClient,
    pub futures_ws: MexcFuturesWsClient,
}

impl MexcPublicRuntimeBuilder {
    pub fn new(
        rest: MexcPublicRestClient,
        spot_ws: MexcSpotWsClient,
        futures_ws: MexcFuturesWsClient,
    ) -> Self {
        Self {
            rest,
            spot_ws,
            futures_ws,
        }
    }

    pub async fn build_manifest(
        &self,
        config: &MexcPublicRuntimeConfig,
    ) -> Result<(
        MexcPublicRuntimeManifest,
        Vec<MexcSpotSubscription>,
        Vec<MexcFuturesSubscription>,
    )> {
        let (spot_symbols, futures_symbols) = self.rest.all_public_symbols().await?;

        let spot_subscriptions =
            build_spot_public_subscriptions(&spot_symbols, &config.spot_coverage);
        let futures_subscriptions =
            build_futures_public_subscriptions(&futures_symbols, &config.futures_coverage);

        if config.futures_subscriptions_per_connection == 0 {
            return Err(anyhow!(
                "futures_subscriptions_per_connection must be greater than zero"
            ));
        }

        let manifest = MexcPublicRuntimeManifest {
            spot_symbol_count: spot_symbols.len(),
            futures_symbol_count: futures_symbols.len(),
            spot_subscription_count: spot_subscriptions.len(),
            futures_subscription_count: futures_subscriptions.len(),
            spot_connection_count: shard_spot_subscriptions(&spot_subscriptions).len(),
            futures_connection_count: shard_futures_subscriptions(
                &futures_subscriptions,
                config.futures_subscriptions_per_connection,
            )
            .len(),
        };

        Ok((manifest, spot_subscriptions, futures_subscriptions))
    }

    pub async fn bootstrap_snapshot(&self) -> Result<MexcPublicBootstrapSnapshot> {
        let spot_server_time = self.rest.spot_server_time().await?;
        let spot_default_symbols = self.rest.spot_default_symbols().await?;
        let spot_offline_symbols = self.rest.spot_offline_symbols().await?;
        let spot_exchange_info = self.rest.spot_exchange_info().await?;
        let spot_ticker_24hr = self.rest.spot_ticker_24hr_all().await?;
        let spot_price_tickers = self.rest.spot_price_ticker_all().await?;
        let spot_book_tickers = self.rest.spot_book_ticker_all().await?;

        let futures_server_time = self.rest.futures_server_time().await?;
        let futures_contracts = self.rest.futures_contracts_all().await?;
        let futures_tickers = self.rest.futures_ticker_all().await?;
        let futures_transferable_currencies = self.rest.futures_transferable_currencies().await?;
        let futures_insurance_balances = self.rest.futures_insurance_fund_balance().await?;

        Ok(MexcPublicBootstrapSnapshot {
            spot: MexcSpotBootstrapSnapshot {
                server_time: spot_server_time,
                default_symbols: spot_default_symbols,
                offline_symbols: spot_offline_symbols,
                exchange_info: spot_exchange_info,
                ticker_24hr: spot_ticker_24hr,
                price_tickers: spot_price_tickers,
                book_tickers: spot_book_tickers,
            },
            futures: MexcFuturesBootstrapSnapshot {
                server_time: futures_server_time,
                contracts: futures_contracts,
                tickers: futures_tickers,
                transferable_currencies: futures_transferable_currencies,
                insurance_balances: futures_insurance_balances,
            },
        })
    }

    pub async fn refresh_public_metadata_snapshot(
        &self,
    ) -> Result<MexcPublicMetadataRefreshSnapshot> {
        Ok(MexcPublicMetadataRefreshSnapshot {
            spot_server_time: self.rest.spot_server_time().await?,
            spot_default_symbols: self.rest.spot_default_symbols().await?,
            spot_offline_symbols: self.rest.spot_offline_symbols().await?,
            spot_exchange_info: self.rest.spot_exchange_info().await?,
            futures_server_time: self.rest.futures_server_time().await?,
            futures_transferable_currencies: self.rest.futures_transferable_currencies().await?,
            futures_insurance_balances: self.rest.futures_insurance_fund_balance().await?,
        })
    }

    pub async fn refresh_futures_contract_snapshot(&self) -> Result<Vec<FuturesContractInfo>> {
        self.rest.futures_contracts_all().await
    }

    pub async fn refresh_futures_contract_symbols(
        &self,
        symbols: &[String],
    ) -> Result<Vec<FuturesContractInfo>> {
        let mut contracts = Vec::new();
        for symbol in symbols.iter().cloned().collect::<BTreeSet<_>>().into_iter() {
            match self.rest.futures_contracts(Some(&symbol)).await? {
                OneOrMany::One(item) => contracts.push(item),
                OneOrMany::Many(items) => contracts.extend(items),
            }
        }
        Ok(contracts)
    }

    pub async fn connect(&self, config: MexcPublicRuntimeConfig) -> Result<MexcPublicRuntime> {
        let (manifest, spot_subscriptions, futures_subscriptions) =
            self.build_manifest(&config).await?;

        let spot_streams = self.spot_ws.connect_sharded(spot_subscriptions).await?;
        let futures_streams = connect_futures_sharded(
            &self.futures_ws,
            futures_subscriptions,
            config.futures_subscriptions_per_connection,
        )
        .await?;

        let (tx, rx) = mpsc::channel(PUBLIC_RUNTIME_EVENT_CHANNEL_CAPACITY);

        for stream in spot_streams {
            let tx = tx.clone();
            tokio::spawn(async move {
                forward_spot_stream(stream, tx).await;
            });
        }

        for stream in futures_streams {
            let tx = tx.clone();
            tokio::spawn(async move {
                forward_futures_stream(stream, tx).await;
            });
        }

        drop(tx);

        Ok(MexcPublicRuntime {
            manifest,
            stream: ReceiverStream::new(rx),
        })
    }

    pub async fn connect_with_snapshot(
        &self,
        config: MexcPublicRuntimeConfig,
    ) -> Result<MexcPublicBootstrapRuntime> {
        let snapshot = self.bootstrap_snapshot().await?;
        let runtime = self.connect(config).await?;
        Ok(MexcPublicBootstrapRuntime {
            manifest: runtime.manifest,
            snapshot,
            stream: runtime.stream,
        })
    }

    pub async fn connect_stateful_with_snapshot(
        &self,
        config: MexcPublicRuntimeConfig,
    ) -> Result<MexcStatefulPublicRuntime> {
        let runtime = self.connect_with_snapshot(config).await?;
        let state = MexcPublicState::from_snapshot(&runtime.snapshot);
        Ok(MexcStatefulPublicRuntime {
            manifest: runtime.manifest,
            state,
            deep_report: None,
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: runtime.stream,
        })
    }

    pub async fn connect_managed_stateful_with_snapshot(
        &self,
        config: MexcPublicRuntimeConfig,
    ) -> Result<MexcManagedStatefulPublicRuntime> {
        let inner = self.connect_stateful_with_snapshot(config.clone()).await?;
        Ok(MexcManagedStatefulPublicRuntime {
            builder: self.clone(),
            config,
            snapshot_mode: MexcManagedSnapshotMode::Snapshot,
            total_rebuilds: 0,
            last_rebuild_at: None,
            last_contract_refresh_at: Some(Instant::now()),
            last_public_metadata_refresh_at: Some(Instant::now()),
            pending_contract_refresh_symbols: BTreeSet::new(),
            inner,
        })
    }

    pub async fn connect_managed_stateful_ready_with_snapshot(
        &self,
        config: MexcPublicRuntimeConfig,
        startup_wait_for: Duration,
    ) -> Result<MexcManagedReadyRuntime> {
        let mut runtime = self.connect_managed_stateful_with_snapshot(config).await?;
        let startup = runtime.await_balanced_startup(startup_wait_for).await?;
        let contract_gap_warmup = runtime
            .warm_futures_contract_gaps(
                READY_CONTRACT_GAP_WARMUP_MAX_PASSES,
                READY_CONTRACT_GAP_WARMUP_BATCH_SIZE,
            )
            .await?;
        Ok(MexcManagedReadyRuntime {
            runtime,
            startup,
            contract_gap_warmup,
        })
    }

    pub async fn connect_with_deep_snapshot(
        &self,
        config: MexcPublicRuntimeConfig,
        concurrency: usize,
    ) -> Result<MexcPublicDeepBootstrapRuntime> {
        let snapshot = self.bootstrap_deep_snapshot(concurrency).await?;
        let runtime = self.connect(config).await?;
        Ok(MexcPublicDeepBootstrapRuntime {
            manifest: runtime.manifest,
            snapshot,
            stream: runtime.stream,
        })
    }

    pub async fn connect_stateful_with_deep_snapshot(
        &self,
        config: MexcPublicRuntimeConfig,
        concurrency: usize,
    ) -> Result<MexcStatefulPublicRuntime> {
        let runtime = self.connect_with_deep_snapshot(config, concurrency).await?;
        let state = MexcPublicState::from_deep_snapshot(&runtime.snapshot);
        Ok(MexcStatefulPublicRuntime {
            manifest: runtime.manifest,
            state,
            deep_report: Some(runtime.snapshot.report.clone()),
            spot_session: None,
            futures_session: None,
            pending_spot_resets: 0,
            pending_futures_resets: 0,
            total_spot_resets: 0,
            total_futures_resets: 0,
            spot_recovery_seen: BTreeSet::new(),
            futures_recovery_seen: BTreeSet::new(),
            last_seen_at_by_kind: BTreeMap::new(),
            stale_since_by_kind: BTreeMap::new(),
            stream: runtime.stream,
        })
    }

    pub async fn connect_managed_stateful_with_deep_snapshot(
        &self,
        config: MexcPublicRuntimeConfig,
        concurrency: usize,
    ) -> Result<MexcManagedStatefulPublicRuntime> {
        let inner = self
            .connect_stateful_with_deep_snapshot(config.clone(), concurrency)
            .await?;
        Ok(MexcManagedStatefulPublicRuntime {
            builder: self.clone(),
            config,
            snapshot_mode: MexcManagedSnapshotMode::DeepSnapshot { concurrency },
            total_rebuilds: 0,
            last_rebuild_at: None,
            last_contract_refresh_at: Some(Instant::now()),
            last_public_metadata_refresh_at: Some(Instant::now()),
            pending_contract_refresh_symbols: BTreeSet::new(),
            inner,
        })
    }

    pub async fn connect_managed_stateful_ready_with_deep_snapshot(
        &self,
        config: MexcPublicRuntimeConfig,
        concurrency: usize,
        startup_wait_for: Duration,
    ) -> Result<MexcManagedReadyRuntime> {
        let mut runtime = self
            .connect_managed_stateful_with_deep_snapshot(config, concurrency)
            .await?;
        let startup = runtime.await_balanced_startup(startup_wait_for).await?;
        let contract_gap_warmup = runtime
            .warm_futures_contract_gaps(
                READY_CONTRACT_GAP_WARMUP_MAX_PASSES,
                READY_CONTRACT_GAP_WARMUP_BATCH_SIZE,
            )
            .await?;
        Ok(MexcManagedReadyRuntime {
            runtime,
            startup,
            contract_gap_warmup,
        })
    }

    async fn fetch_with_mode<T, F, Fut>(
        &self,
        concurrency: usize,
        mut fetcher: F,
    ) -> Result<(Vec<T>, MexcBootstrapFetchMode)>
    where
        F: FnMut(usize) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<T>>>,
    {
        if concurrency <= 1 {
            return Ok((
                fetcher(1).await?,
                MexcBootstrapFetchMode::SequentialRequested,
            ));
        }

        match fetcher(concurrency).await {
            Ok(values) => Ok((values, MexcBootstrapFetchMode::Parallel { concurrency })),
            Err(error) => {
                tracing::warn!(
                    "parallel futures reference bootstrap failed, retrying sequentially: {error:#}"
                );
                Ok((
                    fetcher(1).await?,
                    MexcBootstrapFetchMode::SequentialFallback {
                        requested_concurrency: concurrency,
                    },
                ))
            }
        }
    }

    pub async fn bootstrap_futures_reference_snapshot(
        &self,
        concurrency: usize,
    ) -> Result<(
        MexcFuturesReferenceSnapshot,
        MexcFuturesReferenceBootstrapReport,
    )> {
        let contracts = self.rest.futures_contracts_all().await?;
        let symbols = contracts
            .iter()
            .map(|contract| contract.symbol.clone())
            .collect::<Vec<_>>();
        let tickers = self.rest.futures_ticker_all().await?;
        let ticker_by_symbol = latest_ticker_by_symbol(&tickers);

        let mut index_prices = Vec::new();
        let mut fair_prices = Vec::new();
        let mut funding_rates = Vec::new();
        let mut missing_index_symbols = Vec::new();
        let mut missing_fair_symbols = Vec::new();
        let mut missing_funding_symbols = Vec::new();

        for symbol in &symbols {
            let Some(ticker) = ticker_by_symbol.get(symbol) else {
                missing_index_symbols.push(symbol.clone());
                missing_fair_symbols.push(symbol.clone());
                missing_funding_symbols.push(symbol.clone());
                continue;
            };

            if let Some(index_price) = ticker.index_price {
                index_prices.push(FuturesIndexPrice {
                    symbol: symbol.clone(),
                    index_price,
                    timestamp: ticker.timestamp.unwrap_or(0),
                });
            } else {
                missing_index_symbols.push(symbol.clone());
            }

            if let Some(fair_price) = ticker.fair_price {
                fair_prices.push(FuturesFairPrice {
                    symbol: symbol.clone(),
                    fair_price,
                    timestamp: ticker.timestamp.unwrap_or(0),
                });
            } else {
                missing_fair_symbols.push(symbol.clone());
            }

            if let Some(funding_rate) = ticker.funding_rate {
                funding_rates.push(FuturesFundingRate {
                    symbol: symbol.clone(),
                    funding_rate,
                    max_funding_rate: None,
                    min_funding_rate: None,
                    collect_cycle: None,
                    next_settle_time: None,
                    timestamp: ticker.timestamp,
                    extra: BTreeMap::new(),
                });
            } else {
                missing_funding_symbols.push(symbol.clone());
            }
        }

        let index_price_bulk_count = index_prices.len();
        let fair_price_bulk_count = fair_prices.len();
        let funding_rate_bulk_count = funding_rates.len();

        let (mut index_endpoint_values, index_price_mode) = if missing_index_symbols.is_empty() {
            (Vec::new(), MexcBootstrapFetchMode::BulkOnly)
        } else {
            self.fetch_with_mode(concurrency, |effective_concurrency| {
                self.rest
                    .futures_index_price_for_symbols(&missing_index_symbols, effective_concurrency)
            })
            .await?
        };
        let (mut fair_endpoint_values, fair_price_mode) = if missing_fair_symbols.is_empty() {
            (Vec::new(), MexcBootstrapFetchMode::BulkOnly)
        } else {
            self.fetch_with_mode(concurrency, |effective_concurrency| {
                self.rest
                    .futures_fair_price_for_symbols(&missing_fair_symbols, effective_concurrency)
            })
            .await?
        };
        let (mut funding_endpoint_values, funding_rate_mode) = if missing_funding_symbols.is_empty()
        {
            (Vec::new(), MexcBootstrapFetchMode::BulkOnly)
        } else {
            self.fetch_with_mode(concurrency, |effective_concurrency| {
                self.rest.futures_funding_rate_for_symbols(
                    &missing_funding_symbols,
                    effective_concurrency,
                )
            })
            .await?
        };

        let index_price_endpoint_count = index_endpoint_values.len();
        let fair_price_endpoint_count = fair_endpoint_values.len();
        let funding_rate_endpoint_count = funding_endpoint_values.len();

        index_prices.append(&mut index_endpoint_values);
        fair_prices.append(&mut fair_endpoint_values);
        funding_rates.append(&mut funding_endpoint_values);

        let snapshot = MexcFuturesReferenceSnapshot {
            index_prices,
            fair_prices,
            funding_rates,
        };
        let report = MexcFuturesReferenceBootstrapReport {
            symbol_count: symbols.len(),
            index_price_count: snapshot.index_prices.len(),
            fair_price_count: snapshot.fair_prices.len(),
            funding_rate_count: snapshot.funding_rates.len(),
            index_price_bulk_count,
            fair_price_bulk_count,
            funding_rate_bulk_count,
            index_price_endpoint_count,
            fair_price_endpoint_count,
            funding_rate_endpoint_count,
            index_price_mode,
            fair_price_mode,
            funding_rate_mode,
        };
        Ok((snapshot, report))
    }

    pub async fn bootstrap_deep_snapshot(
        &self,
        concurrency: usize,
    ) -> Result<MexcPublicDeepBootstrapSnapshot> {
        let base = self.bootstrap_snapshot().await?;
        let (futures_reference, report) = self
            .bootstrap_futures_reference_snapshot(concurrency)
            .await?;
        Ok(MexcPublicDeepBootstrapSnapshot {
            base,
            futures_reference,
            report,
        })
    }
}

fn shard_spot_subscriptions(
    subscriptions: &[MexcSpotSubscription],
) -> Vec<Vec<MexcSpotSubscription>> {
    subscriptions
        .chunks(30)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn shard_futures_subscriptions(
    subscriptions: &[MexcFuturesSubscription],
    per_connection_limit: usize,
) -> Vec<Vec<MexcFuturesSubscription>> {
    subscriptions
        .chunks(per_connection_limit)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn latest_ticker_by_symbol(tickers: &[FuturesTicker]) -> BTreeMap<String, FuturesTicker> {
    let mut result: BTreeMap<String, FuturesTicker> = BTreeMap::new();
    for ticker in tickers {
        match result.get(&ticker.symbol) {
            Some(existing) => {
                let current_ts = ticker.timestamp.unwrap_or(0);
                let existing_ts = existing.timestamp.unwrap_or(0);
                if current_ts >= existing_ts {
                    result.insert(ticker.symbol.clone(), ticker.clone());
                }
            }
            None => {
                result.insert(ticker.symbol.clone(), ticker.clone());
            }
        }
    }
    result
}

async fn connect_futures_sharded(
    client: &MexcFuturesWsClient,
    subscriptions: Vec<MexcFuturesSubscription>,
    per_connection_limit: usize,
) -> Result<Vec<ReceiverStream<Result<MexcFuturesWsMessage>>>> {
    let mut streams = Vec::new();
    for shard in shard_futures_subscriptions(&subscriptions, per_connection_limit) {
        streams.push(client.connect(shard).await?);
    }
    Ok(streams)
}

async fn forward_spot_stream(
    mut stream: ReceiverStream<Result<MexcSpotWsMessage>>,
    tx: mpsc::Sender<Result<MexcPublicEvent>>,
) {
    while let Some(event) = stream.next().await {
        if tx.send(event.map(MexcPublicEvent::Spot)).await.is_err() {
            return;
        }
    }
}

async fn forward_futures_stream(
    mut stream: ReceiverStream<Result<MexcFuturesWsMessage>>,
    tx: mpsc::Sender<Result<MexcPublicEvent>>,
) {
    while let Some(event) = stream.next().await {
        if tx.send(event.map(MexcPublicEvent::Futures)).await.is_err() {
            return;
        }
    }
}
