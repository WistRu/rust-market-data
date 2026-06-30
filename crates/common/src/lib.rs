use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MarketDataChannel {
    Trades,
    OrderBook,
    Ticker,
    Liquidations,
    Funding,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Subscription {
    pub symbol: String,
    pub channel: MarketDataChannel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketEvent {
    pub exchange: &'static str,
    pub channel: MarketDataChannel,
    pub symbol: String,
    pub payload: String,
}

#[async_trait]
pub trait MarketDataConnector: Send + Sync {
    fn exchange(&self) -> &'static str;
    fn ws_endpoint(&self) -> &'static str;
    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String>;

    async fn connect(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum ReadinessStatus {
    ScaffoldOnly,
    Partial,
    HandoffReady,
}

impl fmt::Display for ReadinessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::ScaffoldOnly => "scaffold-only",
            Self::Partial => "partial",
            Self::HandoffReady => "handoff-ready",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
    Skipped,
}

impl fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
            Self::Skipped => "skipped",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AcceptanceCheck {
    pub name: String,
    pub status: CheckStatus,
    pub detail: String,
}

impl AcceptanceCheck {
    pub fn pass(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Pass,
            detail: detail.into(),
        }
    }

    pub fn warn(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Warn,
            detail: detail.into(),
        }
    }

    pub fn fail(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Fail,
            detail: detail.into(),
        }
    }

    pub fn skipped(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Skipped,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoverageSummary {
    pub label: String,
    pub required: usize,
    pub covered: usize,
    pub coverage_pct: f64,
    pub missing_count: usize,
    pub first_missing: Vec<String>,
}

impl CoverageSummary {
    pub fn from_symbols(
        label: impl Into<String>,
        required_symbols: impl IntoIterator<Item = String>,
        covered_symbols: impl IntoIterator<Item = String>,
    ) -> Self {
        let required = required_symbols.into_iter().collect::<BTreeSet<_>>();
        let covered = covered_symbols.into_iter().collect::<BTreeSet<_>>();
        let missing = required
            .difference(&covered)
            .take(20)
            .cloned()
            .collect::<Vec<_>>();
        let required_len = required.len();
        let covered_len = required_len.saturating_sub(required.difference(&covered).count());
        Self::from_counts(label, required_len, covered_len, missing)
    }

    pub fn from_counts(
        label: impl Into<String>,
        required: usize,
        covered: usize,
        first_missing: Vec<String>,
    ) -> Self {
        let missing_count = required.saturating_sub(covered);
        let coverage_pct = if required == 0 {
            100.0
        } else {
            (covered as f64 / required as f64) * 100.0
        };
        Self {
            label: label.into(),
            required,
            covered,
            coverage_pct,
            missing_count,
            first_missing,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.required == self.covered && self.missing_count == 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionPlanSummary {
    pub label: String,
    pub subscriptions: usize,
    pub max_streams_per_connection: usize,
    pub connection_count: usize,
}

impl SubscriptionPlanSummary {
    pub fn new(
        label: impl Into<String>,
        subscriptions: usize,
        max_streams_per_connection: usize,
    ) -> Self {
        let max_streams_per_connection = max_streams_per_connection.max(1);
        Self {
            label: label.into(),
            subscriptions,
            max_streams_per_connection,
            connection_count: subscriptions.div_ceil(max_streams_per_connection),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeInventoryItem {
    pub exchange: String,
    pub crate_name: String,
    pub status: ReadinessStatus,
    pub ws_endpoint: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeAcceptanceReport {
    pub exchange: String,
    pub crate_name: String,
    pub status: ReadinessStatus,
    pub rest: Vec<AcceptanceCheck>,
    pub ws: Vec<AcceptanceCheck>,
    pub coverage: Vec<CoverageSummary>,
    pub subscription_plans: Vec<SubscriptionPlanSummary>,
    pub quirks: Vec<String>,
    pub live: bool,
}

impl ExchangeAcceptanceReport {
    pub fn has_failures(&self) -> bool {
        self.rest
            .iter()
            .chain(self.ws.iter())
            .any(|check| check.status == CheckStatus::Fail)
            || self.coverage.iter().any(|coverage| !coverage.is_complete())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_status_serializes_as_contract_vocabulary() {
        assert_eq!(
            serde_json::to_string(&ReadinessStatus::HandoffReady).unwrap(),
            "\"handoff-ready\""
        );
        assert_eq!(ReadinessStatus::ScaffoldOnly.to_string(), "scaffold-only");
    }

    #[test]
    fn coverage_summary_lists_missing_symbols_and_percentage() {
        let summary = CoverageSummary::from_symbols(
            "spot",
            ["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            ["BTCUSDT".to_string()],
        );

        assert_eq!(summary.required, 2);
        assert_eq!(summary.covered, 1);
        assert_eq!(summary.missing_count, 1);
        assert_eq!(summary.first_missing, vec!["ETHUSDT"]);
        assert_eq!(summary.coverage_pct, 50.0);
        assert!(!summary.is_complete());
    }

    #[test]
    fn subscription_plan_uses_ceil_connection_count() {
        let plan = SubscriptionPlanSummary::new("large", 401, 200);
        assert_eq!(plan.connection_count, 3);
    }
}
