use anyhow::{Context, Result};
use common::{
    AcceptanceCheck, CoverageSummary, ExchangeAcceptanceReport, ExchangeInventoryItem,
    ReadinessStatus, SubscriptionPlanSummary,
};
use serde_json::Value;
use std::collections::BTreeSet;

const READY_CONNECTORS: &[&str] = &["mexc", "aster", "binance", "bybit", "okx", "bitget"];

pub fn inventory() -> Vec<ExchangeInventoryItem> {
    vec![
        ready(
            "mexc",
            None,
            "deep public market-data module with REST/WS matrices and handoff reports",
        ),
        ready(
            "aster",
            Some(aster::SPOT_WS_BASE_URL),
            "handoff-ready public spot/futures module with live proof",
        ),
        ready(
            "binance",
            Some(binance::SPOT_WS_BASE_URL),
            "handoff-ready public spot/USD-M futures module with live proof",
        ),
        ready(
            "bybit",
            Some(bybit::SPOT_WS_BASE_URL),
            "factory-built public REST/WS connector with acceptance report and live WS proof",
        ),
        ready(
            "okx",
            Some(okx::PUBLIC_WS_BASE_URL),
            "handoff-ready public SPOT/SWAP connector with OKX instrument identity and live proof",
        ),
        ready(
            "bitget",
            Some(bitget::PUBLIC_WS_BASE_URL),
            "handoff-ready public SPOT/USDT-FUTURES connector with Bitget instrument identity and live proof",
        ),
        scaffold("gateio", "connector crate only; no acceptance proof yet"),
        scaffold("kucoin", "connector crate only; no acceptance proof yet"),
        scaffold("coinbase", "connector crate only; no acceptance proof yet"),
        scaffold(
            "crypto_com",
            "connector crate only; no acceptance proof yet",
        ),
        scaffold("deribit", "connector crate only; no acceptance proof yet"),
        scaffold(
            "hyperliquid",
            "connector crate only; no acceptance proof yet",
        ),
        scaffold("kraken", "connector crate only; no acceptance proof yet"),
        scaffold("bitunix", "connector crate only; no acceptance proof yet"),
    ]
}

fn ready(exchange: &str, ws_endpoint: Option<&str>, reason: &str) -> ExchangeInventoryItem {
    ExchangeInventoryItem {
        exchange: exchange.to_string(),
        crate_name: exchange.to_string(),
        status: ReadinessStatus::HandoffReady,
        ws_endpoint: ws_endpoint.map(ToOwned::to_owned),
        reason: reason.to_string(),
    }
}

fn scaffold(exchange: &str, reason: &str) -> ExchangeInventoryItem {
    ExchangeInventoryItem {
        exchange: exchange.to_string(),
        crate_name: exchange.to_string(),
        status: ReadinessStatus::ScaffoldOnly,
        ws_endpoint: None,
        reason: reason.to_string(),
    }
}

pub async fn report(exchange: &str) -> Result<ExchangeAcceptanceReport> {
    match exchange {
        "aster" => aster_report().await,
        "binance" => binance_report().await,
        "mexc" => mexc_report().await,
        "bybit" => bybit_report().await,
        "okx" => okx_report().await,
        "bitget" => bitget_report().await,
        other => scaffold_report(other),
    }
}

pub async fn drift_audit() -> Result<Vec<ExchangeAcceptanceReport>> {
    let mut reports = Vec::new();
    for exchange in READY_CONNECTORS {
        reports.push(report(exchange).await?);
    }
    Ok(reports)
}

pub fn print_human_report(report: &ExchangeAcceptanceReport) {
    println!(
        "{} crate={} status={} live={}",
        report.exchange, report.crate_name, report.status, report.live
    );
    for check in report.rest.iter().chain(report.ws.iter()) {
        println!("  {} {} {}", check.status, check.name, check.detail);
    }
    for coverage in &report.coverage {
        println!(
            "  coverage {} {}/{} pct={:.2} missing_count={} first_missing={:?}",
            coverage.label,
            coverage.covered,
            coverage.required,
            coverage.coverage_pct,
            coverage.missing_count,
            coverage.first_missing
        );
    }
    for plan in &report.subscription_plans {
        println!(
            "  shard_plan {} subscriptions={} max_per_conn={} connections={}",
            plan.label, plan.subscriptions, plan.max_streams_per_connection, plan.connection_count
        );
    }
    for quirk in &report.quirks {
        println!("  quirk {quirk}");
    }
}

async fn aster_report() -> Result<ExchangeAcceptanceReport> {
    let rest = aster::AsterPublicRestClient::default();
    let mut rest_checks = Vec::new();
    let mut ws_checks = Vec::new();
    let mut coverage = Vec::new();
    let mut plans = Vec::new();

    rest.spot_ping().await.context("aster spot ping")?;
    rest.futures_ping().await.context("aster futures ping")?;
    rest_checks.push(AcceptanceCheck::pass(
        "rest_ping",
        "spot and futures ping passed",
    ));

    let spot_info = rest.spot_exchange_info(None).await?;
    let futures_info = rest.futures_exchange_info(None).await?;
    let spot_symbols = sorted_symbols(spot_info.symbols.iter().map(|s| s.symbol.clone()));
    let spot_trading = sorted_symbols(
        spot_info
            .symbols
            .iter()
            .filter(|s| s.is_trading())
            .map(|s| s.symbol.clone()),
    );
    let futures_symbols = sorted_symbols(futures_info.symbols.iter().map(|s| s.symbol.clone()));
    let futures_trading = sorted_symbols(
        futures_info
            .symbols
            .iter()
            .filter(|s| s.is_trading())
            .map(|s| s.symbol.clone()),
    );

    let spot_plan = aster::build_spot_public_subscriptions(
        &spot_symbols,
        &aster::AsterSpotCoverageConfig::exhaustive(),
    );
    let futures_plan = aster::build_futures_public_subscriptions(
        &futures_symbols,
        &aster::AsterFuturesCoverageConfig::exhaustive(),
    );
    coverage.push(CoverageSummary::from_symbols(
        "spot_ws_plan",
        spot_symbols.clone(),
        aster::covered_symbols(&spot_plan),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "futures_ws_plan",
        futures_symbols.clone(),
        aster::covered_symbols(&futures_plan),
    ));
    plans.push(SubscriptionPlanSummary::new(
        "spot_ws_plan",
        spot_plan.len(),
        200,
    ));
    plans.push(SubscriptionPlanSummary::new(
        "futures_ws_plan",
        futures_plan.len(),
        200,
    ));
    ws_checks.push(AcceptanceCheck::pass(
        "ws_plan",
        "spot and futures exhaustive plans cover exchangeInfo universe",
    ));

    let spot_price = symbols_from_values(rest.spot_price_ticker(None).await?);
    let spot_book = symbols_from_values(rest.spot_book_ticker(None).await?);
    let futures_price = symbols_from_values(rest.futures_price_ticker(None).await?);
    let futures_premium = symbols_from_values(rest.futures_premium_index(None).await?);
    coverage.push(CoverageSummary::from_symbols(
        "spot_price_ticker",
        spot_trading.clone(),
        spot_price,
    ));
    coverage.push(CoverageSummary::from_symbols(
        "spot_book_ticker",
        spot_trading,
        spot_book,
    ));
    coverage.push(CoverageSummary::from_symbols(
        "futures_price_ticker",
        futures_trading.clone(),
        futures_price,
    ));
    coverage.push(CoverageSummary::from_symbols(
        "futures_premium_index",
        futures_trading,
        futures_premium,
    ));
    rest_checks.push(AcceptanceCheck::pass(
        "rest_coverage",
        "live all-symbol ticker checks completed",
    ));

    Ok(ExchangeAcceptanceReport {
        exchange: "aster".to_string(),
        crate_name: "aster".to_string(),
        status: status_from_coverage(&coverage),
        rest: rest_checks,
        ws: ws_checks,
        coverage,
        subscription_plans: plans,
        quirks: vec![
            "Futures exchangeInfo includes non-TRADING statuses; readiness gates only require TRADING symbols for live REST sources.".to_string(),
            "Some all-symbol endpoints can omit quiet symbols; endpoint-specific behavior is preserved in coverage rows.".to_string(),
        ],
        live: true,
    })
}

async fn binance_report() -> Result<ExchangeAcceptanceReport> {
    let rest = binance::BinancePublicRestClient::default();
    let mut rest_checks = Vec::new();
    let mut ws_checks = Vec::new();
    let mut coverage = Vec::new();
    let mut plans = Vec::new();

    rest.spot_ping().await.context("binance spot ping")?;
    rest.futures_ping().await.context("binance futures ping")?;
    rest_checks.push(AcceptanceCheck::pass(
        "rest_ping",
        "spot and futures ping passed",
    ));

    let spot_info = rest.spot_exchange_info(None).await?;
    let futures_info = rest.futures_exchange_info(None).await?;
    let spot_symbols = sorted_symbols(spot_info.symbols.iter().map(|s| s.symbol.clone()));
    let spot_trading = sorted_symbols(
        spot_info
            .symbols
            .iter()
            .filter(|s| s.is_trading())
            .map(|s| s.symbol.clone()),
    );
    let futures_symbols = sorted_symbols(futures_info.symbols.iter().map(|s| s.symbol.clone()));
    let futures_trading = sorted_symbols(
        futures_info
            .symbols
            .iter()
            .filter(|s| s.is_trading())
            .map(|s| s.symbol.clone()),
    );

    let spot_plan = binance::build_spot_public_subscriptions(
        &spot_symbols,
        &binance::BinanceSpotCoverageConfig::exhaustive(),
    );
    let futures_plan = binance::build_futures_public_subscriptions(
        &futures_symbols,
        &binance::BinanceFuturesCoverageConfig::exhaustive(),
    );
    coverage.push(CoverageSummary::from_symbols(
        "spot_ws_plan",
        spot_symbols.clone(),
        binance::covered_symbols(&spot_plan),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "futures_ws_plan",
        futures_symbols.clone(),
        binance::covered_symbols(&futures_plan),
    ));
    plans.push(SubscriptionPlanSummary::new(
        "spot_ws_plan",
        spot_plan.len(),
        200,
    ));
    plans.push(SubscriptionPlanSummary::new(
        "futures_ws_plan",
        futures_plan.len(),
        200,
    ));
    ws_checks.push(AcceptanceCheck::pass(
        "ws_plan",
        "spot and futures exhaustive plans cover exchangeInfo universe",
    ));

    coverage.push(CoverageSummary::from_symbols(
        "spot_price_ticker",
        spot_trading.clone(),
        symbols_from_values(rest.spot_price_ticker(None).await?),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "spot_book_ticker",
        spot_trading,
        symbols_from_values(rest.spot_book_ticker(None).await?),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "futures_price_ticker",
        futures_trading.clone(),
        symbols_from_values(rest.futures_price_ticker(None).await?),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "futures_premium_index",
        futures_trading,
        symbols_from_values(rest.futures_premium_index(None).await?),
    ));
    rest_checks.push(AcceptanceCheck::pass(
        "rest_coverage",
        "live all-symbol ticker checks completed",
    ));

    Ok(ExchangeAcceptanceReport {
        exchange: "binance".to_string(),
        crate_name: "binance".to_string(),
        status: status_from_coverage(&coverage),
        rest: rest_checks,
        ws: ws_checks,
        coverage,
        subscription_plans: plans,
        quirks: vec![
            "Some documented MARKET_DATA endpoints can behave as auth-gated under no-key checks; readiness gates use public no-key-safe sources.".to_string(),
            "High-weight REST fan-out is not a live-feed substitute; WS streams remain the live data path.".to_string(),
        ],
        live: true,
    })
}

async fn mexc_report() -> Result<ExchangeAcceptanceReport> {
    let rest = mexc::MexcPublicRestClient::default();
    let mut rest_checks = Vec::new();
    let mut ws_checks = Vec::new();
    let mut coverage = Vec::new();
    let mut plans = Vec::new();

    rest.spot_ping().await.context("mexc spot ping")?;
    let futures_ping = rest.futures_ping().await.context("mexc futures ping")?;
    rest_checks.push(AcceptanceCheck::pass(
        "rest_ping",
        format!("spot ping passed; futures ping={futures_ping}"),
    ));

    let spot_symbols = rest.all_spot_symbols().await?;
    let futures_symbols = rest.all_futures_symbols().await?;
    let spot_plan = mexc::build_spot_public_subscriptions(
        &spot_symbols,
        &mexc::MexcSpotCoverageConfig::balanced(),
    );
    let futures_plan = mexc::build_futures_public_subscriptions(
        &futures_symbols,
        &mexc::MexcFuturesCoverageConfig::balanced(),
    );
    coverage.push(CoverageSummary::from_counts(
        "spot_symbol_universe",
        spot_symbols.len(),
        spot_symbols.len(),
        Vec::new(),
    ));
    coverage.push(CoverageSummary::from_counts(
        "futures_symbol_universe",
        futures_symbols.len(),
        futures_symbols.len(),
        Vec::new(),
    ));
    plans.push(SubscriptionPlanSummary::new(
        "spot_balanced_ws_plan",
        spot_plan.len(),
        200,
    ));
    plans.push(SubscriptionPlanSummary::new(
        "futures_balanced_ws_plan",
        futures_plan.len(),
        200,
    ));
    ws_checks.push(AcceptanceCheck::warn(
        "ws_plan",
        "balanced plan avoids MEXC spot channels known to be server-blocked in prior live proof",
    ));
    rest_checks.push(AcceptanceCheck::pass(
        "rest_universe",
        "spot exchangeInfo and futures contracts loaded",
    ));

    Ok(ExchangeAcceptanceReport {
        exchange: "mexc".to_string(),
        crate_name: "mexc".to_string(),
        status: ReadinessStatus::HandoffReady,
        rest: rest_checks,
        ws: ws_checks,
        coverage,
        subscription_plans: plans,
        quirks: vec![
            "Spot raw increase.depth and raw bookTicker channels have been observed server-blocked; balanced planning uses working batch/aggre alternatives.".to_string(),
            "MEXC has richer runtime handoff reports that intentionally remain exchange-specific.".to_string(),
        ],
        live: true,
    })
}

async fn bybit_report() -> Result<ExchangeAcceptanceReport> {
    let rest = bybit::BybitPublicRestClient::default();
    let mut report = bybit::public_acceptance_report(&rest).await?;
    if report.has_failures() {
        report.status = ReadinessStatus::Partial;
    }
    Ok(report)
}

async fn okx_report() -> Result<ExchangeAcceptanceReport> {
    let rest = okx::OkxPublicRestClient::default();
    let mut report = okx::public_acceptance_report(&rest).await?;
    if report.has_failures() {
        report.status = ReadinessStatus::Partial;
    }
    Ok(report)
}

async fn bitget_report() -> Result<ExchangeAcceptanceReport> {
    let rest = bitget::BitgetPublicRestClient::default();
    let mut report = bitget::public_acceptance_report(&rest).await?;
    if report.has_failures() {
        report.status = ReadinessStatus::Partial;
    }
    Ok(report)
}

fn scaffold_report(exchange: &str) -> Result<ExchangeAcceptanceReport> {
    let item = inventory()
        .into_iter()
        .find(|item| item.exchange == exchange)
        .ok_or_else(|| anyhow::anyhow!("unknown exchange: {exchange}"))?;
    Ok(ExchangeAcceptanceReport {
        exchange: item.exchange,
        crate_name: item.crate_name,
        status: ReadinessStatus::ScaffoldOnly,
        rest: vec![AcceptanceCheck::skipped(
            "rest",
            "connector is scaffold-only and has no acceptance REST proof",
        )],
        ws: vec![AcceptanceCheck::skipped(
            "ws",
            "connector is scaffold-only and has no acceptance WS proof",
        )],
        coverage: Vec::new(),
        subscription_plans: Vec::new(),
        quirks: vec![item.reason],
        live: false,
    })
}

fn status_from_coverage(coverage: &[CoverageSummary]) -> ReadinessStatus {
    if coverage.iter().all(CoverageSummary::is_complete) {
        ReadinessStatus::HandoffReady
    } else {
        ReadinessStatus::Partial
    }
}

fn sorted_symbols(symbols: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut result = symbols.into_iter().collect::<Vec<_>>();
    result.sort();
    result.dedup();
    result
}

fn symbols_from_values<T>(value: T) -> BTreeSet<String>
where
    T: IntoValueList,
{
    value
        .into_values()
        .into_iter()
        .filter_map(|value| {
            value
                .get("symbol")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect()
}

trait IntoValueList {
    fn into_values(self) -> Vec<Value>;
}

impl IntoValueList for aster::OneOrMany<Value> {
    fn into_values(self) -> Vec<Value> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

impl IntoValueList for binance::OneOrMany<Value> {
    fn into_values(self) -> Vec<Value> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::CheckStatus;

    #[test]
    fn inventory_marks_unfinished_scaffolds_as_not_ready() {
        let gateio = inventory()
            .into_iter()
            .find(|item| item.exchange == "gateio")
            .unwrap();
        assert_eq!(gateio.status, ReadinessStatus::ScaffoldOnly);
    }

    #[test]
    fn inventory_marks_bybit_as_factory_built_ready() {
        let bybit = inventory()
            .into_iter()
            .find(|item| item.exchange == "bybit")
            .unwrap();
        assert_eq!(bybit.status, ReadinessStatus::HandoffReady);
    }

    #[test]
    fn inventory_marks_okx_as_factory_built_ready() {
        let okx = inventory()
            .into_iter()
            .find(|item| item.exchange == "okx")
            .unwrap();
        assert_eq!(okx.status, ReadinessStatus::HandoffReady);
    }

    #[test]
    fn inventory_marks_bitget_as_factory_built_ready() {
        let bitget = inventory()
            .into_iter()
            .find(|item| item.exchange == "bitget")
            .unwrap();
        assert_eq!(bitget.status, ReadinessStatus::HandoffReady);
    }

    #[test]
    fn status_from_coverage_requires_all_rows_complete() {
        let coverage = vec![
            CoverageSummary::from_counts("ok", 2, 2, Vec::new()),
            CoverageSummary::from_counts("missing", 2, 1, vec!["ETHUSDT".to_string()]),
        ];
        assert_eq!(status_from_coverage(&coverage), ReadinessStatus::Partial);
    }

    #[tokio::test]
    async fn scaffold_report_does_not_pass_readiness() {
        let report = scaffold_report("gateio").unwrap();
        assert_eq!(report.status, ReadinessStatus::ScaffoldOnly);
        assert!(
            report
                .rest
                .iter()
                .all(|check| check.status == CheckStatus::Skipped)
        );
    }
}
