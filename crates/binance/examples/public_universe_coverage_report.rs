use anyhow::{Result, bail};
use binance::{
    BinanceFuturesCoverageConfig, BinancePublicRestClient, BinanceSpotCoverageConfig,
    BinanceSymbolInfo, OneOrMany, build_futures_public_subscriptions,
    build_spot_public_subscriptions, covered_symbols,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = BinancePublicRestClient::default();

    let spot_exchange_info = rest.spot_exchange_info(None).await?;
    let futures_exchange_info = rest.futures_exchange_info(None).await?;

    let spot_symbols = sorted_symbols(&spot_exchange_info.symbols);
    let spot_trading_symbols = sorted_trading_symbols(&spot_exchange_info.symbols);
    let futures_symbols = sorted_symbols(&futures_exchange_info.symbols);
    let futures_trading_symbols = sorted_trading_symbols(&futures_exchange_info.symbols);

    let spot_plan =
        build_spot_public_subscriptions(&spot_symbols, &BinanceSpotCoverageConfig::exhaustive());
    let futures_plan = build_futures_public_subscriptions(
        &futures_symbols,
        &BinanceFuturesCoverageConfig::exhaustive(),
    );

    println!(
        "spot_universe exchange_info_total={} trading={} statuses={:?}",
        spot_symbols.len(),
        spot_trading_symbols.len(),
        status_breakdown(&spot_exchange_info.symbols)
    );
    print_plan_coverage("spot_exhaustive_ws_plan", &spot_symbols, &spot_plan)?;

    println!(
        "futures_universe exchange_info_total={} trading={} statuses={:?}",
        futures_symbols.len(),
        futures_trading_symbols.len(),
        status_breakdown(&futures_exchange_info.symbols)
    );
    print_plan_coverage(
        "futures_exhaustive_ws_plan",
        &futures_symbols,
        &futures_plan,
    )?;

    let spot_price_symbols = symbols_from_one_or_many(rest.spot_price_ticker(None).await?);
    let spot_book_symbols = symbols_from_one_or_many(rest.spot_book_ticker(None).await?);
    let spot_24hr_symbols = symbols_from_one_or_many(rest.spot_ticker_24hr(None).await?);
    print_rest_coverage(
        "spot_price_ticker",
        &spot_trading_symbols,
        &spot_price_symbols,
    )?;
    print_rest_coverage(
        "spot_book_ticker",
        &spot_trading_symbols,
        &spot_book_symbols,
    )?;
    print_optional_rest_coverage(
        "spot_24hr_ticker",
        &spot_trading_symbols,
        &spot_24hr_symbols,
    );

    let futures_price_symbols = symbols_from_one_or_many(rest.futures_price_ticker(None).await?);
    let futures_book_symbols = symbols_from_one_or_many(rest.futures_book_ticker(None).await?);
    let futures_24hr_symbols = symbols_from_one_or_many(rest.futures_ticker_24hr(None).await?);
    let futures_premium_symbols = symbols_from_one_or_many(rest.futures_premium_index(None).await?);
    print_rest_coverage(
        "futures_price_ticker",
        &futures_trading_symbols,
        &futures_price_symbols,
    )?;
    print_rest_coverage(
        "futures_book_ticker",
        &futures_trading_symbols,
        &futures_book_symbols,
    )?;
    print_rest_coverage(
        "futures_24hr_ticker",
        &futures_trading_symbols,
        &futures_24hr_symbols,
    )?;
    print_rest_coverage(
        "futures_premium_index",
        &futures_trading_symbols,
        &futures_premium_symbols,
    )?;

    println!(
        "sharding_hint spot_streams={} futures_streams={} at max_streams_per_connection=200",
        spot_plan.len().div_ceil(200),
        futures_plan.len().div_ceil(200)
    );

    Ok(())
}

fn sorted_symbols(symbols: &[BinanceSymbolInfo]) -> Vec<String> {
    let mut result = symbols
        .iter()
        .map(|symbol| symbol.symbol.clone())
        .collect::<Vec<_>>();
    result.sort();
    result.dedup();
    result
}

fn sorted_trading_symbols(symbols: &[BinanceSymbolInfo]) -> Vec<String> {
    let mut result = symbols
        .iter()
        .filter(|symbol| symbol.is_trading())
        .map(|symbol| symbol.symbol.clone())
        .collect::<Vec<_>>();
    result.sort();
    result.dedup();
    result
}

fn status_breakdown(symbols: &[BinanceSymbolInfo]) -> BTreeMap<String, usize> {
    let mut result = BTreeMap::new();
    for symbol in symbols {
        let status = symbol.status.as_deref().unwrap_or("UNKNOWN").to_string();
        *result.entry(status).or_default() += 1;
    }
    result
}

fn symbols_from_one_or_many(value: OneOrMany<Value>) -> BTreeSet<String> {
    match value {
        OneOrMany::One(value) => symbol_from_value(&value).into_iter().collect(),
        OneOrMany::Many(values) => values.iter().filter_map(symbol_from_value).collect(),
    }
}

fn symbol_from_value(value: &Value) -> Option<String> {
    value
        .get("symbol")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn print_plan_coverage(
    label: &str,
    universe: &[String],
    subscriptions: &[binance::BinanceWsSubscription],
) -> Result<()> {
    let universe_set = universe.iter().cloned().collect::<BTreeSet<_>>();
    let covered = covered_symbols(subscriptions);
    let missing = universe_set
        .difference(&covered)
        .cloned()
        .collect::<Vec<_>>();
    let pct = coverage_pct(universe.len() - missing.len(), universe.len());

    println!(
        "{label} subscriptions={} symbols_covered={}/{} coverage_pct={pct:.2} missing_count={} first_missing={:?}",
        subscriptions.len(),
        universe.len() - missing.len(),
        universe.len(),
        missing.len(),
        missing.iter().take(10).collect::<Vec<_>>()
    );

    if !missing.is_empty() {
        bail!("{label} does not cover the full exchangeInfo universe");
    }
    Ok(())
}

fn print_rest_coverage(
    label: &str,
    required_symbols: &[String],
    seen_symbols: &BTreeSet<String>,
) -> Result<()> {
    let missing = missing_symbols(required_symbols, seen_symbols);
    let pct = coverage_pct(
        required_symbols.len() - missing.len(),
        required_symbols.len(),
    );
    println!(
        "{label} trading_symbols_seen={}/{} coverage_pct={pct:.2} missing_count={} first_missing={:?}",
        required_symbols.len() - missing.len(),
        required_symbols.len(),
        missing.len(),
        missing.iter().take(10).collect::<Vec<_>>()
    );

    if !missing.is_empty() {
        bail!("{label} does not cover all TRADING exchangeInfo symbols");
    }
    Ok(())
}

fn print_optional_rest_coverage(
    label: &str,
    required_symbols: &[String],
    seen_symbols: &BTreeSet<String>,
) {
    let missing = missing_symbols(required_symbols, seen_symbols);
    let pct = coverage_pct(
        required_symbols.len() - missing.len(),
        required_symbols.len(),
    );
    println!(
        "{label} trading_symbols_seen={}/{} coverage_pct={pct:.2} missing_count={} first_missing={:?}",
        required_symbols.len() - missing.len(),
        required_symbols.len(),
        missing.len(),
        missing.iter().take(10).collect::<Vec<_>>()
    );
}

fn missing_symbols(required_symbols: &[String], seen_symbols: &BTreeSet<String>) -> Vec<String> {
    required_symbols
        .iter()
        .filter(|symbol| !seen_symbols.contains(*symbol))
        .cloned()
        .collect()
}

fn coverage_pct(seen: usize, total: usize) -> f64 {
    if total == 0 {
        100.0
    } else {
        (seen as f64 / total as f64) * 100.0
    }
}
