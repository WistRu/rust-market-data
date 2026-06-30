use anyhow::{Context, Result, ensure};
use mexc::{
    MexcPublicRestClient, SpotHistoricalArchivePeriod, SpotHistoricalDataKind,
    SpotHistoricalKlineInterval,
};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = MexcPublicRestClient::default();

    let symbol_ids = rest
        .spot_history_symbol_ids(SpotHistoricalDataKind::Kline)
        .await
        .context("load spot historical kline symbol ids")?;
    ensure!(
        !symbol_ids.is_empty(),
        "spot historical symbol id list is empty"
    );
    println!("ok spot_history_kline_symbol_ids {}", symbol_ids.len());

    let symbol_id = &symbol_ids[0];
    let periods = rest
        .spot_history_symbol_periods(SpotHistoricalDataKind::Kline, symbol_id)
        .await
        .with_context(|| format!("load historical periods for symbol id {symbol_id}"))?;
    println!("ok spot_history_symbol_periods {} {:?}", symbol_id, periods);

    let intervals = rest
        .spot_history_symbol_intervals(
            SpotHistoricalDataKind::Kline,
            symbol_id,
            SpotHistoricalArchivePeriod::Monthly,
        )
        .await
        .with_context(|| format!("load historical intervals for symbol id {symbol_id}"))?;
    println!(
        "ok spot_history_symbol_intervals {} {:?}",
        symbol_id, intervals
    );

    let files = rest
        .spot_history_kline_files(
            symbol_id,
            SpotHistoricalArchivePeriod::Monthly,
            SpotHistoricalKlineInterval::Week1,
        )
        .await
        .with_context(|| format!("load historical files for symbol id {symbol_id}"))?;
    ensure!(!files.is_empty(), "spot historical file list is empty");
    let first_file = &files[0];
    println!(
        "ok spot_history_kline_files {} first={} size={}",
        symbol_id, first_file.file_name, first_file.file_size
    );

    let trade_symbol_ids = rest
        .spot_history_symbol_ids(SpotHistoricalDataKind::Trades)
        .await
        .context("load spot historical trade symbol ids")?;
    println!(
        "ok spot_history_trade_symbol_ids {}",
        trade_symbol_ids.len()
    );
    if let Some(trade_symbol_id) = trade_symbol_ids.first() {
        let trade_files = rest
            .spot_history_trade_files(trade_symbol_id, SpotHistoricalArchivePeriod::Monthly)
            .await
            .with_context(|| {
                format!("load historical trade files for symbol id {trade_symbol_id}")
            })?;
        if let Some(first_trade_file) = trade_files.first() {
            println!(
                "ok spot_history_trade_files {} first={} size={}",
                trade_symbol_id, first_trade_file.file_name, first_trade_file.file_size
            );
        } else {
            println!("ok spot_history_trade_files {} empty", trade_symbol_id);
        }
    }

    let directory = rest
        .spot_history_symbols_for_symbol_id(symbol_id)
        .await
        .with_context(|| format!("infer symbols for historical symbol id {symbol_id}"))?;
    println!(
        "ok spot_history_symbols_for_symbol_id {} {:?}",
        directory.symbol_id, directory.symbols
    );

    let build_index = std::env::var("MEXC_HISTORY_BUILD_INDEX")
        .ok()
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if build_index {
        let symbol_map = rest
            .spot_history_symbol_id_map(16)
            .await
            .context("build historical symbol id map")?;
        println!("ok spot_history_symbol_id_map {}", symbol_map.len());
        if let Some(symbol_id) = symbol_map.get("BTC_USDT") {
            println!("ok spot_history_symbol_id_map BTC_USDT {}", symbol_id);
        }
    }

    Ok(())
}
