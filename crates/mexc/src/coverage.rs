use crate::{
    MexcFuturesKlineInterval, MexcFuturesSubscription, MexcPublicRestClient, MexcSpotKlineInterval,
    MexcSpotSubscription, MexcSpotUpdateSpeed, MexcTimezone,
};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct MexcSpotCoverageConfig {
    pub trade_speeds: Vec<MexcSpotUpdateSpeed>,
    pub include_increase_depth: bool,
    pub include_increase_depth_batch: bool,
    pub depth_speeds: Vec<MexcSpotUpdateSpeed>,
    pub partial_depth_levels: Vec<u16>,
    pub include_book_ticker: bool,
    pub include_book_ticker_batch: bool,
    pub include_book_ticker_raw: bool,
    pub kline_intervals: Vec<MexcSpotKlineInterval>,
    pub mini_ticker_timezones: Vec<MexcTimezone>,
    pub include_per_symbol_mini_ticker: bool,
    pub include_mini_tickers: bool,
}

impl MexcSpotCoverageConfig {
    pub fn balanced() -> Self {
        Self {
            trade_speeds: vec![MexcSpotUpdateSpeed::Ms100],
            include_increase_depth: false,
            include_increase_depth_batch: false,
            depth_speeds: vec![MexcSpotUpdateSpeed::Ms100],
            partial_depth_levels: Vec::new(),
            include_book_ticker: false,
            include_book_ticker_batch: true,
            include_book_ticker_raw: false,
            kline_intervals: vec![MexcSpotKlineInterval::Min1],
            mini_ticker_timezones: vec![MexcTimezone::UTC_PLUS_0, MexcTimezone::UTC_PLUS_8],
            include_per_symbol_mini_ticker: false,
            include_mini_tickers: true,
        }
    }

    pub fn exhaustive() -> Self {
        Self {
            trade_speeds: vec![MexcSpotUpdateSpeed::Ms10, MexcSpotUpdateSpeed::Ms100],
            include_increase_depth: false,
            include_increase_depth_batch: true,
            depth_speeds: vec![MexcSpotUpdateSpeed::Ms10, MexcSpotUpdateSpeed::Ms100],
            partial_depth_levels: vec![5, 10, 20],
            include_book_ticker: true,
            include_book_ticker_batch: true,
            include_book_ticker_raw: false,
            kline_intervals: MexcSpotKlineInterval::all(),
            mini_ticker_timezones: MexcTimezone::all(),
            include_per_symbol_mini_ticker: true,
            include_mini_tickers: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MexcFuturesCoverageConfig {
    pub include_all_tickers: bool,
    pub include_per_symbol_ticker: bool,
    pub include_deals: bool,
    pub include_depth: bool,
    pub depth_step_sizes: Vec<String>,
    pub include_depth_full: bool,
    pub include_funding_rate: bool,
    pub include_index_price: bool,
    pub include_fair_price: bool,
    pub kline_intervals: Vec<MexcFuturesKlineInterval>,
    pub include_contract_snapshot_stream: bool,
    pub include_contract_event_stream: bool,
}

impl MexcFuturesCoverageConfig {
    pub fn balanced() -> Self {
        Self {
            include_all_tickers: true,
            include_per_symbol_ticker: true,
            include_deals: true,
            include_depth: true,
            depth_step_sizes: vec!["10".to_string()],
            include_depth_full: false,
            include_funding_rate: true,
            include_index_price: true,
            include_fair_price: true,
            kline_intervals: vec![MexcFuturesKlineInterval::Min1],
            include_contract_snapshot_stream: true,
            include_contract_event_stream: true,
        }
    }

    pub fn exhaustive() -> Self {
        Self {
            include_all_tickers: true,
            include_per_symbol_ticker: true,
            include_deals: true,
            include_depth: true,
            depth_step_sizes: vec!["10".to_string(), "100".to_string(), "1000".to_string()],
            include_depth_full: false,
            include_funding_rate: true,
            include_index_price: true,
            include_fair_price: true,
            kline_intervals: MexcFuturesKlineInterval::all(),
            include_contract_snapshot_stream: true,
            include_contract_event_stream: true,
        }
    }
}

pub fn build_spot_public_subscriptions(
    symbols: &[String],
    config: &MexcSpotCoverageConfig,
) -> Vec<MexcSpotSubscription> {
    let mut subscriptions = Vec::new();

    for symbol in symbols {
        for speed in &config.trade_speeds {
            subscriptions.push(MexcSpotSubscription::AggTrades {
                symbol: symbol.clone(),
                speed: *speed,
            });
        }

        if config.include_increase_depth {
            subscriptions.push(MexcSpotSubscription::IncreaseDepth {
                symbol: symbol.clone(),
            });
        }

        if config.include_increase_depth_batch {
            subscriptions.push(MexcSpotSubscription::IncreaseDepthBatch {
                symbol: symbol.clone(),
            });
        }

        for speed in &config.depth_speeds {
            subscriptions.push(MexcSpotSubscription::AggDepth {
                symbol: symbol.clone(),
                speed: *speed,
            });
        }

        for level in &config.partial_depth_levels {
            subscriptions.push(MexcSpotSubscription::LimitDepth {
                symbol: symbol.clone(),
                level: *level,
            });
        }

        if config.include_book_ticker {
            for speed in &config.depth_speeds {
                subscriptions.push(MexcSpotSubscription::BookTicker {
                    symbol: symbol.clone(),
                    speed: *speed,
                });
            }
        }

        if config.include_book_ticker_raw {
            subscriptions.push(MexcSpotSubscription::AggBookTicker {
                symbol: symbol.clone(),
            });
        }

        for interval in &config.kline_intervals {
            subscriptions.push(MexcSpotSubscription::Kline {
                symbol: symbol.clone(),
                interval: *interval,
            });
        }

        if config.include_per_symbol_mini_ticker {
            for timezone in &config.mini_ticker_timezones {
                subscriptions.push(MexcSpotSubscription::MiniTicker {
                    symbol: symbol.clone(),
                    timezone: *timezone,
                });
            }
        }
    }

    if config.include_book_ticker_batch {
        for symbol in symbols {
            subscriptions.push(MexcSpotSubscription::BookTickerBatch {
                symbol: symbol.clone(),
            });
        }
    }

    if config.include_mini_tickers {
        for timezone in &config.mini_ticker_timezones {
            subscriptions.push(MexcSpotSubscription::MiniTickers {
                timezone: *timezone,
            });
        }
    }

    subscriptions
}

pub fn build_futures_public_subscriptions(
    symbols: &[String],
    config: &MexcFuturesCoverageConfig,
) -> Vec<MexcFuturesSubscription> {
    let mut subscriptions = Vec::new();

    if config.include_all_tickers {
        subscriptions.push(MexcFuturesSubscription::Tickers);
    }
    if config.include_contract_snapshot_stream {
        subscriptions.push(MexcFuturesSubscription::Contract);
    }
    if config.include_contract_event_stream {
        subscriptions.push(MexcFuturesSubscription::EventContract);
    }

    for symbol in symbols {
        if config.include_per_symbol_ticker {
            subscriptions.push(MexcFuturesSubscription::Ticker {
                symbol: symbol.clone(),
            });
        }
        if config.include_deals {
            subscriptions.push(MexcFuturesSubscription::Deals {
                symbol: symbol.clone(),
            });
        }
        if config.include_depth {
            subscriptions.push(MexcFuturesSubscription::Depth {
                symbol: symbol.clone(),
                compress: None,
            });
        }
        for step in &config.depth_step_sizes {
            subscriptions.push(MexcFuturesSubscription::DepthStep {
                symbol: symbol.clone(),
                step: step.clone(),
            });
        }
        if config.include_depth_full {
            subscriptions.push(MexcFuturesSubscription::DepthFull {
                symbol: symbol.clone(),
                limit: None,
            });
        }
        if config.include_funding_rate {
            subscriptions.push(MexcFuturesSubscription::FundingRate {
                symbol: symbol.clone(),
            });
        }
        if config.include_index_price {
            subscriptions.push(MexcFuturesSubscription::IndexPrice {
                symbol: symbol.clone(),
            });
        }
        if config.include_fair_price {
            subscriptions.push(MexcFuturesSubscription::FairPrice {
                symbol: symbol.clone(),
            });
        }
        for interval in &config.kline_intervals {
            subscriptions.push(MexcFuturesSubscription::Kline {
                symbol: symbol.clone(),
                interval: *interval,
            });
        }
    }

    subscriptions
}

pub async fn build_full_public_subscription_sets(
    rest: &MexcPublicRestClient,
    spot_config: &MexcSpotCoverageConfig,
    futures_config: &MexcFuturesCoverageConfig,
) -> Result<(Vec<MexcSpotSubscription>, Vec<MexcFuturesSubscription>)> {
    let (spot_symbols, futures_symbols) = rest.all_public_symbols().await?;
    Ok((
        build_spot_public_subscriptions(&spot_symbols, spot_config),
        build_futures_public_subscriptions(&futures_symbols, futures_config),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhaustive_spot_plan_expands_symbol_matrix() {
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let plan = build_spot_public_subscriptions(&symbols, &MexcSpotCoverageConfig::balanced());
        assert!(plan.len() > symbols.len());
    }

    #[test]
    fn futures_plan_contains_global_and_per_symbol_channels() {
        let symbols = vec!["BTC_USDT".to_string()];
        let plan =
            build_futures_public_subscriptions(&symbols, &MexcFuturesCoverageConfig::balanced());
        assert!(plan.contains(&MexcFuturesSubscription::Tickers));
        assert!(plan.contains(&MexcFuturesSubscription::Contract));
        assert!(plan.contains(&MexcFuturesSubscription::Ticker {
            symbol: "BTC_USDT".to_string(),
        }));
    }

    #[test]
    fn balanced_spot_plan_avoids_redundant_per_symbol_channels() {
        let symbols = vec!["BTCUSDT".to_string()];
        let plan = build_spot_public_subscriptions(&symbols, &MexcSpotCoverageConfig::balanced());
        assert!(plan.contains(&MexcSpotSubscription::AggTrades {
            symbol: "BTCUSDT".to_string(),
            speed: MexcSpotUpdateSpeed::Ms100,
        }));
        assert!(plan.contains(&MexcSpotSubscription::AggDepth {
            symbol: "BTCUSDT".to_string(),
            speed: MexcSpotUpdateSpeed::Ms100,
        }));
        assert!(plan.contains(&MexcSpotSubscription::BookTickerBatch {
            symbol: "BTCUSDT".to_string(),
        }));
        assert!(plan.contains(&MexcSpotSubscription::Kline {
            symbol: "BTCUSDT".to_string(),
            interval: MexcSpotKlineInterval::Min1,
        }));
        assert!(plan.contains(&MexcSpotSubscription::MiniTickers {
            timezone: MexcTimezone::UTC_PLUS_0,
        }));
        assert!(
            !plan.iter().any(|subscription| matches!(
                subscription,
                MexcSpotSubscription::LimitDepth { .. }
            ))
        );
        assert!(
            !plan.iter().any(|subscription| matches!(
                subscription,
                MexcSpotSubscription::BookTicker { .. }
            ))
        );
        assert!(
            !plan.iter().any(|subscription| matches!(
                subscription,
                MexcSpotSubscription::MiniTicker { .. }
            ))
        );
    }
}
