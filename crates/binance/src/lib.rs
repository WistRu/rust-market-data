use anyhow::{Context, Result, anyhow};
use common::{MarketDataChannel, MarketDataConnector, Subscription};
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub const SPOT_REST_BASE_URL: &str = "https://api.binance.com";
pub const FUTURES_REST_BASE_URL: &str = "https://fapi.binance.com";
pub const FUTURES_DATA_BASE_URL: &str = "https://fapi.binance.com";
pub const SPOT_WS_BASE_URL: &str = "wss://stream.binance.com:9443";
pub const FUTURES_WS_BASE_URL: &str = "wss://fstream.binance.com";

pub const BINANCE_KLINE_INTERVALS: &[&str] = &[
    "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w",
    "1M",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceMarket {
    Spot,
    UsdmFutures,
}

impl BinanceMarket {
    pub fn rest_base_url(self) -> &'static str {
        match self {
            Self::Spot => SPOT_REST_BASE_URL,
            Self::UsdmFutures => FUTURES_REST_BASE_URL,
        }
    }

    pub fn ws_base_url(self) -> &'static str {
        match self {
            Self::Spot => SPOT_WS_BASE_URL,
            Self::UsdmFutures => FUTURES_WS_BASE_URL,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceServerTime {
    pub server_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceExchangeInfo {
    pub timezone: Option<String>,
    pub server_time: Option<u64>,
    pub futures_type: Option<String>,
    #[serde(default)]
    pub rate_limits: Vec<Value>,
    #[serde(default)]
    pub exchange_filters: Vec<Value>,
    #[serde(default)]
    pub assets: Vec<Value>,
    #[serde(default)]
    pub symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceSymbolInfo {
    pub symbol: String,
    pub status: Option<String>,
    pub base_asset: Option<String>,
    pub quote_asset: Option<String>,
    pub contract_type: Option<String>,
    pub delivery_date: Option<u64>,
    pub onboard_date: Option<u64>,
    #[serde(default)]
    pub order_types: Vec<String>,
    #[serde(default)]
    pub filters: Vec<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl BinanceSymbolInfo {
    pub fn is_trading(&self) -> bool {
        self.status.as_deref() == Some("TRADING")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOrderBook {
    pub last_update_id: u64,
    pub e: Option<u64>,
    pub t: Option<u64>,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceTrade {
    pub id: u64,
    pub price: String,
    pub qty: String,
    pub quote_qty: Option<String>,
    pub time: u64,
    pub is_buyer_maker: bool,
    pub is_best_match: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceAggTrade {
    #[serde(rename = "a")]
    pub aggregate_trade_id: u64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
    #[serde(rename = "f")]
    pub first_trade_id: Option<u64>,
    #[serde(rename = "l")]
    pub last_trade_id: Option<u64>,
    #[serde(rename = "T")]
    pub trade_time: u64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
    #[serde(rename = "M")]
    pub is_best_match: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    pub fn len(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(items) => items.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone)]
pub struct BinancePublicRestClient {
    http: Client,
    spot_base_url: String,
    futures_base_url: String,
    futures_data_base_url: String,
}

impl Default for BinancePublicRestClient {
    fn default() -> Self {
        Self {
            http: Client::builder()
                .user_agent("rust-market-data/binance")
                .build()
                .expect("reqwest client"),
            spot_base_url: SPOT_REST_BASE_URL.to_string(),
            futures_base_url: FUTURES_REST_BASE_URL.to_string(),
            futures_data_base_url: FUTURES_DATA_BASE_URL.to_string(),
        }
    }
}

impl BinancePublicRestClient {
    pub fn new(
        spot_base_url: impl Into<String>,
        futures_base_url: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            http: Client::builder()
                .user_agent("rust-market-data/binance")
                .build()
                .context("build reqwest client")?,
            spot_base_url: spot_base_url.into(),
            futures_base_url: futures_base_url.into(),
            futures_data_base_url: FUTURES_DATA_BASE_URL.to_string(),
        })
    }

    pub fn with_futures_data_base_url(mut self, futures_data_base_url: impl Into<String>) -> Self {
        self.futures_data_base_url = futures_data_base_url.into();
        self
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let url = format!("{base_url}{path}");
        let body = self
            .http
            .get(url)
            .query(query)
            .send()
            .await
            .context("send GET request")?
            .error_for_status()
            .context("unexpected HTTP status")?
            .text()
            .await
            .context("read response body")?;

        serde_json::from_str::<T>(&body).with_context(|| {
            let snippet = body.chars().take(300).collect::<String>();
            format!("decode JSON response body: {snippet}")
        })
    }

    async fn get_one_or_many<T: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<OneOrMany<T>> {
        let value: Value = self.get_json(base_url, path, query).await?;
        match value {
            Value::Array(_) => serde_json::from_value::<Vec<T>>(value)
                .map(OneOrMany::Many)
                .context("decode JSON response body as array"),
            Value::Object(_) => serde_json::from_value::<T>(value)
                .map(OneOrMany::One)
                .context("decode JSON response body as object"),
            other => Err(anyhow!("expected object or array response, got {other}")),
        }
    }

    async fn get_spot<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        self.get_json(&self.spot_base_url, path, query).await
    }

    async fn get_futures<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        self.get_json(&self.futures_base_url, path, query).await
    }

    async fn get_futures_data<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        self.get_json(&self.futures_data_base_url, path, query)
            .await
    }

    pub async fn spot_ping(&self) -> Result<()> {
        let _: Value = self.get_spot("/api/v3/ping", &[]).await?;
        Ok(())
    }

    pub async fn spot_server_time(&self) -> Result<BinanceServerTime> {
        self.get_spot("/api/v3/time", &[]).await
    }

    pub async fn spot_exchange_info(&self, symbol: Option<&str>) -> Result<BinanceExchangeInfo> {
        let query = optional_symbol_query(symbol);
        self.get_spot("/api/v3/exchangeInfo", &query).await
    }

    pub async fn spot_order_book(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<BinanceOrderBook> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_spot("/api/v3/depth", &query).await
    }

    pub async fn spot_recent_trades(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<Vec<BinanceTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_spot("/api/v3/trades", &query).await
    }

    pub async fn spot_historical_trades(
        &self,
        symbol: &str,
        limit: Option<u16>,
        from_id: Option<u64>,
    ) -> Result<Vec<BinanceTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(from_id) = from_id {
            query.push(("fromId", from_id.to_string()));
        }
        self.get_spot("/api/v3/historicalTrades", &query).await
    }

    pub async fn spot_historical_block_trades(
        &self,
        symbol: &str,
        from_id: u64,
        limit: Option<u16>,
    ) -> Result<Vec<Value>> {
        let mut query = vec![
            ("symbol", symbol.to_string()),
            ("fromId", from_id.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_spot("/api/v3/historicalBlockTrades", &query).await
    }

    pub async fn spot_aggregate_trades(
        &self,
        symbol: &str,
        limit: Option<u16>,
        from_id: Option<u64>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<BinanceAggTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(from_id) = from_id {
            query.push(("fromId", from_id.to_string()));
        }
        push_time_range(&mut query, start_time, end_time);
        self.get_spot("/api/v3/aggTrades", &query).await
    }

    pub async fn spot_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Vec<Value>>> {
        let query = kline_query(symbol, interval, limit, start_time, end_time);
        self.get_spot("/api/v3/klines", &query).await
    }

    pub async fn spot_ui_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Vec<Value>>> {
        let query = kline_query(symbol, interval, limit, start_time, end_time);
        self.get_spot("/api/v3/uiKlines", &query).await
    }

    pub async fn spot_average_price(&self, symbol: &str) -> Result<Value> {
        self.get_spot("/api/v3/avgPrice", &[("symbol", symbol.to_string())])
            .await
    }

    pub async fn spot_ticker_24hr(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.spot_base_url, "/api/v3/ticker/24hr", &query)
            .await
    }

    pub async fn spot_trading_day_ticker(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.spot_base_url, "/api/v3/ticker/tradingDay", &query)
            .await
    }

    pub async fn spot_price_ticker(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.spot_base_url, "/api/v3/ticker/price", &query)
            .await
    }

    pub async fn spot_book_ticker(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.spot_base_url, "/api/v3/ticker/bookTicker", &query)
            .await
    }

    pub async fn spot_rolling_window_ticker(
        &self,
        symbol: Option<&str>,
        window_size: Option<&str>,
    ) -> Result<OneOrMany<Value>> {
        let mut query = optional_symbol_query(symbol);
        if let Some(window_size) = window_size {
            query.push(("windowSize", window_size.to_string()));
        }
        self.get_one_or_many(&self.spot_base_url, "/api/v3/ticker", &query)
            .await
    }

    pub async fn spot_reference_price(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.spot_base_url, "/api/v3/referencePrice", &query)
            .await
    }

    pub async fn spot_reference_price_calculation(&self, symbol: &str) -> Result<Value> {
        self.get_spot(
            "/api/v3/referencePrice/calculation",
            &[("symbol", symbol.to_string())],
        )
        .await
    }

    pub async fn futures_ping(&self) -> Result<()> {
        let _: Value = self.get_futures("/fapi/v1/ping", &[]).await?;
        Ok(())
    }

    pub async fn futures_server_time(&self) -> Result<BinanceServerTime> {
        self.get_futures("/fapi/v1/time", &[]).await
    }

    pub async fn futures_exchange_info(&self, symbol: Option<&str>) -> Result<BinanceExchangeInfo> {
        let query = optional_symbol_query(symbol);
        self.get_futures("/fapi/v1/exchangeInfo", &query).await
    }

    pub async fn futures_order_book(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<BinanceOrderBook> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_futures("/fapi/v1/depth", &query).await
    }

    pub async fn futures_recent_trades(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<Vec<BinanceTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_futures("/fapi/v1/trades", &query).await
    }

    pub async fn futures_historical_trades(
        &self,
        symbol: &str,
        limit: Option<u16>,
        from_id: Option<u64>,
    ) -> Result<Vec<BinanceTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(from_id) = from_id {
            query.push(("fromId", from_id.to_string()));
        }
        self.get_futures("/fapi/v1/historicalTrades", &query).await
    }

    pub async fn futures_aggregate_trades(
        &self,
        symbol: &str,
        limit: Option<u16>,
        from_id: Option<u64>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<BinanceAggTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(from_id) = from_id {
            query.push(("fromId", from_id.to_string()));
        }
        push_time_range(&mut query, start_time, end_time);
        self.get_futures("/fapi/v1/aggTrades", &query).await
    }

    pub async fn futures_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Vec<Value>>> {
        let query = kline_query(symbol, interval, limit, start_time, end_time);
        self.get_futures("/fapi/v1/klines", &query).await
    }

    pub async fn futures_continuous_klines(
        &self,
        pair: &str,
        contract_type: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Vec<Value>>> {
        let mut query = vec![
            ("pair", pair.to_string()),
            ("contractType", contract_type.to_string()),
            ("interval", interval.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        push_time_range(&mut query, start_time, end_time);
        self.get_futures("/fapi/v1/continuousKlines", &query).await
    }

    pub async fn futures_index_price_klines(
        &self,
        pair: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Vec<Value>>> {
        let mut query = vec![
            ("pair", pair.to_string()),
            ("interval", interval.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        push_time_range(&mut query, start_time, end_time);
        self.get_futures("/fapi/v1/indexPriceKlines", &query).await
    }

    pub async fn futures_mark_price_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Vec<Value>>> {
        let query = kline_query(symbol, interval, limit, start_time, end_time);
        self.get_futures("/fapi/v1/markPriceKlines", &query).await
    }

    pub async fn futures_premium_index_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Vec<Value>>> {
        let query = kline_query(symbol, interval, limit, start_time, end_time);
        self.get_futures("/fapi/v1/premiumIndexKlines", &query)
            .await
    }

    pub async fn futures_premium_index(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v1/premiumIndex", &query)
            .await
    }

    pub async fn futures_funding_rate(
        &self,
        symbol: Option<&str>,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Value>> {
        let mut query = optional_symbol_query(symbol);
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        push_time_range(&mut query, start_time, end_time);
        self.get_futures("/fapi/v1/fundingRate", &query).await
    }

    pub async fn futures_funding_info(&self) -> Result<Vec<Value>> {
        self.get_futures("/fapi/v1/fundingInfo", &[]).await
    }

    pub async fn futures_ticker_24hr(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v1/ticker/24hr", &query)
            .await
    }

    pub async fn futures_price_ticker(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v1/ticker/price", &query)
            .await
    }

    pub async fn futures_price_ticker_v2(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v2/ticker/price", &query)
            .await
    }

    pub async fn futures_book_ticker(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v1/ticker/bookTicker", &query)
            .await
    }

    pub async fn futures_open_interest(&self, symbol: &str) -> Result<Value> {
        self.get_futures("/fapi/v1/openInterest", &[("symbol", symbol.to_string())])
            .await
    }

    pub async fn futures_index_info(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v1/indexInfo", &query)
            .await
    }

    pub async fn futures_asset_index(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v1/assetIndex", &query)
            .await
    }

    pub async fn futures_constituents(&self, symbol: &str) -> Result<Value> {
        self.get_futures("/fapi/v1/constituents", &[("symbol", symbol.to_string())])
            .await
    }

    pub async fn futures_symbol_adl_risk(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v1/symbolAdlRisk", &query)
            .await
    }

    pub async fn futures_open_interest_hist(
        &self,
        symbol: &str,
        period: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Value>> {
        let query = futures_data_period_query(symbol, period, limit, start_time, end_time);
        self.get_futures_data("/futures/data/openInterestHist", &query)
            .await
    }

    pub async fn futures_top_long_short_account_ratio(
        &self,
        symbol: &str,
        period: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Value>> {
        let query = futures_data_period_query(symbol, period, limit, start_time, end_time);
        self.get_futures_data("/futures/data/topLongShortAccountRatio", &query)
            .await
    }

    pub async fn futures_top_long_short_position_ratio(
        &self,
        symbol: &str,
        period: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Value>> {
        let query = futures_data_period_query(symbol, period, limit, start_time, end_time);
        self.get_futures_data("/futures/data/topLongShortPositionRatio", &query)
            .await
    }

    pub async fn futures_global_long_short_account_ratio(
        &self,
        symbol: &str,
        period: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Value>> {
        let query = futures_data_period_query(symbol, period, limit, start_time, end_time);
        self.get_futures_data("/futures/data/globalLongShortAccountRatio", &query)
            .await
    }

    pub async fn futures_taker_long_short_ratio(
        &self,
        symbol: &str,
        period: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Value>> {
        let query = futures_data_period_query(symbol, period, limit, start_time, end_time);
        self.get_futures_data("/futures/data/takerlongshortRatio", &query)
            .await
    }

    pub async fn futures_basis(
        &self,
        pair: &str,
        contract_type: &str,
        period: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<Value>> {
        let mut query = vec![
            ("pair", pair.to_string()),
            ("contractType", contract_type.to_string()),
            ("period", period.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        push_time_range(&mut query, start_time, end_time);
        self.get_futures_data("/futures/data/basis", &query).await
    }

    pub async fn spot_symbols(&self) -> Result<Vec<String>> {
        let mut symbols = self
            .spot_exchange_info(None)
            .await?
            .symbols
            .into_iter()
            .map(|symbol| symbol.symbol)
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        Ok(symbols)
    }

    pub async fn spot_trading_symbols(&self) -> Result<Vec<String>> {
        let mut symbols = self
            .spot_exchange_info(None)
            .await?
            .symbols
            .into_iter()
            .filter(BinanceSymbolInfo::is_trading)
            .map(|symbol| symbol.symbol)
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        Ok(symbols)
    }

    pub async fn futures_symbols(&self) -> Result<Vec<String>> {
        let mut symbols = self
            .futures_exchange_info(None)
            .await?
            .symbols
            .into_iter()
            .map(|symbol| symbol.symbol)
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        Ok(symbols)
    }

    pub async fn futures_trading_symbols(&self) -> Result<Vec<String>> {
        let mut symbols = self
            .futures_exchange_info(None)
            .await?
            .symbols
            .into_iter()
            .filter(BinanceSymbolInfo::is_trading)
            .map(|symbol| symbol.symbol)
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        Ok(symbols)
    }

    pub async fn all_public_symbols(&self) -> Result<(Vec<String>, Vec<String>)> {
        Ok((self.spot_symbols().await?, self.futures_symbols().await?))
    }

    pub async fn all_trading_symbols(&self) -> Result<(Vec<String>, Vec<String>)> {
        Ok((
            self.spot_trading_symbols().await?,
            self.futures_trading_symbols().await?,
        ))
    }
}

fn optional_symbol_query(symbol: Option<&str>) -> Vec<(&'static str, String)> {
    symbol
        .map(|symbol| vec![("symbol", symbol.to_string())])
        .unwrap_or_default()
}

fn kline_query(
    symbol: &str,
    interval: &str,
    limit: Option<u16>,
    start_time: Option<u64>,
    end_time: Option<u64>,
) -> Vec<(&'static str, String)> {
    let mut query = vec![
        ("symbol", symbol.to_string()),
        ("interval", interval.to_string()),
    ];
    if let Some(limit) = limit {
        query.push(("limit", limit.to_string()));
    }
    push_time_range(&mut query, start_time, end_time);
    query
}

fn futures_data_period_query(
    symbol: &str,
    period: &str,
    limit: Option<u16>,
    start_time: Option<u64>,
    end_time: Option<u64>,
) -> Vec<(&'static str, String)> {
    let mut query = vec![
        ("symbol", symbol.to_string()),
        ("period", period.to_string()),
    ];
    if let Some(limit) = limit {
        query.push(("limit", limit.to_string()));
    }
    push_time_range(&mut query, start_time, end_time);
    query
}

fn push_time_range(
    query: &mut Vec<(&'static str, String)>,
    start_time: Option<u64>,
    end_time: Option<u64>,
) {
    if let Some(start_time) = start_time {
        query.push(("startTime", start_time.to_string()));
    }
    if let Some(end_time) = end_time {
        query.push(("endTime", end_time.to_string()));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinanceWsSubscription {
    AggTrade {
        symbol: String,
    },
    Trade {
        symbol: String,
    },
    BlockTrade {
        symbol: String,
    },
    Kline {
        symbol: String,
        interval: String,
    },
    MiniTicker {
        symbol: String,
    },
    AllMiniTickers,
    Ticker {
        symbol: String,
    },
    RollingWindowTicker {
        symbol: String,
        window_size: String,
    },
    AllTickers,
    AllRollingWindowTickers {
        window_size: String,
    },
    BookTicker {
        symbol: String,
    },
    AllBookTickers,
    AveragePrice {
        symbol: String,
    },
    ReferencePrice {
        symbol: String,
    },
    MarkPrice {
        symbol: String,
        fast: bool,
    },
    AllMarkPrices {
        fast: bool,
    },
    Liquidation {
        symbol: String,
    },
    AllLiquidations,
    PartialDepth {
        symbol: String,
        levels: u16,
        speed_ms: Option<u16>,
    },
    DiffDepth {
        symbol: String,
        speed_ms: Option<u16>,
    },
}

impl BinanceWsSubscription {
    pub fn stream_name(&self) -> String {
        match self {
            Self::AggTrade { symbol } => format!("{}@aggTrade", stream_symbol(symbol)),
            Self::Trade { symbol } => format!("{}@trade", stream_symbol(symbol)),
            Self::BlockTrade { symbol } => format!("{}@blockTrade", stream_symbol(symbol)),
            Self::Kline { symbol, interval } => {
                format!("{}@kline_{interval}", stream_symbol(symbol))
            }
            Self::MiniTicker { symbol } => format!("{}@miniTicker", stream_symbol(symbol)),
            Self::AllMiniTickers => "!miniTicker@arr".to_string(),
            Self::Ticker { symbol } => format!("{}@ticker", stream_symbol(symbol)),
            Self::RollingWindowTicker {
                symbol,
                window_size,
            } => {
                format!("{}@ticker_{window_size}", stream_symbol(symbol))
            }
            Self::AllTickers => "!ticker@arr".to_string(),
            Self::AllRollingWindowTickers { window_size } => {
                format!("!ticker_{window_size}@arr")
            }
            Self::BookTicker { symbol } => format!("{}@bookTicker", stream_symbol(symbol)),
            Self::AllBookTickers => "!bookTicker".to_string(),
            Self::AveragePrice { symbol } => format!("{}@avgPrice", stream_symbol(symbol)),
            Self::ReferencePrice { symbol } => {
                format!("{}@referencePrice", stream_symbol(symbol))
            }
            Self::MarkPrice { symbol, fast } => {
                let suffix = if *fast { "@1s" } else { "" };
                format!("{}@markPrice{suffix}", stream_symbol(symbol))
            }
            Self::AllMarkPrices { fast } => {
                let suffix = if *fast { "@1s" } else { "" };
                format!("!markPrice@arr{suffix}")
            }
            Self::Liquidation { symbol } => format!("{}@forceOrder", stream_symbol(symbol)),
            Self::AllLiquidations => "!forceOrder@arr".to_string(),
            Self::PartialDepth {
                symbol,
                levels,
                speed_ms,
            } => format!(
                "{}@depth{levels}{}",
                stream_symbol(symbol),
                speed_suffix(*speed_ms)
            ),
            Self::DiffDepth { symbol, speed_ms } => {
                format!("{}@depth{}", stream_symbol(symbol), speed_suffix(*speed_ms))
            }
        }
    }

    pub fn symbol(&self) -> Option<&str> {
        match self {
            Self::AggTrade { symbol }
            | Self::Trade { symbol }
            | Self::BlockTrade { symbol }
            | Self::Kline { symbol, .. }
            | Self::MiniTicker { symbol }
            | Self::Ticker { symbol }
            | Self::RollingWindowTicker { symbol, .. }
            | Self::BookTicker { symbol }
            | Self::AveragePrice { symbol }
            | Self::ReferencePrice { symbol }
            | Self::MarkPrice { symbol, .. }
            | Self::Liquidation { symbol }
            | Self::PartialDepth { symbol, .. }
            | Self::DiffDepth { symbol, .. } => Some(symbol),
            Self::AllMiniTickers
            | Self::AllTickers
            | Self::AllRollingWindowTickers { .. }
            | Self::AllBookTickers
            | Self::AllMarkPrices { .. }
            | Self::AllLiquidations => None,
        }
    }
}

fn stream_symbol(symbol: &str) -> String {
    symbol.to_ascii_lowercase()
}

fn speed_suffix(speed_ms: Option<u16>) -> String {
    speed_ms
        .map(|speed_ms| format!("@{speed_ms}ms"))
        .unwrap_or_default()
}

#[derive(Clone)]
pub struct BinanceWsClient {
    market: BinanceMarket,
    ws_base_url: String,
}

impl BinanceWsClient {
    pub fn spot() -> Self {
        Self::new(BinanceMarket::Spot, SPOT_WS_BASE_URL)
    }

    pub fn futures() -> Self {
        Self::new(BinanceMarket::UsdmFutures, FUTURES_WS_BASE_URL)
    }

    pub fn new(market: BinanceMarket, ws_base_url: impl Into<String>) -> Self {
        Self {
            market,
            ws_base_url: ws_base_url.into(),
        }
    }

    pub fn market(&self) -> BinanceMarket {
        self.market
    }

    pub fn websocket_endpoint(&self) -> String {
        format!("{}/ws", self.ws_base_url)
    }

    pub fn combined_stream_url(&self, subscriptions: &[BinanceWsSubscription]) -> String {
        let streams = subscriptions
            .iter()
            .map(BinanceWsSubscription::stream_name)
            .collect::<Vec<_>>()
            .join("/");
        format!("{}/stream?streams={streams}", self.ws_base_url)
    }

    pub async fn connect_streams(
        &self,
        subscriptions: Vec<BinanceWsSubscription>,
    ) -> Result<ReceiverStream<Result<Value>>> {
        let url = self.combined_stream_url(&subscriptions);
        let (ws_stream, _) = connect_async(&url)
            .await
            .with_context(|| format!("connect websocket stream {url}"))?;
        let (_write, mut read) = ws_stream.split();
        let (sender, receiver) = mpsc::channel(256);

        tokio::spawn(async move {
            while let Some(message) = read.next().await {
                let decoded = decode_ws_message(message);
                if sender.send(decoded).await.is_err() {
                    break;
                }
            }
        });

        Ok(ReceiverStream::new(receiver))
    }

    pub async fn connect_sharded(
        &self,
        subscriptions: Vec<BinanceWsSubscription>,
        max_streams_per_connection: usize,
    ) -> Result<Vec<ReceiverStream<Result<Value>>>> {
        if max_streams_per_connection == 0 {
            return Err(anyhow!(
                "max_streams_per_connection must be greater than zero"
            ));
        }

        let mut streams = Vec::new();
        for chunk in subscriptions.chunks(max_streams_per_connection) {
            streams.push(self.connect_streams(chunk.to_vec()).await?);
        }
        Ok(streams)
    }

    pub async fn subscribe(
        &self,
        subscriptions: Vec<BinanceWsSubscription>,
    ) -> Result<ReceiverStream<Result<Value>>> {
        let url = self.websocket_endpoint();
        let (ws_stream, _) = connect_async(&url)
            .await
            .with_context(|| format!("connect websocket endpoint {url}"))?;
        let (mut write, mut read) = ws_stream.split();
        let params = subscriptions
            .iter()
            .map(BinanceWsSubscription::stream_name)
            .collect::<Vec<_>>();
        write
            .send(Message::Text(
                json!({
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": 1
                })
                .to_string(),
            ))
            .await
            .context("send subscribe request")?;
        let (sender, receiver) = mpsc::channel(256);

        tokio::spawn(async move {
            while let Some(message) = read.next().await {
                let decoded = decode_ws_message(message);
                if sender.send(decoded).await.is_err() {
                    break;
                }
            }
        });

        Ok(ReceiverStream::new(receiver))
    }
}

fn decode_ws_message(
    message: Result<Message, tokio_tungstenite::tungstenite::Error>,
) -> Result<Value> {
    match message.context("read websocket message")? {
        Message::Text(text) => serde_json::from_str(&text).context("decode websocket text JSON"),
        Message::Binary(bytes) => {
            serde_json::from_slice(&bytes).context("decode websocket binary JSON")
        }
        Message::Ping(_) | Message::Pong(_) => Ok(json!({"event": "control"})),
        Message::Close(frame) => Ok(json!({"event": "close", "frame": format!("{frame:?}")})),
        Message::Frame(_) => Ok(json!({"event": "frame"})),
    }
}

#[derive(Debug, Clone)]
pub struct BinanceSpotCoverageConfig {
    pub include_agg_trade: bool,
    pub include_trade: bool,
    pub include_block_trade: bool,
    pub kline_intervals: Vec<String>,
    pub include_mini_ticker: bool,
    pub include_all_mini_tickers: bool,
    pub include_ticker: bool,
    pub include_all_tickers: bool,
    pub rolling_windows: Vec<String>,
    pub include_book_ticker: bool,
    pub include_all_book_tickers: bool,
    pub include_avg_price: bool,
    pub include_reference_price: bool,
    pub partial_depth_levels: Vec<u16>,
    pub partial_depth_speeds_ms: Vec<Option<u16>>,
    pub diff_depth_speeds_ms: Vec<Option<u16>>,
}

impl BinanceSpotCoverageConfig {
    pub fn balanced() -> Self {
        Self {
            include_agg_trade: true,
            include_trade: false,
            include_block_trade: false,
            kline_intervals: vec!["1m".to_string()],
            include_mini_ticker: false,
            include_all_mini_tickers: true,
            include_ticker: true,
            include_all_tickers: true,
            rolling_windows: Vec::new(),
            include_book_ticker: true,
            include_all_book_tickers: true,
            include_avg_price: true,
            include_reference_price: true,
            partial_depth_levels: vec![5],
            partial_depth_speeds_ms: vec![Some(100)],
            diff_depth_speeds_ms: vec![Some(100)],
        }
    }

    pub fn exhaustive() -> Self {
        Self {
            include_agg_trade: true,
            include_trade: true,
            include_block_trade: true,
            kline_intervals: BINANCE_KLINE_INTERVALS
                .iter()
                .map(|interval| (*interval).to_string())
                .collect(),
            include_mini_ticker: true,
            include_all_mini_tickers: true,
            include_ticker: true,
            include_all_tickers: true,
            rolling_windows: vec!["1h".to_string(), "4h".to_string(), "1d".to_string()],
            include_book_ticker: true,
            include_all_book_tickers: true,
            include_avg_price: true,
            include_reference_price: true,
            partial_depth_levels: vec![5, 10, 20],
            partial_depth_speeds_ms: vec![None, Some(100)],
            diff_depth_speeds_ms: vec![None, Some(100)],
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinanceFuturesCoverageConfig {
    pub include_agg_trade: bool,
    pub include_mark_price: bool,
    pub include_fast_mark_price: bool,
    pub include_all_mark_prices: bool,
    pub include_fast_all_mark_prices: bool,
    pub kline_intervals: Vec<String>,
    pub include_ticker: bool,
    pub include_all_tickers: bool,
    pub include_book_ticker: bool,
    pub include_all_book_tickers: bool,
    pub include_liquidation: bool,
    pub include_all_liquidations: bool,
    pub partial_depth_levels: Vec<u16>,
    pub partial_depth_speeds_ms: Vec<Option<u16>>,
    pub diff_depth_speeds_ms: Vec<Option<u16>>,
}

impl BinanceFuturesCoverageConfig {
    pub fn balanced() -> Self {
        Self {
            include_agg_trade: true,
            include_mark_price: false,
            include_fast_mark_price: true,
            include_all_mark_prices: false,
            include_fast_all_mark_prices: true,
            kline_intervals: vec!["1m".to_string()],
            include_ticker: true,
            include_all_tickers: true,
            include_book_ticker: true,
            include_all_book_tickers: true,
            include_liquidation: true,
            include_all_liquidations: true,
            partial_depth_levels: vec![5],
            partial_depth_speeds_ms: vec![Some(100)],
            diff_depth_speeds_ms: vec![Some(100)],
        }
    }

    pub fn exhaustive() -> Self {
        Self {
            include_agg_trade: true,
            include_mark_price: true,
            include_fast_mark_price: true,
            include_all_mark_prices: true,
            include_fast_all_mark_prices: true,
            kline_intervals: BINANCE_KLINE_INTERVALS
                .iter()
                .filter(|interval| **interval != "1s")
                .map(|interval| (*interval).to_string())
                .collect(),
            include_ticker: true,
            include_all_tickers: true,
            include_book_ticker: true,
            include_all_book_tickers: true,
            include_liquidation: true,
            include_all_liquidations: true,
            partial_depth_levels: vec![5, 10, 20],
            partial_depth_speeds_ms: vec![None, Some(500), Some(100)],
            diff_depth_speeds_ms: vec![None, Some(500), Some(100)],
        }
    }
}

pub fn build_spot_public_subscriptions(
    symbols: &[String],
    config: &BinanceSpotCoverageConfig,
) -> Vec<BinanceWsSubscription> {
    let mut subscriptions = Vec::new();

    for symbol in symbols {
        if config.include_agg_trade {
            subscriptions.push(BinanceWsSubscription::AggTrade {
                symbol: symbol.clone(),
            });
        }
        if config.include_trade {
            subscriptions.push(BinanceWsSubscription::Trade {
                symbol: symbol.clone(),
            });
        }
        if config.include_block_trade {
            subscriptions.push(BinanceWsSubscription::BlockTrade {
                symbol: symbol.clone(),
            });
        }
        for interval in &config.kline_intervals {
            subscriptions.push(BinanceWsSubscription::Kline {
                symbol: symbol.clone(),
                interval: interval.clone(),
            });
        }
        if config.include_mini_ticker {
            subscriptions.push(BinanceWsSubscription::MiniTicker {
                symbol: symbol.clone(),
            });
        }
        if config.include_ticker {
            subscriptions.push(BinanceWsSubscription::Ticker {
                symbol: symbol.clone(),
            });
        }
        for window_size in &config.rolling_windows {
            subscriptions.push(BinanceWsSubscription::RollingWindowTicker {
                symbol: symbol.clone(),
                window_size: window_size.clone(),
            });
        }
        if config.include_book_ticker {
            subscriptions.push(BinanceWsSubscription::BookTicker {
                symbol: symbol.clone(),
            });
        }
        if config.include_avg_price {
            subscriptions.push(BinanceWsSubscription::AveragePrice {
                symbol: symbol.clone(),
            });
        }
        if config.include_reference_price {
            subscriptions.push(BinanceWsSubscription::ReferencePrice {
                symbol: symbol.clone(),
            });
        }
        for level in &config.partial_depth_levels {
            for speed_ms in &config.partial_depth_speeds_ms {
                subscriptions.push(BinanceWsSubscription::PartialDepth {
                    symbol: symbol.clone(),
                    levels: *level,
                    speed_ms: *speed_ms,
                });
            }
        }
        for speed_ms in &config.diff_depth_speeds_ms {
            subscriptions.push(BinanceWsSubscription::DiffDepth {
                symbol: symbol.clone(),
                speed_ms: *speed_ms,
            });
        }
    }

    if config.include_all_mini_tickers {
        subscriptions.push(BinanceWsSubscription::AllMiniTickers);
    }
    if config.include_all_tickers {
        subscriptions.push(BinanceWsSubscription::AllTickers);
    }
    for window_size in &config.rolling_windows {
        subscriptions.push(BinanceWsSubscription::AllRollingWindowTickers {
            window_size: window_size.clone(),
        });
    }
    if config.include_all_book_tickers {
        subscriptions.push(BinanceWsSubscription::AllBookTickers);
    }

    subscriptions
}

pub fn build_futures_public_subscriptions(
    symbols: &[String],
    config: &BinanceFuturesCoverageConfig,
) -> Vec<BinanceWsSubscription> {
    let mut subscriptions = Vec::new();

    for symbol in symbols {
        if config.include_agg_trade {
            subscriptions.push(BinanceWsSubscription::AggTrade {
                symbol: symbol.clone(),
            });
        }
        if config.include_mark_price {
            subscriptions.push(BinanceWsSubscription::MarkPrice {
                symbol: symbol.clone(),
                fast: false,
            });
        }
        if config.include_fast_mark_price {
            subscriptions.push(BinanceWsSubscription::MarkPrice {
                symbol: symbol.clone(),
                fast: true,
            });
        }
        for interval in &config.kline_intervals {
            subscriptions.push(BinanceWsSubscription::Kline {
                symbol: symbol.clone(),
                interval: interval.clone(),
            });
        }
        if config.include_ticker {
            subscriptions.push(BinanceWsSubscription::Ticker {
                symbol: symbol.clone(),
            });
        }
        if config.include_book_ticker {
            subscriptions.push(BinanceWsSubscription::BookTicker {
                symbol: symbol.clone(),
            });
        }
        if config.include_liquidation {
            subscriptions.push(BinanceWsSubscription::Liquidation {
                symbol: symbol.clone(),
            });
        }
        for level in &config.partial_depth_levels {
            for speed_ms in &config.partial_depth_speeds_ms {
                subscriptions.push(BinanceWsSubscription::PartialDepth {
                    symbol: symbol.clone(),
                    levels: *level,
                    speed_ms: *speed_ms,
                });
            }
        }
        for speed_ms in &config.diff_depth_speeds_ms {
            subscriptions.push(BinanceWsSubscription::DiffDepth {
                symbol: symbol.clone(),
                speed_ms: *speed_ms,
            });
        }
    }

    if config.include_all_mark_prices {
        subscriptions.push(BinanceWsSubscription::AllMarkPrices { fast: false });
    }
    if config.include_fast_all_mark_prices {
        subscriptions.push(BinanceWsSubscription::AllMarkPrices { fast: true });
    }
    if config.include_all_tickers {
        subscriptions.push(BinanceWsSubscription::AllTickers);
    }
    if config.include_all_book_tickers {
        subscriptions.push(BinanceWsSubscription::AllBookTickers);
    }
    if config.include_all_liquidations {
        subscriptions.push(BinanceWsSubscription::AllLiquidations);
    }

    subscriptions
}

pub async fn build_full_public_subscription_sets(
    rest: &BinancePublicRestClient,
    spot_config: &BinanceSpotCoverageConfig,
    futures_config: &BinanceFuturesCoverageConfig,
) -> Result<(Vec<BinanceWsSubscription>, Vec<BinanceWsSubscription>)> {
    let (spot_symbols, futures_symbols) = rest.all_public_symbols().await?;
    Ok((
        build_spot_public_subscriptions(&spot_symbols, spot_config),
        build_futures_public_subscriptions(&futures_symbols, futures_config),
    ))
}

pub fn covered_symbols(subscriptions: &[BinanceWsSubscription]) -> BTreeSet<String> {
    subscriptions
        .iter()
        .filter_map(|subscription| subscription.symbol().map(ToOwned::to_owned))
        .collect()
}

pub struct BinanceConnector {
    pub rest: BinancePublicRestClient,
    pub spot_ws: BinanceWsClient,
    pub futures_ws: BinanceWsClient,
}

impl Default for BinanceConnector {
    fn default() -> Self {
        Self {
            rest: BinancePublicRestClient::default(),
            spot_ws: BinanceWsClient::spot(),
            futures_ws: BinanceWsClient::futures(),
        }
    }
}

impl MarketDataConnector for BinanceConnector {
    fn exchange(&self) -> &'static str {
        "binance"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://stream.binance.com:9443/ws"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| {
                let symbol = item.symbol.clone();
                match &item.channel {
                    MarketDataChannel::Trades => {
                        BinanceWsSubscription::AggTrade { symbol }.stream_name()
                    }
                    MarketDataChannel::OrderBook => BinanceWsSubscription::DiffDepth {
                        symbol,
                        speed_ms: Some(100),
                    }
                    .stream_name(),
                    MarketDataChannel::Ticker => {
                        BinanceWsSubscription::Ticker { symbol }.stream_name()
                    }
                    MarketDataChannel::Liquidations => {
                        BinanceWsSubscription::Liquidation { symbol }.stream_name()
                    }
                    MarketDataChannel::Funding => {
                        BinanceWsSubscription::MarkPrice { symbol, fast: true }.stream_name()
                    }
                    MarketDataChannel::Custom(channel) => channel.clone(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhaustive_spot_plan_covers_every_input_symbol() {
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let plan =
            build_spot_public_subscriptions(&symbols, &BinanceSpotCoverageConfig::exhaustive());
        assert_eq!(covered_symbols(&plan), symbols.into_iter().collect());
        assert!(plan.contains(&BinanceWsSubscription::ReferencePrice {
            symbol: "BTCUSDT".to_string(),
        }));
        assert!(plan.contains(&BinanceWsSubscription::AllBookTickers));
    }

    #[test]
    fn exhaustive_futures_plan_covers_mark_price_and_liquidations() {
        let symbols = vec!["BTCUSDT".to_string()];
        let plan = build_futures_public_subscriptions(
            &symbols,
            &BinanceFuturesCoverageConfig::exhaustive(),
        );
        assert_eq!(covered_symbols(&plan), symbols.into_iter().collect());
        assert!(plan.contains(&BinanceWsSubscription::MarkPrice {
            symbol: "BTCUSDT".to_string(),
            fast: true,
        }));
        assert!(plan.contains(&BinanceWsSubscription::AllMarkPrices { fast: true }));
        assert!(plan.contains(&BinanceWsSubscription::AllLiquidations));
    }

    #[test]
    fn stream_names_match_binance_docs() {
        assert_eq!(
            BinanceWsSubscription::AllMarkPrices { fast: true }.stream_name(),
            "!markPrice@arr@1s"
        );
        assert_eq!(
            BinanceWsSubscription::PartialDepth {
                symbol: "BTCUSDT".to_string(),
                levels: 20,
                speed_ms: Some(100),
            }
            .stream_name(),
            "btcusdt@depth20@100ms"
        );
        assert_eq!(
            BinanceWsSubscription::AllRollingWindowTickers {
                window_size: "1h".to_string()
            }
            .stream_name(),
            "!ticker_1h@arr"
        );
    }
}
