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

pub const SPOT_REST_BASE_URL: &str = "https://sapi.asterdex.com";
pub const FUTURES_REST_BASE_URL: &str = "https://fapi.asterdex.com";
pub const SPOT_WS_BASE_URL: &str = "wss://sstream.asterdex.com";
pub const FUTURES_WS_BASE_URL: &str = "wss://fstream.asterdex.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsterMarket {
    Spot,
    Futures,
}

impl AsterMarket {
    pub fn rest_base_url(self) -> &'static str {
        match self {
            Self::Spot => SPOT_REST_BASE_URL,
            Self::Futures => FUTURES_REST_BASE_URL,
        }
    }

    pub fn ws_base_url(self) -> &'static str {
        match self {
            Self::Spot => SPOT_WS_BASE_URL,
            Self::Futures => FUTURES_WS_BASE_URL,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterServerTime {
    pub server_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterExchangeInfo {
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
    pub symbols: Vec<AsterSymbolInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterSymbolInfo {
    pub symbol: String,
    pub status: Option<String>,
    pub base_asset: Option<String>,
    pub quote_asset: Option<String>,
    pub contract_type: Option<String>,
    pub delivery_date: Option<u64>,
    pub onboard_date: Option<u64>,
    pub price_precision: Option<u32>,
    pub quantity_precision: Option<u32>,
    #[serde(default)]
    pub filters: Vec<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl AsterSymbolInfo {
    pub fn is_trading(&self) -> bool {
        self.status.as_deref() == Some("TRADING")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterOrderBook {
    pub last_update_id: u64,
    pub e: Option<u64>,
    pub t: Option<u64>,
    pub symbol: Option<String>,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterTrade {
    pub id: u64,
    pub price: String,
    pub qty: String,
    pub quote_qty: Option<String>,
    pub time: u64,
    pub is_buyer_maker: bool,
    pub is_best_match: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsterAggTrade {
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
pub struct AsterPublicRestClient {
    http: Client,
    spot_base_url: String,
    futures_base_url: String,
}

impl Default for AsterPublicRestClient {
    fn default() -> Self {
        Self {
            http: Client::builder()
                .user_agent("rust-market-data/aster")
                .build()
                .expect("reqwest client"),
            spot_base_url: SPOT_REST_BASE_URL.to_string(),
            futures_base_url: FUTURES_REST_BASE_URL.to_string(),
        }
    }
}

impl AsterPublicRestClient {
    pub fn new(
        spot_base_url: impl Into<String>,
        futures_base_url: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            http: Client::builder()
                .user_agent("rust-market-data/aster")
                .build()
                .context("build reqwest client")?,
            spot_base_url: spot_base_url.into(),
            futures_base_url: futures_base_url.into(),
        })
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

    pub async fn spot_ping(&self) -> Result<()> {
        let _: Value = self.get_spot("/api/v3/ping", &[]).await?;
        Ok(())
    }

    pub async fn spot_server_time(&self) -> Result<AsterServerTime> {
        self.get_spot("/api/v3/time", &[]).await
    }

    pub async fn spot_exchange_info(&self, symbol: Option<&str>) -> Result<AsterExchangeInfo> {
        let query = optional_symbol_query(symbol);
        self.get_spot("/api/v3/exchangeInfo", &query).await
    }

    pub async fn spot_order_book(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<AsterOrderBook> {
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
    ) -> Result<Vec<AsterTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_spot("/api/v3/trades", &query).await
    }

    pub async fn spot_aggregate_trades(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<Vec<AsterAggTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
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

    pub async fn spot_ticker_24hr(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.spot_base_url, "/api/v3/ticker/24hr", &query)
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

    pub async fn futures_ping(&self) -> Result<()> {
        let _: Value = self.get_futures("/fapi/v3/ping", &[]).await?;
        Ok(())
    }

    pub async fn futures_server_time(&self) -> Result<AsterServerTime> {
        self.get_futures("/fapi/v3/time", &[]).await
    }

    pub async fn futures_exchange_info(&self, symbol: Option<&str>) -> Result<AsterExchangeInfo> {
        let query = optional_symbol_query(symbol);
        self.get_futures("/fapi/v3/exchangeInfo", &query).await
    }

    pub async fn futures_order_book(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<AsterOrderBook> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_futures("/fapi/v3/depth", &query).await
    }

    pub async fn futures_recent_trades(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<Vec<AsterTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_futures("/fapi/v3/trades", &query).await
    }

    pub async fn futures_aggregate_trades(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<Vec<AsterAggTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_futures("/fapi/v3/aggTrades", &query).await
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
        self.get_futures("/fapi/v3/klines", &query).await
    }

    pub async fn futures_premium_index(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v3/premiumIndex", &query)
            .await
    }

    pub async fn futures_funding_rate(
        &self,
        symbol: Option<&str>,
        limit: Option<u16>,
    ) -> Result<Vec<Value>> {
        let mut query = optional_symbol_query(symbol);
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_futures("/fapi/v3/fundingRate", &query).await
    }

    pub async fn futures_ticker_24hr(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v3/ticker/24hr", &query)
            .await
    }

    pub async fn futures_price_ticker(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v3/ticker/price", &query)
            .await
    }

    pub async fn futures_book_ticker(&self, symbol: Option<&str>) -> Result<OneOrMany<Value>> {
        let query = optional_symbol_query(symbol);
        self.get_one_or_many(&self.futures_base_url, "/fapi/v3/ticker/bookTicker", &query)
            .await
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
            .filter(AsterSymbolInfo::is_trading)
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
            .filter(AsterSymbolInfo::is_trading)
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
    if let Some(start_time) = start_time {
        query.push(("startTime", start_time.to_string()));
    }
    if let Some(end_time) = end_time {
        query.push(("endTime", end_time.to_string()));
    }
    query
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsterWsSubscription {
    AggTrade {
        symbol: String,
    },
    Trade {
        symbol: String,
    },
    MarkPrice {
        symbol: String,
        fast: bool,
    },
    AllMarkPrices {
        fast: bool,
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
    AllTickers,
    BookTicker {
        symbol: String,
    },
    AllBookTickers,
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

impl AsterWsSubscription {
    pub fn stream_name(&self) -> String {
        match self {
            Self::AggTrade { symbol } => format!("{}@aggTrade", stream_symbol(symbol)),
            Self::Trade { symbol } => format!("{}@trade", stream_symbol(symbol)),
            Self::MarkPrice { symbol, fast } => {
                let suffix = if *fast { "@1s" } else { "" };
                format!("{}@markPrice{suffix}", stream_symbol(symbol))
            }
            Self::AllMarkPrices { fast } => {
                let suffix = if *fast { "@1s" } else { "" };
                format!("!markPrice@arr{suffix}")
            }
            Self::Kline { symbol, interval } => {
                format!("{}@kline_{interval}", stream_symbol(symbol))
            }
            Self::MiniTicker { symbol } => format!("{}@miniTicker", stream_symbol(symbol)),
            Self::AllMiniTickers => "!miniTicker@arr".to_string(),
            Self::Ticker { symbol } => format!("{}@ticker", stream_symbol(symbol)),
            Self::AllTickers => "!ticker@arr".to_string(),
            Self::BookTicker { symbol } => format!("{}@bookTicker", stream_symbol(symbol)),
            Self::AllBookTickers => "!bookTicker".to_string(),
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
            | Self::MarkPrice { symbol, .. }
            | Self::Kline { symbol, .. }
            | Self::MiniTicker { symbol }
            | Self::Ticker { symbol }
            | Self::BookTicker { symbol }
            | Self::Liquidation { symbol }
            | Self::PartialDepth { symbol, .. }
            | Self::DiffDepth { symbol, .. } => Some(symbol),
            Self::AllMarkPrices { .. }
            | Self::AllMiniTickers
            | Self::AllTickers
            | Self::AllBookTickers
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
pub struct AsterWsClient {
    market: AsterMarket,
    ws_base_url: String,
}

impl AsterWsClient {
    pub fn spot() -> Self {
        Self::new(AsterMarket::Spot, SPOT_WS_BASE_URL)
    }

    pub fn futures() -> Self {
        Self::new(AsterMarket::Futures, FUTURES_WS_BASE_URL)
    }

    pub fn new(market: AsterMarket, ws_base_url: impl Into<String>) -> Self {
        Self {
            market,
            ws_base_url: ws_base_url.into(),
        }
    }

    pub fn market(&self) -> AsterMarket {
        self.market
    }

    pub fn base_url(&self) -> &str {
        &self.ws_base_url
    }

    pub fn websocket_endpoint(&self) -> String {
        format!("{}/ws", self.ws_base_url)
    }

    pub fn combined_stream_url(&self, subscriptions: &[AsterWsSubscription]) -> String {
        let streams = subscriptions
            .iter()
            .map(AsterWsSubscription::stream_name)
            .collect::<Vec<_>>()
            .join("/");
        format!("{}/stream?streams={streams}", self.ws_base_url)
    }

    pub fn single_stream_url(&self, subscription: &AsterWsSubscription) -> String {
        format!("{}/ws/{}", self.ws_base_url, subscription.stream_name())
    }

    pub async fn connect_streams(
        &self,
        subscriptions: Vec<AsterWsSubscription>,
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
        subscriptions: Vec<AsterWsSubscription>,
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
        subscriptions: Vec<AsterWsSubscription>,
    ) -> Result<ReceiverStream<Result<Value>>> {
        let url = self.websocket_endpoint();
        let (ws_stream, _) = connect_async(&url)
            .await
            .with_context(|| format!("connect websocket endpoint {url}"))?;
        let (mut write, mut read) = ws_stream.split();
        let params = subscriptions
            .iter()
            .map(AsterWsSubscription::stream_name)
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

pub const ASTER_KLINE_INTERVALS: &[&str] = &[
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M",
];

#[derive(Debug, Clone)]
pub struct AsterSpotCoverageConfig {
    pub include_agg_trade: bool,
    pub include_trade: bool,
    pub kline_intervals: Vec<String>,
    pub include_mini_ticker: bool,
    pub include_all_mini_tickers: bool,
    pub include_ticker: bool,
    pub include_all_tickers: bool,
    pub include_book_ticker: bool,
    pub include_all_book_tickers: bool,
    pub partial_depth_levels: Vec<u16>,
    pub partial_depth_speeds_ms: Vec<Option<u16>>,
    pub diff_depth_speeds_ms: Vec<Option<u16>>,
}

impl AsterSpotCoverageConfig {
    pub fn balanced() -> Self {
        Self {
            include_agg_trade: true,
            include_trade: false,
            kline_intervals: vec!["1m".to_string()],
            include_mini_ticker: false,
            include_all_mini_tickers: true,
            include_ticker: true,
            include_all_tickers: true,
            include_book_ticker: true,
            include_all_book_tickers: true,
            partial_depth_levels: vec![5],
            partial_depth_speeds_ms: vec![Some(100)],
            diff_depth_speeds_ms: vec![Some(100)],
        }
    }

    pub fn exhaustive() -> Self {
        Self {
            include_agg_trade: true,
            include_trade: true,
            kline_intervals: ASTER_KLINE_INTERVALS
                .iter()
                .map(|interval| (*interval).to_string())
                .collect(),
            include_mini_ticker: true,
            include_all_mini_tickers: true,
            include_ticker: true,
            include_all_tickers: true,
            include_book_ticker: true,
            include_all_book_tickers: true,
            partial_depth_levels: vec![5, 10, 20],
            partial_depth_speeds_ms: vec![None, Some(100)],
            diff_depth_speeds_ms: vec![None, Some(100)],
        }
    }
}

#[derive(Debug, Clone)]
pub struct AsterFuturesCoverageConfig {
    pub include_agg_trade: bool,
    pub include_mark_price: bool,
    pub include_fast_mark_price: bool,
    pub include_all_mark_prices: bool,
    pub include_fast_all_mark_prices: bool,
    pub kline_intervals: Vec<String>,
    pub include_mini_ticker: bool,
    pub include_all_mini_tickers: bool,
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

impl AsterFuturesCoverageConfig {
    pub fn balanced() -> Self {
        Self {
            include_agg_trade: true,
            include_mark_price: false,
            include_fast_mark_price: true,
            include_all_mark_prices: false,
            include_fast_all_mark_prices: true,
            kline_intervals: vec!["1m".to_string()],
            include_mini_ticker: false,
            include_all_mini_tickers: true,
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
            kline_intervals: ASTER_KLINE_INTERVALS
                .iter()
                .map(|interval| (*interval).to_string())
                .collect(),
            include_mini_ticker: true,
            include_all_mini_tickers: true,
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
    config: &AsterSpotCoverageConfig,
) -> Vec<AsterWsSubscription> {
    let mut subscriptions = Vec::new();

    for symbol in symbols {
        if config.include_agg_trade {
            subscriptions.push(AsterWsSubscription::AggTrade {
                symbol: symbol.clone(),
            });
        }
        if config.include_trade {
            subscriptions.push(AsterWsSubscription::Trade {
                symbol: symbol.clone(),
            });
        }
        for interval in &config.kline_intervals {
            subscriptions.push(AsterWsSubscription::Kline {
                symbol: symbol.clone(),
                interval: interval.clone(),
            });
        }
        if config.include_mini_ticker {
            subscriptions.push(AsterWsSubscription::MiniTicker {
                symbol: symbol.clone(),
            });
        }
        if config.include_ticker {
            subscriptions.push(AsterWsSubscription::Ticker {
                symbol: symbol.clone(),
            });
        }
        if config.include_book_ticker {
            subscriptions.push(AsterWsSubscription::BookTicker {
                symbol: symbol.clone(),
            });
        }
        for level in &config.partial_depth_levels {
            for speed_ms in &config.partial_depth_speeds_ms {
                subscriptions.push(AsterWsSubscription::PartialDepth {
                    symbol: symbol.clone(),
                    levels: *level,
                    speed_ms: *speed_ms,
                });
            }
        }
        for speed_ms in &config.diff_depth_speeds_ms {
            subscriptions.push(AsterWsSubscription::DiffDepth {
                symbol: symbol.clone(),
                speed_ms: *speed_ms,
            });
        }
    }

    if config.include_all_mini_tickers {
        subscriptions.push(AsterWsSubscription::AllMiniTickers);
    }
    if config.include_all_tickers {
        subscriptions.push(AsterWsSubscription::AllTickers);
    }
    if config.include_all_book_tickers {
        subscriptions.push(AsterWsSubscription::AllBookTickers);
    }

    subscriptions
}

pub fn build_futures_public_subscriptions(
    symbols: &[String],
    config: &AsterFuturesCoverageConfig,
) -> Vec<AsterWsSubscription> {
    let mut subscriptions = Vec::new();

    for symbol in symbols {
        if config.include_agg_trade {
            subscriptions.push(AsterWsSubscription::AggTrade {
                symbol: symbol.clone(),
            });
        }
        if config.include_mark_price {
            subscriptions.push(AsterWsSubscription::MarkPrice {
                symbol: symbol.clone(),
                fast: false,
            });
        }
        if config.include_fast_mark_price {
            subscriptions.push(AsterWsSubscription::MarkPrice {
                symbol: symbol.clone(),
                fast: true,
            });
        }
        for interval in &config.kline_intervals {
            subscriptions.push(AsterWsSubscription::Kline {
                symbol: symbol.clone(),
                interval: interval.clone(),
            });
        }
        if config.include_mini_ticker {
            subscriptions.push(AsterWsSubscription::MiniTicker {
                symbol: symbol.clone(),
            });
        }
        if config.include_ticker {
            subscriptions.push(AsterWsSubscription::Ticker {
                symbol: symbol.clone(),
            });
        }
        if config.include_book_ticker {
            subscriptions.push(AsterWsSubscription::BookTicker {
                symbol: symbol.clone(),
            });
        }
        if config.include_liquidation {
            subscriptions.push(AsterWsSubscription::Liquidation {
                symbol: symbol.clone(),
            });
        }
        for level in &config.partial_depth_levels {
            for speed_ms in &config.partial_depth_speeds_ms {
                subscriptions.push(AsterWsSubscription::PartialDepth {
                    symbol: symbol.clone(),
                    levels: *level,
                    speed_ms: *speed_ms,
                });
            }
        }
        for speed_ms in &config.diff_depth_speeds_ms {
            subscriptions.push(AsterWsSubscription::DiffDepth {
                symbol: symbol.clone(),
                speed_ms: *speed_ms,
            });
        }
    }

    if config.include_all_mark_prices {
        subscriptions.push(AsterWsSubscription::AllMarkPrices { fast: false });
    }
    if config.include_fast_all_mark_prices {
        subscriptions.push(AsterWsSubscription::AllMarkPrices { fast: true });
    }
    if config.include_all_mini_tickers {
        subscriptions.push(AsterWsSubscription::AllMiniTickers);
    }
    if config.include_all_tickers {
        subscriptions.push(AsterWsSubscription::AllTickers);
    }
    if config.include_all_book_tickers {
        subscriptions.push(AsterWsSubscription::AllBookTickers);
    }
    if config.include_all_liquidations {
        subscriptions.push(AsterWsSubscription::AllLiquidations);
    }

    subscriptions
}

pub async fn build_full_public_subscription_sets(
    rest: &AsterPublicRestClient,
    spot_config: &AsterSpotCoverageConfig,
    futures_config: &AsterFuturesCoverageConfig,
) -> Result<(Vec<AsterWsSubscription>, Vec<AsterWsSubscription>)> {
    let (spot_symbols, futures_symbols) = rest.all_public_symbols().await?;
    Ok((
        build_spot_public_subscriptions(&spot_symbols, spot_config),
        build_futures_public_subscriptions(&futures_symbols, futures_config),
    ))
}

pub fn covered_symbols(subscriptions: &[AsterWsSubscription]) -> BTreeSet<String> {
    subscriptions
        .iter()
        .filter_map(|subscription| subscription.symbol().map(ToOwned::to_owned))
        .collect()
}

pub struct AsterConnector {
    pub rest: AsterPublicRestClient,
    pub spot_ws: AsterWsClient,
    pub futures_ws: AsterWsClient,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhaustive_spot_plan_covers_every_input_symbol() {
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let plan =
            build_spot_public_subscriptions(&symbols, &AsterSpotCoverageConfig::exhaustive());
        let covered = covered_symbols(&plan);
        assert_eq!(covered, symbols.into_iter().collect());
        assert!(plan.contains(&AsterWsSubscription::Trade {
            symbol: "BTCUSDT".to_string(),
        }));
        assert!(plan.contains(&AsterWsSubscription::AllBookTickers));
    }

    #[test]
    fn exhaustive_futures_plan_covers_mark_price_and_liquidations() {
        let symbols = vec!["BTCUSDT".to_string()];
        let plan =
            build_futures_public_subscriptions(&symbols, &AsterFuturesCoverageConfig::exhaustive());
        let covered = covered_symbols(&plan);
        assert_eq!(covered, symbols.into_iter().collect());
        assert!(plan.contains(&AsterWsSubscription::MarkPrice {
            symbol: "BTCUSDT".to_string(),
            fast: true,
        }));
        assert!(plan.contains(&AsterWsSubscription::AllMarkPrices { fast: true }));
        assert!(plan.contains(&AsterWsSubscription::AllLiquidations));
    }

    #[test]
    fn stream_names_match_aster_docs() {
        assert_eq!(
            AsterWsSubscription::AllMarkPrices { fast: true }.stream_name(),
            "!markPrice@arr@1s"
        );
        assert_eq!(
            AsterWsSubscription::PartialDepth {
                symbol: "BTCUSDT".to_string(),
                levels: 20,
                speed_ms: Some(100),
            }
            .stream_name(),
            "btcusdt@depth20@100ms"
        );
    }
}

impl Default for AsterConnector {
    fn default() -> Self {
        Self {
            rest: AsterPublicRestClient::default(),
            spot_ws: AsterWsClient::spot(),
            futures_ws: AsterWsClient::futures(),
        }
    }
}

impl MarketDataConnector for AsterConnector {
    fn exchange(&self) -> &'static str {
        "aster"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://sstream.asterdex.com"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| {
                let symbol = item.symbol.clone();
                match &item.channel {
                    MarketDataChannel::Trades => {
                        AsterWsSubscription::AggTrade { symbol }.stream_name()
                    }
                    MarketDataChannel::OrderBook => AsterWsSubscription::DiffDepth {
                        symbol,
                        speed_ms: Some(100),
                    }
                    .stream_name(),
                    MarketDataChannel::Ticker => {
                        AsterWsSubscription::Ticker { symbol }.stream_name()
                    }
                    MarketDataChannel::Liquidations => {
                        AsterWsSubscription::Liquidation { symbol }.stream_name()
                    }
                    MarketDataChannel::Funding => {
                        AsterWsSubscription::MarkPrice { symbol, fast: true }.stream_name()
                    }
                    MarketDataChannel::Custom(channel) => channel.clone(),
                }
            })
            .collect()
    }
}
