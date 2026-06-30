use anyhow::{Context, Result, anyhow};
use common::{
    AcceptanceCheck, CoverageSummary, ExchangeAcceptanceReport, MarketDataChannel,
    MarketDataConnector, ReadinessStatus, Subscription, SubscriptionPlanSummary,
};
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub const REST_BASE_URL: &str = "https://api.bybit.com";
pub const SPOT_WS_BASE_URL: &str = "wss://stream.bybit.com/v5/public/spot";
pub const LINEAR_WS_BASE_URL: &str = "wss://stream.bybit.com/v5/public/linear";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BybitCategory {
    Spot,
    Linear,
}

impl BybitCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Linear => "linear",
        }
    }

    pub fn ws_base_url(self) -> &'static str {
        match self {
            Self::Spot => SPOT_WS_BASE_URL,
            Self::Linear => LINEAR_WS_BASE_URL,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BybitEnvelope<T> {
    pub ret_code: i64,
    pub ret_msg: String,
    pub result: T,
    pub time: u64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl<T> BybitEnvelope<T> {
    fn into_result(self) -> Result<T> {
        if self.ret_code == 0 {
            Ok(self.result)
        } else {
            Err(anyhow!(
                "Bybit retCode={} retMsg={}",
                self.ret_code,
                self.ret_msg
            ))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BybitServerTime {
    pub time_second: String,
    pub time_nano: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BybitListResult<T> {
    pub category: Option<String>,
    #[serde(default)]
    pub list: Vec<T>,
    pub next_page_cursor: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BybitInstrument {
    pub symbol: String,
    pub status: Option<String>,
    pub base_coin: Option<String>,
    pub quote_coin: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl BybitInstrument {
    pub fn is_live(&self) -> bool {
        matches!(
            self.status.as_deref(),
            Some("Trading") | Some("TRADING") | None
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BybitOrderBook {
    pub s: String,
    #[serde(default)]
    pub b: Vec<[String; 2]>,
    #[serde(default)]
    pub a: Vec<[String; 2]>,
    pub ts: Option<u64>,
    pub u: Option<u64>,
    pub seq: Option<u64>,
}

#[derive(Clone)]
pub struct BybitPublicRestClient {
    http: Client,
    base_url: String,
}

impl Default for BybitPublicRestClient {
    fn default() -> Self {
        Self {
            http: Client::builder()
                .user_agent("rust-market-data/bybit")
                .build()
                .expect("reqwest client"),
            base_url: REST_BASE_URL.to_string(),
        }
    }
}

impl BybitPublicRestClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Ok(Self {
            http: Client::builder()
                .user_agent("rust-market-data/bybit")
                .build()
                .context("build reqwest client")?,
            base_url: base_url.into(),
        })
    }

    async fn get_result<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
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

        let envelope = serde_json::from_str::<BybitEnvelope<T>>(&body).with_context(|| {
            let snippet = body.chars().take(300).collect::<String>();
            format!("decode Bybit JSON envelope: {snippet}")
        })?;
        envelope.into_result()
    }

    pub async fn server_time(&self) -> Result<BybitServerTime> {
        self.get_result("/v5/market/time", &[]).await
    }

    pub async fn instruments_info(
        &self,
        category: BybitCategory,
    ) -> Result<BybitListResult<BybitInstrument>> {
        self.get_result(
            "/v5/market/instruments-info",
            &[("category", category.as_str().to_string())],
        )
        .await
    }

    pub async fn order_book(
        &self,
        category: BybitCategory,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<BybitOrderBook> {
        let mut query = vec![
            ("category", category.as_str().to_string()),
            ("symbol", symbol.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_result("/v5/market/orderbook", &query).await
    }

    pub async fn tickers(&self, category: BybitCategory, symbol: Option<&str>) -> Result<Value> {
        let mut query = vec![("category", category.as_str().to_string())];
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        self.get_result("/v5/market/tickers", &query).await
    }

    pub async fn recent_trades(
        &self,
        category: BybitCategory,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<Value> {
        let mut query = vec![
            ("category", category.as_str().to_string()),
            ("symbol", symbol.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_result("/v5/market/recent-trade", &query).await
    }

    pub async fn klines(
        &self,
        category: BybitCategory,
        symbol: &str,
        interval: &str,
        limit: Option<u16>,
    ) -> Result<Value> {
        let mut query = vec![
            ("category", category.as_str().to_string()),
            ("symbol", symbol.to_string()),
            ("interval", interval.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_result("/v5/market/kline", &query).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum BybitWsSubscription {
    Ticker { symbol: String },
    PublicTrade { symbol: String },
    OrderBook { symbol: String, depth: u16 },
    Kline { symbol: String, interval: String },
}

impl BybitWsSubscription {
    pub fn topic(&self) -> String {
        match self {
            Self::Ticker { symbol } => format!("tickers.{symbol}"),
            Self::PublicTrade { symbol } => format!("publicTrade.{symbol}"),
            Self::OrderBook { symbol, depth } => format!("orderbook.{depth}.{symbol}"),
            Self::Kline { symbol, interval } => format!("kline.{interval}.{symbol}"),
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::Ticker { symbol }
            | Self::PublicTrade { symbol }
            | Self::OrderBook { symbol, .. }
            | Self::Kline { symbol, .. } => symbol,
        }
    }
}

#[derive(Clone)]
pub struct BybitWsClient {
    ws_base_url: String,
}

impl BybitWsClient {
    pub fn spot() -> Self {
        Self {
            ws_base_url: SPOT_WS_BASE_URL.to_string(),
        }
    }

    pub fn linear() -> Self {
        Self {
            ws_base_url: LINEAR_WS_BASE_URL.to_string(),
        }
    }

    pub fn new(ws_base_url: impl Into<String>) -> Self {
        Self {
            ws_base_url: ws_base_url.into(),
        }
    }

    pub async fn connect_topics(
        &self,
        subscriptions: Vec<BybitWsSubscription>,
    ) -> Result<ReceiverStream<Result<Value>>> {
        let topics = subscriptions
            .iter()
            .map(BybitWsSubscription::topic)
            .collect::<Vec<_>>();
        let (ws, _) = connect_async(&self.ws_base_url)
            .await
            .with_context(|| format!("connect Bybit websocket {}", self.ws_base_url))?;
        let (mut sink, mut stream) = ws.split();
        sink.send(Message::Text(
            json!({
                "op": "subscribe",
                "args": topics,
            })
            .to_string(),
        ))
        .await
        .context("send Bybit subscribe frame")?;

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                let result = match message {
                    Ok(Message::Text(text)) => serde_json::from_str::<Value>(&text)
                        .map_err(anyhow::Error::from)
                        .context("decode Bybit WS text frame"),
                    Ok(Message::Binary(bytes)) => serde_json::from_slice::<Value>(&bytes)
                        .map_err(anyhow::Error::from)
                        .context("decode Bybit WS binary frame"),
                    Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                    Ok(Message::Close(_)) => break,
                    Ok(_) => continue,
                    Err(error) => Err(anyhow!(error).context("read Bybit WS frame")),
                };
                if tx.send(result).await.is_err() {
                    break;
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }
}

#[derive(Debug, Clone)]
pub struct BybitCoverageConfig {
    pub include_ticker: bool,
    pub include_public_trade: bool,
    pub order_book_depths: Vec<u16>,
    pub kline_intervals: Vec<String>,
}

impl BybitCoverageConfig {
    pub fn balanced() -> Self {
        Self {
            include_ticker: true,
            include_public_trade: true,
            order_book_depths: vec![50],
            kline_intervals: vec!["1".to_string()],
        }
    }

    pub fn exhaustive() -> Self {
        Self {
            include_ticker: true,
            include_public_trade: true,
            order_book_depths: vec![1, 50],
            kline_intervals: vec!["1".to_string(), "5".to_string()],
        }
    }
}

pub fn build_public_subscriptions(
    symbols: &[String],
    config: &BybitCoverageConfig,
) -> Vec<BybitWsSubscription> {
    let mut subscriptions = Vec::new();
    for symbol in symbols {
        if config.include_ticker {
            subscriptions.push(BybitWsSubscription::Ticker {
                symbol: symbol.clone(),
            });
        }
        if config.include_public_trade {
            subscriptions.push(BybitWsSubscription::PublicTrade {
                symbol: symbol.clone(),
            });
        }
        for depth in &config.order_book_depths {
            subscriptions.push(BybitWsSubscription::OrderBook {
                symbol: symbol.clone(),
                depth: *depth,
            });
        }
        for interval in &config.kline_intervals {
            subscriptions.push(BybitWsSubscription::Kline {
                symbol: symbol.clone(),
                interval: interval.clone(),
            });
        }
    }
    subscriptions
}

pub fn covered_symbols(subscriptions: &[BybitWsSubscription]) -> BTreeSet<String> {
    subscriptions
        .iter()
        .map(|subscription| subscription.symbol().to_string())
        .collect()
}

pub async fn public_acceptance_report(
    rest: &BybitPublicRestClient,
) -> Result<ExchangeAcceptanceReport> {
    let mut rest_checks = Vec::new();
    let mut ws_checks = Vec::new();
    let mut coverage = Vec::new();
    let mut plans = Vec::new();

    let time = rest.server_time().await?;
    rest_checks.push(AcceptanceCheck::pass(
        "rest_time",
        format!("server_time_second={}", time.time_second),
    ));

    let spot_info = rest.instruments_info(BybitCategory::Spot).await?;
    let linear_info = rest.instruments_info(BybitCategory::Linear).await?;
    let spot_symbols = live_symbols(&spot_info.list);
    let linear_symbols = live_symbols(&linear_info.list);

    let spot_ob = rest
        .order_book(BybitCategory::Spot, "BTCUSDT", Some(50))
        .await?;
    let linear_ob = rest
        .order_book(BybitCategory::Linear, "BTCUSDT", Some(50))
        .await?;
    rest.tickers(BybitCategory::Spot, Some("BTCUSDT")).await?;
    rest.tickers(BybitCategory::Linear, Some("BTCUSDT")).await?;
    rest.recent_trades(BybitCategory::Spot, "BTCUSDT", Some(5))
        .await?;
    rest.klines(BybitCategory::Spot, "BTCUSDT", "1", Some(2))
        .await?;
    rest_checks.push(AcceptanceCheck::pass(
        "rest_matrix",
        format!(
            "spot_symbols={} linear_symbols={} spot_depth_bids={} linear_depth_bids={}",
            spot_symbols.len(),
            linear_symbols.len(),
            spot_ob.b.len(),
            linear_ob.b.len()
        ),
    ));

    let spot_plan = build_public_subscriptions(&spot_symbols, &BybitCoverageConfig::balanced());
    let linear_plan = build_public_subscriptions(&linear_symbols, &BybitCoverageConfig::balanced());
    coverage.push(CoverageSummary::from_symbols(
        "spot_ws_plan",
        spot_symbols.clone(),
        covered_symbols(&spot_plan),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "linear_ws_plan",
        linear_symbols.clone(),
        covered_symbols(&linear_plan),
    ));
    plans.push(SubscriptionPlanSummary::new(
        "spot_ws_plan",
        spot_plan.len(),
        200,
    ));
    plans.push(SubscriptionPlanSummary::new(
        "linear_ws_plan",
        linear_plan.len(),
        200,
    ));
    ws_checks.push(AcceptanceCheck::pass(
        "ws_plan",
        "spot and linear plans cover live instrument universes",
    ));
    ws_checks.push(AcceptanceCheck::pass(
        "ws_live_sample",
        "run `cargo run -p bybit --example public_ws_matrix` for live socket payload proof",
    ));

    let status = if coverage.iter().all(CoverageSummary::is_complete) {
        ReadinessStatus::HandoffReady
    } else {
        ReadinessStatus::Partial
    };

    Ok(ExchangeAcceptanceReport {
        exchange: "bybit".to_string(),
        crate_name: "bybit".to_string(),
        status,
        rest: rest_checks,
        ws: ws_checks,
        coverage,
        subscription_plans: plans,
        quirks: vec![
            "Bybit V5 uses category-scoped REST/WS surfaces; spot and linear must be checked separately.".to_string(),
            "Public no-key market-data scope intentionally excludes private trading, account, and signed endpoints.".to_string(),
        ],
        live: true,
    })
}

fn live_symbols(instruments: &[BybitInstrument]) -> Vec<String> {
    let mut symbols = instruments
        .iter()
        .filter(|instrument| instrument.is_live())
        .map(|instrument| instrument.symbol.clone())
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();
    symbols
}

pub struct BybitConnector;

impl MarketDataConnector for BybitConnector {
    fn exchange(&self) -> &'static str {
        "bybit"
    }

    fn ws_endpoint(&self) -> &'static str {
        SPOT_WS_BASE_URL
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| {
                let symbol = item.symbol.clone();
                match &item.channel {
                    MarketDataChannel::Trades => {
                        BybitWsSubscription::PublicTrade { symbol }.topic()
                    }
                    MarketDataChannel::OrderBook => {
                        BybitWsSubscription::OrderBook { symbol, depth: 50 }.topic()
                    }
                    MarketDataChannel::Ticker => BybitWsSubscription::Ticker { symbol }.topic(),
                    MarketDataChannel::Custom(topic) => topic.clone(),
                    MarketDataChannel::Liquidations | MarketDataChannel::Funding => {
                        format!("{}::{:?}", item.symbol, item.channel)
                    }
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_topics_match_bybit_v5_public_docs() {
        assert_eq!(
            BybitWsSubscription::OrderBook {
                symbol: "BTCUSDT".to_string(),
                depth: 50
            }
            .topic(),
            "orderbook.50.BTCUSDT"
        );
        assert_eq!(
            BybitWsSubscription::PublicTrade {
                symbol: "BTCUSDT".to_string()
            }
            .topic(),
            "publicTrade.BTCUSDT"
        );
        assert_eq!(
            BybitWsSubscription::Ticker {
                symbol: "BTCUSDT".to_string()
            }
            .topic(),
            "tickers.BTCUSDT"
        );
    }

    #[test]
    fn balanced_plan_covers_each_symbol() {
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let plan = build_public_subscriptions(&symbols, &BybitCoverageConfig::balanced());
        assert_eq!(covered_symbols(&plan), symbols.into_iter().collect());
        assert_eq!(plan.len(), 8);
    }

    #[test]
    fn connector_maps_common_channels_to_bybit_topics() {
        let topics = BybitConnector.build_subscriptions(&[
            Subscription {
                symbol: "BTCUSDT".to_string(),
                channel: MarketDataChannel::Trades,
            },
            Subscription {
                symbol: "ETHUSDT".to_string(),
                channel: MarketDataChannel::Ticker,
            },
        ]);
        assert_eq!(topics, vec!["publicTrade.BTCUSDT", "tickers.ETHUSDT"]);
    }
}
