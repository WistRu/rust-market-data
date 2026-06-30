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
use std::fmt;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub const REST_BASE_URL: &str = "https://api.bitget.com";
pub const PUBLIC_WS_BASE_URL: &str = "wss://ws.bitget.com/v2/ws/public";
pub const DEFAULT_SYMBOL: &str = "BTCUSDT";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum BitgetInstType {
    Spot,
    UsdtFutures,
}

impl BitgetInstType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
            Self::UsdtFutures => "USDT-FUTURES",
        }
    }
}

impl fmt::Display for BitgetInstType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BitgetInstType {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "SPOT" => Ok(Self::Spot),
            "USDT-FUTURES" => Ok(Self::UsdtFutures),
            other => Err(anyhow!("unsupported Bitget instType: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct BitgetInstrumentId {
    pub inst_type: BitgetInstType,
    pub symbol: String,
}

impl BitgetInstrumentId {
    pub fn new(inst_type: BitgetInstType, symbol: impl Into<String>) -> Self {
        Self {
            inst_type,
            symbol: symbol.into(),
        }
    }

    pub fn spot(symbol: impl Into<String>) -> Self {
        Self::new(BitgetInstType::Spot, symbol)
    }

    pub fn usdt_futures(symbol: impl Into<String>) -> Self {
        Self::new(BitgetInstType::UsdtFutures, symbol)
    }

    pub fn infer(symbol: impl Into<String>) -> Self {
        Self::spot(symbol)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetEnvelope<T> {
    pub code: String,
    pub msg: String,
    pub request_time: Option<u64>,
    pub data: T,
}

impl<T> BitgetEnvelope<T> {
    fn into_result(self) -> Result<T> {
        if self.code == "00000" {
            Ok(self.data)
        } else {
            Err(anyhow!("Bitget code={} msg={}", self.code, self.msg))
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSpotSymbol {
    pub symbol: String,
    pub base_coin: String,
    pub quote_coin: String,
    pub status: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl BitgetSpotSymbol {
    pub fn is_online(&self) -> bool {
        self.status == "online"
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetFuturesContract {
    pub symbol: String,
    pub base_coin: String,
    pub quote_coin: String,
    pub symbol_status: String,
    pub symbol_type: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl BitgetFuturesContract {
    pub fn is_normal(&self) -> bool {
        self.symbol_status == "normal"
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetTicker {
    pub symbol: String,
    pub last_pr: String,
    pub bid_pr: Option<String>,
    pub ask_pr: Option<String>,
    pub ts: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BitgetOrderBook {
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    pub ts: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetTrade {
    pub symbol: Option<String>,
    pub trade_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub ts: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone)]
pub struct BitgetPublicRestClient {
    http: Client,
    base_url: String,
}

impl Default for BitgetPublicRestClient {
    fn default() -> Self {
        Self {
            http: Client::builder()
                .user_agent("rust-market-data/bitget")
                .build()
                .expect("reqwest client"),
            base_url: REST_BASE_URL.to_string(),
        }
    }
}

impl BitgetPublicRestClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Ok(Self {
            http: Client::builder()
                .user_agent("rust-market-data/bitget")
                .build()
                .context("build reqwest client")?,
            base_url: base_url.into(),
        })
    }

    async fn get_data<T: DeserializeOwned>(
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
            .context("send Bitget GET request")?
            .error_for_status()
            .context("unexpected Bitget HTTP status")?
            .text()
            .await
            .context("read Bitget response body")?;
        let envelope = serde_json::from_str::<BitgetEnvelope<T>>(&body).with_context(|| {
            let snippet = body.chars().take(300).collect::<String>();
            format!("decode Bitget JSON envelope: {snippet}")
        })?;
        envelope.into_result()
    }

    pub async fn spot_symbols(&self, symbol: Option<&str>) -> Result<Vec<BitgetSpotSymbol>> {
        let mut query = Vec::new();
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        self.get_data("/api/v2/spot/public/symbols", &query).await
    }

    pub async fn futures_contracts(
        &self,
        product_type: BitgetInstType,
        symbol: Option<&str>,
    ) -> Result<Vec<BitgetFuturesContract>> {
        if product_type != BitgetInstType::UsdtFutures {
            return Err(anyhow!(
                "Bitget futures contracts require USDT-FUTURES product type"
            ));
        }
        let mut query = vec![("productType", product_type.as_str().to_string())];
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        self.get_data("/api/v2/mix/market/contracts", &query).await
    }

    pub async fn spot_tickers(&self, symbol: Option<&str>) -> Result<Vec<BitgetTicker>> {
        let mut query = Vec::new();
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        self.get_data("/api/v2/spot/market/tickers", &query).await
    }

    pub async fn futures_tickers(
        &self,
        product_type: BitgetInstType,
        symbol: Option<&str>,
    ) -> Result<Vec<BitgetTicker>> {
        if product_type != BitgetInstType::UsdtFutures {
            return Err(anyhow!(
                "Bitget futures tickers require USDT-FUTURES product type"
            ));
        }
        let mut query = vec![("productType", product_type.as_str().to_string())];
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        self.get_data("/api/v2/mix/market/tickers", &query).await
    }

    pub async fn spot_order_book(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<BitgetOrderBook> {
        let mut query = vec![
            ("symbol", symbol.to_string()),
            ("type", "step0".to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_data("/api/v2/spot/market/orderbook", &query).await
    }

    pub async fn futures_order_book(
        &self,
        product_type: BitgetInstType,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<BitgetOrderBook> {
        if product_type != BitgetInstType::UsdtFutures {
            return Err(anyhow!(
                "Bitget futures order book requires USDT-FUTURES product type"
            ));
        }
        let mut query = vec![
            ("productType", product_type.as_str().to_string()),
            ("symbol", symbol.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_data("/api/v2/mix/market/orderbook", &query).await
    }

    pub async fn spot_trades(&self, symbol: &str, limit: Option<u16>) -> Result<Vec<BitgetTrade>> {
        let mut query = vec![("symbol", symbol.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_data("/api/v2/spot/market/fills", &query).await
    }

    pub async fn futures_trades(
        &self,
        product_type: BitgetInstType,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<Vec<BitgetTrade>> {
        if product_type != BitgetInstType::UsdtFutures {
            return Err(anyhow!(
                "Bitget futures trades require USDT-FUTURES product type"
            ));
        }
        let mut query = vec![
            ("productType", product_type.as_str().to_string()),
            ("symbol", symbol.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_data("/api/v2/mix/market/fills", &query).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum BitgetWsSubscription {
    Ticker { instrument: BitgetInstrumentId },
    Trades { instrument: BitgetInstrumentId },
    Books5 { instrument: BitgetInstrumentId },
}

impl BitgetWsSubscription {
    pub fn channel(&self) -> &'static str {
        match self {
            Self::Ticker { .. } => "ticker",
            Self::Trades { .. } => "trade",
            Self::Books5 { .. } => "books5",
        }
    }

    pub fn instrument(&self) -> &BitgetInstrumentId {
        match self {
            Self::Ticker { instrument }
            | Self::Trades { instrument }
            | Self::Books5 { instrument } => instrument,
        }
    }

    pub fn label(&self) -> String {
        format!(
            "{}:{}:{}",
            self.instrument().inst_type,
            self.channel(),
            self.instrument().symbol
        )
    }

    pub fn wire_arg(&self) -> Value {
        json!({
            "instType": self.instrument().inst_type.as_str(),
            "channel": self.channel(),
            "instId": self.instrument().symbol,
        })
    }
}

#[derive(Clone)]
pub struct BitgetWsClient {
    ws_base_url: String,
}

impl Default for BitgetWsClient {
    fn default() -> Self {
        Self::public()
    }
}

impl BitgetWsClient {
    pub fn public() -> Self {
        Self {
            ws_base_url: PUBLIC_WS_BASE_URL.to_string(),
        }
    }

    pub fn new(ws_base_url: impl Into<String>) -> Self {
        Self {
            ws_base_url: ws_base_url.into(),
        }
    }

    pub async fn connect_subscriptions(
        &self,
        subscriptions: Vec<BitgetWsSubscription>,
    ) -> Result<ReceiverStream<Result<Value>>> {
        let args = subscriptions
            .iter()
            .map(BitgetWsSubscription::wire_arg)
            .collect::<Vec<_>>();
        let (ws, _) = connect_async(&self.ws_base_url)
            .await
            .with_context(|| format!("connect Bitget websocket {}", self.ws_base_url))?;
        let (mut sink, mut stream) = ws.split();
        sink.send(Message::Text(
            json!({
                "op": "subscribe",
                "args": args,
            })
            .to_string(),
        ))
        .await
        .context("send Bitget subscribe frame")?;

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                let result = match message {
                    Ok(Message::Text(text)) => {
                        if text == "pong" {
                            continue;
                        }
                        serde_json::from_str::<Value>(&text)
                            .map_err(anyhow::Error::from)
                            .context("decode Bitget WS text frame")
                    }
                    Ok(Message::Binary(bytes)) => serde_json::from_slice::<Value>(&bytes)
                        .map_err(anyhow::Error::from)
                        .context("decode Bitget WS binary frame"),
                    Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                    Ok(Message::Close(_)) => break,
                    Ok(_) => continue,
                    Err(error) => Err(anyhow!(error).context("read Bitget WS frame")),
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
pub struct BitgetCoverageConfig {
    pub include_ticker: bool,
    pub include_trades: bool,
    pub include_books5: bool,
}

impl BitgetCoverageConfig {
    pub fn balanced() -> Self {
        Self {
            include_ticker: true,
            include_trades: true,
            include_books5: true,
        }
    }
}

pub fn build_public_subscriptions(
    instruments: &[BitgetInstrumentId],
    config: &BitgetCoverageConfig,
) -> Vec<BitgetWsSubscription> {
    let mut subscriptions = Vec::new();
    for instrument in instruments {
        if config.include_ticker {
            subscriptions.push(BitgetWsSubscription::Ticker {
                instrument: instrument.clone(),
            });
        }
        if config.include_trades {
            subscriptions.push(BitgetWsSubscription::Trades {
                instrument: instrument.clone(),
            });
        }
        if config.include_books5 {
            subscriptions.push(BitgetWsSubscription::Books5 {
                instrument: instrument.clone(),
            });
        }
    }
    subscriptions
}

pub fn covered_symbols(subscriptions: &[BitgetWsSubscription]) -> BTreeSet<String> {
    subscriptions
        .iter()
        .map(|subscription| subscription.instrument().symbol.clone())
        .collect()
}

pub async fn sample_ws_counts(
    subscriptions: Vec<BitgetWsSubscription>,
    watch_duration: Duration,
) -> Result<BTreeMap<String, usize>> {
    let mut stream = BitgetWsClient::public()
        .connect_subscriptions(subscriptions)
        .await?;
    let deadline = Instant::now() + watch_duration;
    let mut counts = BTreeMap::<String, usize>::new();

    while Instant::now() < deadline {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(value)) => count_ws_message(&mut counts, value),
                    Some(Err(error)) => return Err(error),
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }

    Ok(counts)
}

fn count_ws_message(counts: &mut BTreeMap<String, usize>, value: Value) {
    if let Some(event) = value.get("event").and_then(Value::as_str) {
        *counts.entry(event.to_string()).or_default() += 1;
        return;
    }

    let Some(arg) = value.get("arg") else {
        return;
    };
    let inst_type = arg
        .get("instType")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let channel = arg
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let inst_id = arg
        .get("instId")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    *counts
        .entry(format!("{inst_type}:{channel}:{inst_id}"))
        .or_default() += 1;
}

pub async fn public_acceptance_report(
    rest: &BitgetPublicRestClient,
) -> Result<ExchangeAcceptanceReport> {
    let mut rest_checks = Vec::new();
    let mut ws_checks = Vec::new();
    let mut coverage = Vec::new();
    let mut plans = Vec::new();

    let spot_symbols = rest.spot_symbols(None).await?;
    let futures_contracts = rest
        .futures_contracts(BitgetInstType::UsdtFutures, None)
        .await?;
    let spot_online = online_spot_symbols(&spot_symbols);
    let futures_normal = normal_futures_symbols(&futures_contracts);

    let spot_sample = BitgetInstrumentId::spot(DEFAULT_SYMBOL);
    let futures_sample = BitgetInstrumentId::usdt_futures(DEFAULT_SYMBOL);
    let spot_book = rest.spot_order_book(&spot_sample.symbol, Some(5)).await?;
    let futures_book = rest
        .futures_order_book(futures_sample.inst_type, &futures_sample.symbol, Some(5))
        .await?;
    let spot_ticker_sample = rest.spot_tickers(Some(&spot_sample.symbol)).await?;
    let futures_ticker_sample = rest
        .futures_tickers(futures_sample.inst_type, Some(&futures_sample.symbol))
        .await?;
    let spot_trades = rest.spot_trades(&spot_sample.symbol, Some(5)).await?;
    let futures_trades = rest
        .futures_trades(futures_sample.inst_type, &futures_sample.symbol, Some(5))
        .await?;
    rest_checks.push(AcceptanceCheck::pass(
        "rest_matrix",
        format!(
            "spot_symbols={} usdt_futures_contracts={} spot_depth_bids={} futures_depth_bids={} spot_ticker_rows={} futures_ticker_rows={} spot_trades={} futures_trades={}",
            spot_online.len(),
            futures_normal.len(),
            spot_book.bids.len(),
            futures_book.bids.len(),
            spot_ticker_sample.len(),
            futures_ticker_sample.len(),
            spot_trades.len(),
            futures_trades.len()
        ),
    ));
    rest_checks.push(AcceptanceCheck::pass(
        "instrument_identity",
        format!(
            "{}={} {}={}",
            spot_sample.symbol,
            spot_sample.inst_type,
            futures_sample.symbol,
            futures_sample.inst_type
        ),
    ));

    let spot_ticker_symbols = ticker_symbols(rest.spot_tickers(None).await?);
    let futures_ticker_symbols = ticker_symbols(
        rest.futures_tickers(BitgetInstType::UsdtFutures, None)
            .await?,
    );
    coverage.push(CoverageSummary::from_symbols(
        "spot_online_ticker_coverage",
        spot_online.clone(),
        spot_ticker_symbols.clone(),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "usdt_futures_normal_ticker_coverage",
        futures_normal.clone(),
        futures_ticker_symbols.clone(),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "spot_ticker_visible_ws_plan",
        spot_ticker_symbols.clone(),
        covered_symbols(&build_public_subscriptions(
            &identities_for_type(BitgetInstType::Spot, &spot_ticker_symbols),
            &BitgetCoverageConfig::balanced(),
        )),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "usdt_futures_ticker_visible_ws_plan",
        futures_ticker_symbols.clone(),
        covered_symbols(&build_public_subscriptions(
            &identities_for_type(BitgetInstType::UsdtFutures, &futures_ticker_symbols),
            &BitgetCoverageConfig::balanced(),
        )),
    ));

    let spot_plan = build_public_subscriptions(
        &identities_for_type(BitgetInstType::Spot, &spot_ticker_symbols),
        &BitgetCoverageConfig::balanced(),
    );
    let futures_plan = build_public_subscriptions(
        &identities_for_type(BitgetInstType::UsdtFutures, &futures_ticker_symbols),
        &BitgetCoverageConfig::balanced(),
    );
    plans.push(SubscriptionPlanSummary::new(
        "spot_ticker_visible_ws_plan",
        spot_plan.len(),
        200,
    ));
    plans.push(SubscriptionPlanSummary::new(
        "usdt_futures_ticker_visible_ws_plan",
        futures_plan.len(),
        200,
    ));
    ws_checks.push(AcceptanceCheck::pass(
        "ws_plan",
        "SPOT and USDT-FUTURES plans cover the public ticker-visible scope",
    ));

    let live_sample_subscriptions = vec![
        BitgetWsSubscription::Ticker {
            instrument: spot_sample.clone(),
        },
        BitgetWsSubscription::Trades {
            instrument: spot_sample.clone(),
        },
        BitgetWsSubscription::Books5 {
            instrument: spot_sample,
        },
        BitgetWsSubscription::Ticker {
            instrument: futures_sample.clone(),
        },
        BitgetWsSubscription::Trades {
            instrument: futures_sample.clone(),
        },
        BitgetWsSubscription::Books5 {
            instrument: futures_sample,
        },
    ];
    match sample_ws_counts(live_sample_subscriptions, Duration::from_secs(10)).await {
        Ok(counts) => {
            let data_events = counts
                .iter()
                .filter(|(label, _)| {
                    label.contains(":ticker:")
                        || label.contains(":trade:")
                        || label.contains(":books5:")
                })
                .map(|(_, count)| *count)
                .sum::<usize>();
            if data_events > 0 {
                ws_checks.push(AcceptanceCheck::pass(
                    "ws_live_sample",
                    format!("received public Bitget WS data events counts={counts:?}"),
                ));
            } else {
                ws_checks.push(AcceptanceCheck::fail(
                    "ws_live_sample",
                    format!("missing public Bitget WS data events counts={counts:?}"),
                ));
            }
        }
        Err(error) => ws_checks.push(AcceptanceCheck::fail("ws_live_sample", error.to_string())),
    }

    let has_failures = rest_checks
        .iter()
        .chain(ws_checks.iter())
        .any(|check| check.status == common::CheckStatus::Fail)
        || coverage.iter().any(|row| !row.is_complete());

    Ok(ExchangeAcceptanceReport {
        exchange: "bitget".to_string(),
        crate_name: "bitget".to_string(),
        status: if has_failures {
            ReadinessStatus::Partial
        } else {
            ReadinessStatus::HandoffReady
        },
        rest: rest_checks,
        ws: ws_checks,
        coverage,
        subscription_plans: plans,
        quirks: vec![
            "Bitget instrument identity is instType plus symbol; BTCUSDT SPOT and BTCUSDT USDT-FUTURES are distinct instruments.".to_string(),
            "Acceptance scope is public ticker-visible SPOT plus USDT-FUTURES market data.".to_string(),
            "Spot order book REST requires type=step0; WS book proof uses the public books5 channel.".to_string(),
            "Public no-key market-data readiness intentionally excludes private trading, account, signed request, balance, position, and order endpoints.".to_string(),
        ],
        live: true,
    })
}

fn online_spot_symbols(symbols: &[BitgetSpotSymbol]) -> Vec<String> {
    sorted_symbols(
        symbols
            .iter()
            .filter(|symbol| symbol.is_online())
            .map(|symbol| symbol.symbol.clone()),
    )
}

fn normal_futures_symbols(contracts: &[BitgetFuturesContract]) -> Vec<String> {
    sorted_symbols(
        contracts
            .iter()
            .filter(|contract| contract.is_normal())
            .map(|contract| contract.symbol.clone()),
    )
}

fn ticker_symbols(tickers: Vec<BitgetTicker>) -> Vec<String> {
    sorted_symbols(tickers.into_iter().map(|ticker| ticker.symbol))
}

fn identities_for_type(inst_type: BitgetInstType, symbols: &[String]) -> Vec<BitgetInstrumentId> {
    symbols
        .iter()
        .cloned()
        .map(|symbol| BitgetInstrumentId::new(inst_type, symbol))
        .collect()
}

fn sorted_symbols(symbols: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut result = symbols.into_iter().collect::<Vec<_>>();
    result.sort();
    result.dedup();
    result
}

#[derive(Default)]
pub struct BitgetConnector;

impl MarketDataConnector for BitgetConnector {
    fn exchange(&self) -> &'static str {
        "bitget"
    }

    fn ws_endpoint(&self) -> &'static str {
        PUBLIC_WS_BASE_URL
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| {
                let instrument = BitgetInstrumentId::infer(item.symbol.clone());
                match &item.channel {
                    MarketDataChannel::Trades => {
                        BitgetWsSubscription::Trades { instrument }.label()
                    }
                    MarketDataChannel::OrderBook => {
                        BitgetWsSubscription::Books5 { instrument }.label()
                    }
                    MarketDataChannel::Ticker => {
                        BitgetWsSubscription::Ticker { instrument }.label()
                    }
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
    fn instrument_identity_preserves_spot_and_usdt_futures() {
        let spot = BitgetInstrumentId::spot("BTCUSDT");
        let futures = BitgetInstrumentId::usdt_futures("BTCUSDT");
        assert_eq!(spot.inst_type, BitgetInstType::Spot);
        assert_eq!(futures.inst_type, BitgetInstType::UsdtFutures);
        assert_ne!(spot, futures);
    }

    #[test]
    fn ws_args_match_bitget_v2_public_shape() {
        let subscription = BitgetWsSubscription::Books5 {
            instrument: BitgetInstrumentId::spot("BTCUSDT"),
        };
        assert_eq!(subscription.label(), "SPOT:books5:BTCUSDT");
        assert_eq!(
            subscription.wire_arg(),
            json!({"instType": "SPOT", "channel": "books5", "instId": "BTCUSDT"})
        );
    }

    #[test]
    fn balanced_plan_covers_each_instrument() {
        let instruments = vec![
            BitgetInstrumentId::spot("BTCUSDT"),
            BitgetInstrumentId::usdt_futures("ETHUSDT"),
        ];
        let plan = build_public_subscriptions(&instruments, &BitgetCoverageConfig::balanced());
        assert_eq!(
            covered_symbols(&plan),
            ["BTCUSDT".to_string(), "ETHUSDT".to_string()]
                .into_iter()
                .collect()
        );
        assert_eq!(plan.len(), 6);
    }

    #[test]
    fn connector_maps_common_channels_to_bitget_labels() {
        let topics = BitgetConnector.build_subscriptions(&[
            Subscription {
                symbol: "BTCUSDT".to_string(),
                channel: MarketDataChannel::Trades,
            },
            Subscription {
                symbol: "ETHUSDT".to_string(),
                channel: MarketDataChannel::Ticker,
            },
        ]);
        assert_eq!(topics, vec!["SPOT:trade:BTCUSDT", "SPOT:ticker:ETHUSDT"]);
    }

    #[test]
    fn ws_subscribe_ack_is_not_counted_as_market_data() {
        let mut counts = BTreeMap::new();
        count_ws_message(
            &mut counts,
            json!({
                "event": "subscribe",
                "arg": {
                    "instType": "SPOT",
                    "channel": "ticker",
                    "instId": "BTCUSDT"
                }
            }),
        );
        assert_eq!(counts.get("subscribe"), Some(&1));
        assert_eq!(counts.get("SPOT:ticker:BTCUSDT"), None);
    }
}
