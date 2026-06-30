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

pub const REST_BASE_URL: &str = "https://www.okx.com";
pub const PUBLIC_WS_BASE_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
pub const DEFAULT_SPOT_INST_ID: &str = "BTC-USDT";
pub const DEFAULT_SWAP_INST_ID: &str = "BTC-USDT-SWAP";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "UPPERCASE")]
pub enum OkxInstType {
    Spot,
    Swap,
}

impl OkxInstType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
            Self::Swap => "SWAP",
        }
    }
}

impl fmt::Display for OkxInstType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OkxInstType {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "SPOT" => Ok(Self::Spot),
            "SWAP" => Ok(Self::Swap),
            other => Err(anyhow!("unsupported OKX instType: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct OkxInstrumentId {
    pub inst_type: OkxInstType,
    pub inst_id: String,
}

impl OkxInstrumentId {
    pub fn new(inst_type: OkxInstType, inst_id: impl Into<String>) -> Self {
        Self {
            inst_type,
            inst_id: inst_id.into(),
        }
    }

    pub fn spot(inst_id: impl Into<String>) -> Self {
        Self::new(OkxInstType::Spot, inst_id)
    }

    pub fn swap(inst_id: impl Into<String>) -> Self {
        Self::new(OkxInstType::Swap, inst_id)
    }

    pub fn infer_from_inst_id(inst_id: impl Into<String>) -> Self {
        let inst_id = inst_id.into();
        if inst_id.ends_with("-SWAP") {
            Self::swap(inst_id)
        } else {
            Self::spot(inst_id)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OkxEnvelope<T> {
    pub code: String,
    pub msg: String,
    pub data: Vec<T>,
}

impl<T> OkxEnvelope<T> {
    fn into_result(self) -> Result<Vec<T>> {
        if self.code == "0" {
            Ok(self.data)
        } else {
            Err(anyhow!("OKX code={} msg={}", self.code, self.msg))
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OkxInstrument {
    pub inst_type: String,
    pub inst_id: String,
    pub state: String,
    pub base_ccy: Option<String>,
    pub quote_ccy: Option<String>,
    pub inst_family: Option<String>,
    pub uly: Option<String>,
    pub settle_ccy: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl OkxInstrument {
    pub fn is_live(&self) -> bool {
        self.state == "live"
    }

    pub fn identity(&self) -> Result<OkxInstrumentId> {
        Ok(OkxInstrumentId::new(
            OkxInstType::from_str(&self.inst_type)?,
            self.inst_id.clone(),
        ))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OkxTicker {
    pub inst_type: String,
    pub inst_id: String,
    pub last: String,
    pub bid_px: Option<String>,
    pub ask_px: Option<String>,
    pub ts: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OkxTrade {
    pub inst_id: String,
    pub trade_id: String,
    pub px: String,
    pub sz: String,
    pub side: String,
    pub ts: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OkxOrderBook {
    #[serde(default)]
    pub bids: Vec<Vec<String>>,
    #[serde(default)]
    pub asks: Vec<Vec<String>>,
    pub ts: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone)]
pub struct OkxPublicRestClient {
    http: Client,
    base_url: String,
}

impl Default for OkxPublicRestClient {
    fn default() -> Self {
        Self {
            http: Client::builder()
                .user_agent("rust-market-data/okx")
                .build()
                .expect("reqwest client"),
            base_url: REST_BASE_URL.to_string(),
        }
    }
}

impl OkxPublicRestClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Ok(Self {
            http: Client::builder()
                .user_agent("rust-market-data/okx")
                .build()
                .context("build reqwest client")?,
            base_url: base_url.into(),
        })
    }

    async fn get_data<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Vec<T>> {
        let url = format!("{}{}", self.base_url, path);
        let body = self
            .http
            .get(url)
            .query(query)
            .send()
            .await
            .context("send OKX GET request")?
            .error_for_status()
            .context("unexpected OKX HTTP status")?
            .text()
            .await
            .context("read OKX response body")?;

        let envelope = serde_json::from_str::<OkxEnvelope<T>>(&body).with_context(|| {
            let snippet = body.chars().take(300).collect::<String>();
            format!("decode OKX JSON envelope: {snippet}")
        })?;
        envelope.into_result()
    }

    pub async fn instruments(&self, inst_type: OkxInstType) -> Result<Vec<OkxInstrument>> {
        self.get_data(
            "/api/v5/public/instruments",
            &[("instType", inst_type.as_str().to_string())],
        )
        .await
    }

    pub async fn tickers(&self, inst_type: OkxInstType) -> Result<Vec<OkxTicker>> {
        self.get_data(
            "/api/v5/market/tickers",
            &[("instType", inst_type.as_str().to_string())],
        )
        .await
    }

    pub async fn ticker(&self, instrument: &OkxInstrumentId) -> Result<OkxTicker> {
        self.get_one(
            "/api/v5/market/ticker",
            &[("instId", instrument.inst_id.clone())],
        )
        .await
    }

    pub async fn trades(
        &self,
        instrument: &OkxInstrumentId,
        limit: Option<u16>,
    ) -> Result<Vec<OkxTrade>> {
        let mut query = vec![("instId", instrument.inst_id.clone())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get_data("/api/v5/market/trades", &query).await
    }

    pub async fn order_book(
        &self,
        instrument: &OkxInstrumentId,
        size: Option<u16>,
    ) -> Result<OkxOrderBook> {
        let mut query = vec![("instId", instrument.inst_id.clone())];
        if let Some(size) = size {
            query.push(("sz", size.to_string()));
        }
        self.get_one("/api/v5/market/books", &query).await
    }

    async fn get_one<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        self.get_data(path, query)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("OKX {path} returned no data"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum OkxWsSubscription {
    Ticker { instrument: OkxInstrumentId },
    Trades { instrument: OkxInstrumentId },
    Books5 { instrument: OkxInstrumentId },
}

impl OkxWsSubscription {
    pub fn channel(&self) -> &'static str {
        match self {
            Self::Ticker { .. } => "tickers",
            Self::Trades { .. } => "trades",
            Self::Books5 { .. } => "books5",
        }
    }

    pub fn instrument(&self) -> &OkxInstrumentId {
        match self {
            Self::Ticker { instrument }
            | Self::Trades { instrument }
            | Self::Books5 { instrument } => instrument,
        }
    }

    pub fn label(&self) -> String {
        format!("{}:{}", self.channel(), self.instrument().inst_id)
    }

    pub fn wire_arg(&self) -> Value {
        json!({
            "channel": self.channel(),
            "instId": self.instrument().inst_id,
        })
    }
}

#[derive(Clone)]
pub struct OkxWsClient {
    ws_base_url: String,
}

impl Default for OkxWsClient {
    fn default() -> Self {
        Self::public()
    }
}

impl OkxWsClient {
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
        subscriptions: Vec<OkxWsSubscription>,
    ) -> Result<ReceiverStream<Result<Value>>> {
        let args = subscriptions
            .iter()
            .map(OkxWsSubscription::wire_arg)
            .collect::<Vec<_>>();
        let (ws, _) = connect_async(&self.ws_base_url)
            .await
            .with_context(|| format!("connect OKX websocket {}", self.ws_base_url))?;
        let (mut sink, mut stream) = ws.split();
        sink.send(Message::Text(
            json!({
                "op": "subscribe",
                "args": args,
            })
            .to_string(),
        ))
        .await
        .context("send OKX subscribe frame")?;

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                let result = match message {
                    Ok(Message::Text(text)) => serde_json::from_str::<Value>(&text)
                        .map_err(anyhow::Error::from)
                        .context("decode OKX WS text frame"),
                    Ok(Message::Binary(bytes)) => serde_json::from_slice::<Value>(&bytes)
                        .map_err(anyhow::Error::from)
                        .context("decode OKX WS binary frame"),
                    Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                    Ok(Message::Close(_)) => break,
                    Ok(_) => continue,
                    Err(error) => Err(anyhow!(error).context("read OKX WS frame")),
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
pub struct OkxCoverageConfig {
    pub include_ticker: bool,
    pub include_trades: bool,
    pub include_books5: bool,
}

impl OkxCoverageConfig {
    pub fn balanced() -> Self {
        Self {
            include_ticker: true,
            include_trades: true,
            include_books5: true,
        }
    }
}

pub fn build_public_subscriptions(
    instruments: &[OkxInstrumentId],
    config: &OkxCoverageConfig,
) -> Vec<OkxWsSubscription> {
    let mut subscriptions = Vec::new();
    for instrument in instruments {
        if config.include_ticker {
            subscriptions.push(OkxWsSubscription::Ticker {
                instrument: instrument.clone(),
            });
        }
        if config.include_trades {
            subscriptions.push(OkxWsSubscription::Trades {
                instrument: instrument.clone(),
            });
        }
        if config.include_books5 {
            subscriptions.push(OkxWsSubscription::Books5 {
                instrument: instrument.clone(),
            });
        }
    }
    subscriptions
}

pub fn covered_inst_ids(subscriptions: &[OkxWsSubscription]) -> BTreeSet<String> {
    subscriptions
        .iter()
        .map(|subscription| subscription.instrument().inst_id.clone())
        .collect()
}

pub async fn sample_ws_counts(
    subscriptions: Vec<OkxWsSubscription>,
    watch_duration: Duration,
) -> Result<BTreeMap<String, usize>> {
    let mut stream = OkxWsClient::public()
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
    let Some(arg) = value.get("arg") else {
        if let Some(event) = value.get("event").and_then(Value::as_str) {
            *counts.entry(event.to_string()).or_default() += 1;
        }
        return;
    };
    let channel = arg
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let inst_id = arg
        .get("instId")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    *counts.entry(format!("{channel}:{inst_id}")).or_default() += 1;
}

pub async fn public_acceptance_report(
    rest: &OkxPublicRestClient,
) -> Result<ExchangeAcceptanceReport> {
    let mut rest_checks = Vec::new();
    let mut ws_checks = Vec::new();
    let mut coverage = Vec::new();
    let mut plans = Vec::new();

    let spot_instruments = rest.instruments(OkxInstType::Spot).await?;
    let swap_instruments = rest.instruments(OkxInstType::Swap).await?;
    let spot_live = live_inst_ids_from_instruments(&spot_instruments)?;
    let swap_live = live_inst_ids_from_instruments(&swap_instruments)?;

    let spot_sample = OkxInstrumentId::spot(DEFAULT_SPOT_INST_ID);
    let swap_sample = OkxInstrumentId::swap(DEFAULT_SWAP_INST_ID);
    let spot_book = rest.order_book(&spot_sample, Some(5)).await?;
    let swap_book = rest.order_book(&swap_sample, Some(5)).await?;
    rest.ticker(&spot_sample).await?;
    rest.ticker(&swap_sample).await?;
    let spot_trades = rest.trades(&spot_sample, Some(5)).await?;
    let swap_trades = rest.trades(&swap_sample, Some(5)).await?;
    rest_checks.push(AcceptanceCheck::pass(
        "rest_matrix",
        format!(
            "spot_instruments={} swap_instruments={} spot_depth_bids={} swap_depth_bids={} spot_trades={} swap_trades={}",
            spot_live.len(),
            swap_live.len(),
            spot_book.bids.len(),
            swap_book.bids.len(),
            spot_trades.len(),
            swap_trades.len()
        ),
    ));
    rest_checks.push(AcceptanceCheck::pass(
        "instrument_identity",
        format!(
            "{}={} {}={}",
            spot_sample.inst_id, spot_sample.inst_type, swap_sample.inst_id, swap_sample.inst_type
        ),
    ));

    let spot_ticker_ids = inst_ids_from_tickers(rest.tickers(OkxInstType::Spot).await?);
    let swap_ticker_ids = inst_ids_from_tickers(rest.tickers(OkxInstType::Swap).await?);
    let spot_scope = identities_for_type(OkxInstType::Spot, &spot_ticker_ids);
    let swap_scope = identities_for_type(OkxInstType::Swap, &swap_ticker_ids);

    let spot_plan = build_public_subscriptions(&spot_scope, &OkxCoverageConfig::balanced());
    let swap_plan = build_public_subscriptions(&swap_scope, &OkxCoverageConfig::balanced());
    coverage.push(CoverageSummary::from_symbols(
        "spot_ticker_visible_ws_plan",
        spot_ticker_ids.clone(),
        covered_inst_ids(&spot_plan),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "swap_ticker_visible_ws_plan",
        swap_ticker_ids.clone(),
        covered_inst_ids(&swap_plan),
    ));
    coverage.push(CoverageSummary::from_symbols(
        "spot_tickers",
        spot_ticker_ids.clone(),
        spot_ticker_ids,
    ));
    coverage.push(CoverageSummary::from_symbols(
        "swap_tickers",
        swap_ticker_ids.clone(),
        swap_ticker_ids,
    ));
    plans.push(SubscriptionPlanSummary::new(
        "spot_ticker_visible_ws_plan",
        spot_plan.len(),
        200,
    ));
    plans.push(SubscriptionPlanSummary::new(
        "swap_ticker_visible_ws_plan",
        swap_plan.len(),
        200,
    ));
    ws_checks.push(AcceptanceCheck::pass(
        "ws_plan",
        "spot and swap plans cover the public ticker-visible scope",
    ));

    let live_sample_subscriptions = vec![
        OkxWsSubscription::Books5 {
            instrument: spot_sample,
        },
        OkxWsSubscription::Books5 {
            instrument: swap_sample,
        },
    ];
    match sample_ws_counts(live_sample_subscriptions, Duration::from_secs(8)).await {
        Ok(counts) => {
            let spot_seen = counts.get("books5:BTC-USDT").copied().unwrap_or_default();
            let swap_seen = counts
                .get("books5:BTC-USDT-SWAP")
                .copied()
                .unwrap_or_default();
            if spot_seen > 0 && swap_seen > 0 {
                ws_checks.push(AcceptanceCheck::pass(
                    "ws_live_books5_sample",
                    format!("books5 spot_events={spot_seen} swap_events={swap_seen}"),
                ));
            } else {
                ws_checks.push(AcceptanceCheck::fail(
                    "ws_live_books5_sample",
                    format!("missing books5 live sample counts={counts:?}"),
                ));
            }
        }
        Err(error) => ws_checks.push(AcceptanceCheck::fail(
            "ws_live_books5_sample",
            error.to_string(),
        )),
    }

    let has_failures = ws_checks
        .iter()
        .chain(rest_checks.iter())
        .any(|check| check.status == common::CheckStatus::Fail)
        || coverage.iter().any(|row| !row.is_complete());

    Ok(ExchangeAcceptanceReport {
        exchange: "okx".to_string(),
        crate_name: "okx".to_string(),
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
            "OKX instrument identity is instType plus instId; BTC-USDT and BTC-USDT-SWAP are distinct instruments.".to_string(),
            "Acceptance coverage is scoped to the public ticker-visible SPOT and SWAP universe, then REST instruments are recorded as live context.".to_string(),
            "Public no-key market-data readiness intentionally excludes private trading, account, signed request, balance, and order endpoints.".to_string(),
        ],
        live: true,
    })
}

fn live_inst_ids_from_instruments(instruments: &[OkxInstrument]) -> Result<Vec<OkxInstrumentId>> {
    let mut ids = instruments
        .iter()
        .filter(|instrument| instrument.is_live())
        .map(OkxInstrument::identity)
        .collect::<Result<Vec<_>>>()?;
    ids.sort();
    ids.dedup();
    Ok(ids)
}

fn inst_ids_from_tickers(tickers: Vec<OkxTicker>) -> Vec<String> {
    let mut ids = tickers
        .into_iter()
        .map(|ticker| ticker.inst_id)
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    ids
}

fn identities_for_type(inst_type: OkxInstType, inst_ids: &[String]) -> Vec<OkxInstrumentId> {
    inst_ids
        .iter()
        .cloned()
        .map(|inst_id| OkxInstrumentId::new(inst_type, inst_id))
        .collect()
}

#[derive(Default)]
pub struct OkxConnector;

impl MarketDataConnector for OkxConnector {
    fn exchange(&self) -> &'static str {
        "okx"
    }

    fn ws_endpoint(&self) -> &'static str {
        PUBLIC_WS_BASE_URL
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| {
                let instrument = OkxInstrumentId::infer_from_inst_id(item.symbol.clone());
                match &item.channel {
                    MarketDataChannel::Trades => OkxWsSubscription::Trades { instrument }.label(),
                    MarketDataChannel::OrderBook => {
                        OkxWsSubscription::Books5 { instrument }.label()
                    }
                    MarketDataChannel::Ticker => OkxWsSubscription::Ticker { instrument }.label(),
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
    fn instrument_identity_preserves_spot_and_swap() {
        let spot = OkxInstrumentId::infer_from_inst_id("BTC-USDT");
        let swap = OkxInstrumentId::infer_from_inst_id("BTC-USDT-SWAP");
        assert_eq!(spot.inst_type, OkxInstType::Spot);
        assert_eq!(swap.inst_type, OkxInstType::Swap);
        assert_ne!(spot, swap);
    }

    #[test]
    fn ws_args_match_okx_public_channel_shape() {
        let subscription = OkxWsSubscription::Books5 {
            instrument: OkxInstrumentId::spot("BTC-USDT"),
        };
        assert_eq!(subscription.label(), "books5:BTC-USDT");
        assert_eq!(
            subscription.wire_arg(),
            json!({"channel": "books5", "instId": "BTC-USDT"})
        );
    }

    #[test]
    fn balanced_plan_covers_each_instrument() {
        let instruments = vec![
            OkxInstrumentId::spot("BTC-USDT"),
            OkxInstrumentId::swap("BTC-USDT-SWAP"),
        ];
        let plan = build_public_subscriptions(&instruments, &OkxCoverageConfig::balanced());
        assert_eq!(
            covered_inst_ids(&plan),
            ["BTC-USDT".to_string(), "BTC-USDT-SWAP".to_string()]
                .into_iter()
                .collect()
        );
        assert_eq!(plan.len(), 6);
    }

    #[test]
    fn connector_maps_common_channels_to_okx_labels() {
        let topics = OkxConnector.build_subscriptions(&[
            Subscription {
                symbol: "BTC-USDT".to_string(),
                channel: MarketDataChannel::Trades,
            },
            Subscription {
                symbol: "BTC-USDT-SWAP".to_string(),
                channel: MarketDataChannel::Ticker,
            },
        ]);
        assert_eq!(topics, vec!["trades:BTC-USDT", "tickers:BTC-USDT-SWAP"]);
    }
}
