use crate::model::{
    FuturesContractInfo, FuturesDeal, FuturesDepthSnapshot, FuturesDepthStepSnapshot,
    FuturesEventContract, FuturesTicker, FuturesWsChannelEnvelope, FuturesWsFundingRatePoint,
    FuturesWsKline, FuturesWsPricePoint, MexcFuturesEnvelope, MexcWsAck, MexcWsSessionStart,
    OneOrMany,
};
use anyhow::{Context, Result, anyhow};
use flate2::read::GzDecoder;
use futures::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::io::Read;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, warn};

const DEFAULT_FUTURES_WS_ENDPOINT: &str = "wss://contract.mexc.com/edge";
const FUTURES_STREAM_CHANNEL_CAPACITY: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MexcFuturesKlineInterval {
    Min1,
    Min5,
    Min15,
    Min30,
    Min60,
    Hour4,
    Hour8,
    Day1,
    Week1,
    Month1,
}

impl MexcFuturesKlineInterval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Min1 => "Min1",
            Self::Min5 => "Min5",
            Self::Min15 => "Min15",
            Self::Min30 => "Min30",
            Self::Min60 => "Min60",
            Self::Hour4 => "Hour4",
            Self::Hour8 => "Hour8",
            Self::Day1 => "Day1",
            Self::Week1 => "Week1",
            Self::Month1 => "Month1",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![
            Self::Min1,
            Self::Min5,
            Self::Min15,
            Self::Min30,
            Self::Min60,
            Self::Hour4,
            Self::Hour8,
            Self::Day1,
            Self::Week1,
            Self::Month1,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MexcFuturesSubscription {
    Tickers,
    Ticker {
        symbol: String,
    },
    Deals {
        symbol: String,
    },
    DealsRaw {
        symbol: String,
    },
    Depth {
        symbol: String,
        compress: Option<bool>,
    },
    DepthStep {
        symbol: String,
        step: String,
    },
    DepthFull {
        symbol: String,
        limit: Option<u16>,
    },
    FundingRate {
        symbol: String,
    },
    IndexPrice {
        symbol: String,
    },
    FairPrice {
        symbol: String,
    },
    Kline {
        symbol: String,
        interval: MexcFuturesKlineInterval,
    },
    Contract,
    EventContract,
}

impl MexcFuturesSubscription {
    pub fn method(&self) -> &'static str {
        match self {
            Self::Tickers => "sub.tickers",
            Self::Ticker { .. } => "sub.ticker",
            Self::Deals { .. } => "sub.deal",
            Self::DealsRaw { .. } => "sub.deal",
            Self::Depth { .. } => "sub.depth",
            Self::DepthStep { .. } => "sub.depth.step",
            Self::DepthFull { .. } => "sub.depth.full",
            Self::FundingRate { .. } => "sub.funding.rate",
            Self::IndexPrice { .. } => "sub.index.price",
            Self::FairPrice { .. } => "sub.fair.price",
            Self::Kline { .. } => "sub.kline",
            Self::Contract => "sub.contract",
            Self::EventContract => "sub.event.contract",
        }
    }

    pub fn request(&self) -> Value {
        match self {
            Self::Tickers | Self::Contract | Self::EventContract => {
                json!({"method": self.method(), "gzip": false})
            }
            Self::Ticker { symbol }
            | Self::Deals { symbol }
            | Self::FundingRate { symbol }
            | Self::IndexPrice { symbol }
            | Self::FairPrice { symbol } => json!({
                "method": self.method(),
                "param": { "symbol": symbol },
                "gzip": false
            }),
            Self::Depth { symbol, compress } => {
                let mut param = json!({ "symbol": symbol });
                if let Some(compress) = compress {
                    param["compress"] = Value::Bool(*compress);
                }
                json!({
                    "method": self.method(),
                    "param": param,
                    "gzip": false
                })
            }
            Self::DepthFull { symbol, limit } => {
                let mut param = json!({ "symbol": symbol });
                if let Some(limit) = limit {
                    param["limit"] = json!(limit);
                }
                json!({
                    "method": self.method(),
                    "param": param,
                    "gzip": false
                })
            }
            Self::DealsRaw { symbol } => json!({
                "method": self.method(),
                "param": {
                    "symbol": symbol,
                    "compress": false
                },
                "gzip": false
            }),
            Self::DepthStep { symbol, step } => json!({
                "method": self.method(),
                "param": {
                    "symbol": symbol,
                    "step": step,
                },
                "gzip": false
            }),
            Self::Kline { symbol, interval } => json!({
                "method": self.method(),
                "param": {
                    "symbol": symbol,
                    "interval": interval.as_str(),
                },
                "gzip": false
            }),
        }
    }

    pub fn unsubscribe_request(&self) -> Value {
        match self {
            Self::Tickers | Self::Contract | Self::EventContract => {
                json!({ "method": self.method().replacen("sub.", "unsub.", 1) })
            }
            Self::Ticker { symbol }
            | Self::Deals { symbol }
            | Self::DealsRaw { symbol }
            | Self::Depth { symbol, .. }
            | Self::DepthFull { symbol, .. }
            | Self::FundingRate { symbol }
            | Self::IndexPrice { symbol }
            | Self::FairPrice { symbol } => json!({
                "method": self.method().replacen("sub.", "unsub.", 1),
                "param": { "symbol": symbol },
            }),
            Self::DepthStep { symbol, step } => json!({
                "method": self.method().replacen("sub.", "unsub.", 1),
                "param": {
                    "symbol": symbol,
                    "step": step,
                }
            }),
            Self::Kline { symbol, .. } => json!({
                "method": self.method().replacen("sub.", "unsub.", 1),
                "param": { "symbol": symbol },
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MexcFuturesWsClient {
    endpoint: String,
    reconnect_delay: Duration,
    ping_interval: Duration,
    session_max_age: Option<Duration>,
    auto_reconnect: bool,
}

impl Default for MexcFuturesWsClient {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_FUTURES_WS_ENDPOINT.to_string(),
            reconnect_delay: Duration::from_secs(3),
            ping_interval: Duration::from_secs(15),
            session_max_age: None,
            auto_reconnect: true,
        }
    }
}

impl MexcFuturesWsClient {
    pub fn endpoint(&self) -> &'static str {
        DEFAULT_FUTURES_WS_ENDPOINT
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    pub fn with_auto_reconnect(mut self, auto_reconnect: bool) -> Self {
        self.auto_reconnect = auto_reconnect;
        self
    }

    pub fn with_session_max_age(mut self, session_max_age: Duration) -> Self {
        self.session_max_age = Some(session_max_age);
        self
    }

    pub fn without_session_max_age(mut self) -> Self {
        self.session_max_age = None;
        self
    }

    pub fn session_max_age(&self) -> Option<Duration> {
        self.session_max_age
    }

    pub async fn connect(
        &self,
        subscriptions: Vec<MexcFuturesSubscription>,
    ) -> Result<ReceiverStream<Result<MexcFuturesWsMessage>>> {
        if subscriptions.is_empty() {
            return Err(anyhow!("at least one futures subscription is required"));
        }

        let (tx, rx) = mpsc::channel(FUTURES_STREAM_CHANNEL_CAPACITY);
        let endpoint = self.endpoint.clone();
        let reconnect_delay = self.reconnect_delay;
        let ping_interval = self.ping_interval;
        let session_max_age = self.session_max_age;
        let auto_reconnect = self.auto_reconnect;

        tokio::spawn(async move {
            let mut session_id = 0u64;
            loop {
                session_id += 1;
                let result = run_futures_connection(
                    &endpoint,
                    &subscriptions,
                    ping_interval,
                    session_max_age,
                    session_id,
                    session_id > 1,
                    tx.clone(),
                )
                .await;

                if tx.is_closed() {
                    break;
                }

                match result {
                    Ok(()) if !auto_reconnect => break,
                    Ok(()) => {
                        warn!("mexc futures websocket closed, reconnecting");
                    }
                    Err(error) if !auto_reconnect => {
                        let _ = tx.send(Err(error)).await;
                        break;
                    }
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                    }
                }

                if tx.is_closed() {
                    break;
                }
                tokio::time::sleep(reconnect_delay).await;
            }
        });

        Ok(ReceiverStream::new(rx))
    }
}

#[derive(Debug, Clone)]
pub enum MexcFuturesWsMessage {
    SessionStart(MexcWsSessionStart),
    Ack(MexcWsAck),
    Raw(Value),
    Tickers(MexcFuturesEnvelope<Vec<FuturesTicker>>),
    Ticker(MexcFuturesEnvelope<FuturesTicker>),
    Deals(MexcFuturesEnvelope<OneOrMany<FuturesDeal>>),
    Depth(MexcFuturesEnvelope<FuturesDepthSnapshot>),
    DepthStep(FuturesWsChannelEnvelope<FuturesDepthStepSnapshot>),
    DepthFull(MexcFuturesEnvelope<FuturesDepthSnapshot>),
    FundingRate(MexcFuturesEnvelope<FuturesWsFundingRatePoint>),
    IndexPrice(MexcFuturesEnvelope<FuturesWsPricePoint>),
    FairPrice(MexcFuturesEnvelope<FuturesWsPricePoint>),
    Kline(MexcFuturesEnvelope<FuturesWsKline>),
    Contract(MexcFuturesEnvelope<FuturesContractInfo>),
    EventContract(MexcFuturesEnvelope<FuturesEventContract>),
}

async fn run_futures_connection(
    endpoint: &str,
    subscriptions: &[MexcFuturesSubscription],
    ping_interval: Duration,
    session_max_age: Option<Duration>,
    session_id: u64,
    resumed: bool,
    tx: mpsc::Sender<Result<MexcFuturesWsMessage>>,
) -> Result<()> {
    let (mut socket, _) = connect_async(endpoint)
        .await
        .with_context(|| format!("connect to MEXC futures websocket {endpoint}"))?;

    for subscription in subscriptions {
        socket
            .send(Message::Text(subscription.request().to_string()))
            .await
            .with_context(|| format!("send futures subscription {}", subscription.method()))?;
    }

    if tx
        .send(Ok(MexcFuturesWsMessage::SessionStart(MexcWsSessionStart {
            session_id,
            resumed,
            subscription_count: subscriptions.len(),
        })))
        .await
        .is_err()
    {
        return Ok(());
    }

    let mut ping = tokio::time::interval(ping_interval);
    ping.tick().await;
    let rotation_deadline = session_max_age.map(tokio::time::sleep);
    tokio::pin!(rotation_deadline);

    loop {
        tokio::select! {
            _ = async {
                if let Some(deadline) = rotation_deadline.as_mut().as_pin_mut() {
                    deadline.await;
                } else {
                    futures::future::pending::<()>().await;
                }
            } => {
                warn!("mexc futures websocket session reached configured max age, rotating");
                return Ok(());
            }
            _ = ping.tick() => {
                socket
                    .send(Message::Text(json!({"method":"ping"}).to_string()))
                    .await
                    .context("send MEXC futures ping")?;
            }
            maybe_message = socket.next() => {
                let message = match maybe_message {
                    Some(message) => message.context("read MEXC futures websocket frame")?,
                    None => return Ok(()),
                };

                match message {
                    Message::Text(text) => {
                        debug!("mexc futures text frame: {text}");
                        let value: Value = serde_json::from_str(&text)
                            .context("decode MEXC futures text frame as JSON")?;
                        let parsed = parse_futures_value(value)?;
                        if tx.send(Ok(parsed)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Message::Binary(bytes) => {
                        let text = maybe_gzip_to_string(bytes)?;
                        let value: Value = serde_json::from_str(&text)
                            .context("decode MEXC futures binary payload as JSON")?;
                        let parsed = parse_futures_value(value)?;
                        if tx.send(Ok(parsed)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Message::Ping(payload) => {
                        socket.send(Message::Pong(payload)).await.ok();
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => return Ok(()),
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

fn maybe_gzip_to_string(bytes: Vec<u8>) -> Result<String> {
    if let Ok(text) = String::from_utf8(bytes.clone()) {
        return Ok(text);
    }

    let mut decoder = GzDecoder::new(bytes.as_slice());
    let mut text = String::new();
    decoder
        .read_to_string(&mut text)
        .context("gunzip MEXC futures frame")?;
    Ok(text)
}

fn parse_channel_payload<T: DeserializeOwned>(value: Value) -> Result<MexcFuturesEnvelope<T>> {
    serde_json::from_value(value).context("decode typed MEXC futures payload")
}

fn parse_channel_payload_or_raw<T, F>(
    channel: &str,
    value: Value,
    mapper: F,
) -> Result<MexcFuturesWsMessage>
where
    T: DeserializeOwned,
    F: FnOnce(MexcFuturesEnvelope<T>) -> MexcFuturesWsMessage,
{
    let raw = value.clone();
    match parse_channel_payload::<T>(value) {
        Ok(payload) => Ok(mapper(payload)),
        Err(error) => {
            warn!(
                "decode drift on MEXC futures {channel} payload, emitting raw frame instead: {error:#}"
            );
            Ok(MexcFuturesWsMessage::Raw(raw))
        }
    }
}

fn parse_futures_value(value: Value) -> Result<MexcFuturesWsMessage> {
    if value.get("code").is_some() || value.get("msg").is_some() {
        if let Ok(ack) = serde_json::from_value::<MexcWsAck>(value.clone()) {
            return Ok(MexcFuturesWsMessage::Ack(ack));
        }
    }

    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("MEXC futures frame missing channel"))?;

    if channel.starts_with("rs.") {
        if let Ok(ack) = serde_json::from_value::<MexcWsAck>(value.clone()) {
            return Ok(MexcFuturesWsMessage::Ack(ack));
        }
        return Ok(MexcFuturesWsMessage::Raw(value));
    }

    match channel {
        "push.tickers" => Ok(MexcFuturesWsMessage::Tickers(parse_channel_payload(value)?)),
        "push.ticker" => Ok(MexcFuturesWsMessage::Ticker(parse_channel_payload(value)?)),
        "push.deal" => Ok(MexcFuturesWsMessage::Deals(parse_channel_payload(value)?)),
        "push.depth" => Ok(MexcFuturesWsMessage::Depth(parse_channel_payload(value)?)),
        "push.depth.step" => Ok(MexcFuturesWsMessage::DepthStep(
            serde_json::from_value(value).context("decode typed MEXC futures payload")?,
        )),
        "push.depth.full" => Ok(MexcFuturesWsMessage::DepthFull(parse_channel_payload(
            value,
        )?)),
        "push.funding.rate" => Ok(MexcFuturesWsMessage::FundingRate(parse_channel_payload(
            value,
        )?)),
        "push.index.price" => Ok(MexcFuturesWsMessage::IndexPrice(parse_channel_payload(
            value,
        )?)),
        "push.fair.price" => Ok(MexcFuturesWsMessage::FairPrice(parse_channel_payload(
            value,
        )?)),
        "push.kline" => Ok(MexcFuturesWsMessage::Kline(parse_channel_payload(value)?)),
        "push.contract" => parse_channel_payload_or_raw::<FuturesContractInfo, _>(
            "push.contract",
            value,
            MexcFuturesWsMessage::Contract,
        ),
        "push.event.contract" => parse_channel_payload_or_raw::<FuturesEventContract, _>(
            "push.event.contract",
            value,
            MexcFuturesWsMessage::EventContract,
        ),
        _ => Ok(MexcFuturesWsMessage::Raw(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticker_request_shape_is_stable() {
        let request = MexcFuturesSubscription::Ticker {
            symbol: "BTC_USDT".to_string(),
        }
        .request();
        assert_eq!(request["method"], "sub.ticker");
        assert_eq!(request["param"]["symbol"], "BTC_USDT");
    }

    #[test]
    fn depth_request_can_disable_merge() {
        let request = MexcFuturesSubscription::Depth {
            symbol: "BTC_USDT".to_string(),
            compress: Some(false),
        }
        .request();
        assert_eq!(request["method"], "sub.depth");
        assert_eq!(request["param"]["symbol"], "BTC_USDT");
        assert_eq!(request["param"]["compress"], false);
    }

    #[test]
    fn kline_request_contains_interval() {
        let request = MexcFuturesSubscription::Kline {
            symbol: "BTC_USDT".to_string(),
            interval: MexcFuturesKlineInterval::Min1,
        }
        .request();
        assert_eq!(request["method"], "sub.kline");
        assert_eq!(request["param"]["interval"], "Min1");
    }

    #[test]
    fn depth_step_request_contains_step() {
        let request = MexcFuturesSubscription::DepthStep {
            symbol: "BTC_USDT".to_string(),
            step: "10".to_string(),
        }
        .request();
        assert_eq!(request["method"], "sub.depth.step");
        assert_eq!(request["param"]["step"], "10");
    }

    #[test]
    fn depth_full_request_can_set_limit() {
        let request = MexcFuturesSubscription::DepthFull {
            symbol: "BTC_USDT".to_string(),
            limit: Some(5),
        }
        .request();
        assert_eq!(request["method"], "sub.depth.full");
        assert_eq!(request["param"]["symbol"], "BTC_USDT");
        assert_eq!(request["param"]["limit"], 5);
    }

    #[test]
    fn raw_deals_request_disables_aggregation() {
        let request = MexcFuturesSubscription::DealsRaw {
            symbol: "BTC_USDT".to_string(),
        }
        .request();
        assert_eq!(request["method"], "sub.deal");
        assert_eq!(request["param"]["symbol"], "BTC_USDT");
        assert_eq!(request["param"]["compress"], false);
        assert_eq!(request["gzip"], false);
    }

    #[test]
    fn futures_rotation_is_disabled_by_default() {
        assert_eq!(MexcFuturesWsClient::default().session_max_age(), None);
    }

    #[test]
    fn funding_rate_payload_uses_rate_field() {
        let value = json!({
            "channel": "push.funding.rate",
            "data": {
                "rate": 0.001,
                "symbol": "BTC_USDT",
                "nextSettleTime": 1587442022003u64
            },
            "symbol": "BTC_USDT",
            "ts": 1587442022003u64
        });

        let parsed = parse_futures_value(value).expect("parse funding payload");
        let MexcFuturesWsMessage::FundingRate(event) = parsed else {
            panic!("expected funding rate event");
        };
        assert_eq!(event.data.symbol, "BTC_USDT");
        assert_eq!(event.data.rate, 0.001);
        assert_eq!(event.data.next_settle_time, Some(1587442022003u64));
    }

    #[test]
    fn contract_payload_parses_documented_fields() {
        let value: Value = serde_json::from_str(
            r#"{
              "channel": "push.contract",
              "data": {
                "amountScale": 4,
                "askLimitPriceRate": 0.2,
                "baseCoin": "CLO",
                "baseCoinName": "CLO",
                "bidLimitPriceRate": 0.2,
                "contractSize": 10,
                "depthStepList": ["0.0001", "0.001", "0.01", "0.1"],
                "displayName": "CLO_USDT永续",
                "displayNameEn": "CLO_USDT PERPETUAL",
                "feeRateMode": "NORMAL",
                "futureType": 1,
                "indexOrigin": ["MEXC_FUTURE", "MEXC", "KUCOIN"],
                "initialMarginRate": 0.02,
                "isHidden": false,
                "isHot": false,
                "isNew": false,
                "liquidationFeeRate": 0.0002,
                "limitMaxVol": 9000,
                "maintenanceMarginRate": 0.01,
                "makerFeeRate": 0,
                "maxLeverage": 50,
                "maxNumOrders": [200, 50],
                "maxVol": 9000,
                "minLeverage": 1,
                "minVol": 1,
                "openingCountdownOption": 1,
                "openingTime": 1760440200000,
                "positionOpenType": 3,
                "priceCoefficientVariation": 0.4,
                "priceScale": 4,
                "priceUnit": 0.0001,
                "quoteCoin": "USDT",
                "quoteCoinName": "USDT",
                "riskBaseVol": 9000,
                "riskBaseVolLong": 9000,
                "riskBaseVolShort": 9000,
                "riskIncrImr": 0.005,
                "riskIncrMmr": 0.005,
                "riskIncrVol": 9000,
                "riskIncrVolLong": 9000,
                "riskIncrVolShort": 9000,
                "riskLevelLimit": 1,
                "riskLimitMode": "INCREASE",
                "riskLimitType": "BY_VOLUME",
                "riskLongShortSwitch": 0,
                "settleCoin": "USDT",
                "state": 0,
                "symbol": "CLO_USDT",
                "takerFeeRate": 0.0002,
                "tieredAppointContract": false,
                "tieredDealAmount": 0.0,
                "tieredEffectiveDay": 0,
                "tieredExcludeContractId": false,
                "tieredExcludeZeroFee": false,
                "triggerProtect": 0.1,
                "volScale": 0,
                "volUnit": 1
              },
              "symbol": "CLO_USDT",
              "ts": 1760942212002
            }"#,
        )
        .expect("decode contract sample json");

        let parsed = parse_futures_value(value).expect("parse contract payload");
        let MexcFuturesWsMessage::Contract(event) = parsed else {
            panic!("expected contract event");
        };
        assert_eq!(event.data.symbol, "CLO_USDT");
        assert_eq!(
            event
                .data
                .fee_rate_mode
                .as_ref()
                .map(|value| value.as_str_lossy()),
            Some("NORMAL".to_string())
        );
        assert_eq!(event.data.max_num_orders, vec![200, 50]);
        assert_eq!(event.data.risk_base_vol_long, Some(9000.0));
        assert_eq!(event.data.tiered_exclude_zero_fee, Some(false));
    }

    #[test]
    fn event_contract_payload_parses_documented_fields() {
        let value = json!({
            "channel": "push.event.contract",
            "data": {
                "contractId": "123",
                "symbol": "BTC_USDT",
                "baseCoin": "BTC",
                "quoteCoin": "USDT",
                "baseCoinName": "Bitcoin",
                "quoteCoinName": "Tether",
                "settleCoin": "USDT",
                "baseCoinIconUrl": "https://example.com/btc.png",
                "investMinAmount": 10.0,
                "investMaxAmount": 1000.0,
                "amountScale": 4,
                "payRateScale": 6,
                "indexPriceScale": 2,
                "availableScale": 2
            },
            "symbol": "BTC_USDT",
            "ts": 1760942212002u64
        });

        let parsed = parse_futures_value(value).expect("parse event contract payload");
        let MexcFuturesWsMessage::EventContract(event) = parsed else {
            panic!("expected event contract event");
        };
        assert_eq!(event.data.symbol, "BTC_USDT");
        assert_eq!(event.data.contract_id.as_str_lossy(), "123");
        assert_eq!(event.data.amount_scale, Some(4));
        assert_eq!(event.data.available_scale, Some(2));
    }

    #[test]
    fn malformed_contract_payload_falls_back_to_raw() {
        let value = json!({
            "channel": "push.contract",
            "data": [{"symbol": "BTC_USDT"}],
            "symbol": "BTC_USDT",
            "ts": 1
        });

        let parsed = parse_futures_value(value).expect("parse malformed contract payload");
        let MexcFuturesWsMessage::Raw(raw) = parsed else {
            panic!("expected raw fallback");
        };
        assert_eq!(raw["channel"], "push.contract");
    }

    #[test]
    fn malformed_event_contract_payload_falls_back_to_raw() {
        let value = json!({
            "channel": "push.event.contract",
            "data": [{"symbol": "BTC_USDT"}],
            "symbol": "BTC_USDT",
            "ts": 1
        });

        let parsed = parse_futures_value(value).expect("parse malformed event contract payload");
        let MexcFuturesWsMessage::Raw(raw) = parsed else {
            panic!("expected raw fallback");
        };
        assert_eq!(raw["channel"], "push.event.contract");
    }
}
