use crate::model::{MexcSpotEnvelope, MexcWsAck, MexcWsSessionStart};
use crate::spot_proto::{PushDataV3ApiWrapper, push_data_v3_api_wrapper::Body};
use anyhow::{Context, Result, anyhow};
use futures::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, warn};

const DEFAULT_SPOT_WS_ENDPOINT: &str = "wss://wbs-api.mexc.com/ws";
const SPOT_SUBSCRIPTION_LIMIT: usize = 30;
const SPOT_STREAM_CHANNEL_CAPACITY: usize = 128;
const DEFAULT_SPOT_SESSION_MAX_AGE: Duration = Duration::from_secs(23 * 60 * 60 + 55 * 60);
const DEFAULT_SPOT_SESSION_ROTATION_SPREAD: Duration = Duration::from_secs(4 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MexcSpotUpdateSpeed {
    Ms10,
    Ms100,
}

impl MexcSpotUpdateSpeed {
    pub fn as_suffix(self) -> &'static str {
        match self {
            Self::Ms10 => "@10ms",
            Self::Ms100 => "@100ms",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MexcSpotKlineInterval {
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

impl MexcSpotKlineInterval {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MexcTimezone(&'static str);

impl MexcTimezone {
    pub const ROLLING_24H: Self = Self("24H");
    pub const UTC_MINUS_10: Self = Self("UTC-10");
    pub const UTC_MINUS_8: Self = Self("UTC-8");
    pub const UTC_MINUS_7: Self = Self("UTC-7");
    pub const UTC_MINUS_6: Self = Self("UTC-6");
    pub const UTC_MINUS_5: Self = Self("UTC-5");
    pub const UTC_MINUS_4: Self = Self("UTC-4");
    pub const UTC_MINUS_3: Self = Self("UTC-3");
    pub const UTC_PLUS_0: Self = Self("UTC+0");
    pub const UTC_PLUS_1: Self = Self("UTC+1");
    pub const UTC_PLUS_2: Self = Self("UTC+2");
    pub const UTC_PLUS_3: Self = Self("UTC+3");
    pub const UTC_PLUS_4: Self = Self("UTC+4");
    pub const UTC_PLUS_4_30: Self = Self("UTC+4:30");
    pub const UTC_PLUS_5: Self = Self("UTC+5");
    pub const UTC_PLUS_5_30: Self = Self("UTC+5:30");
    pub const UTC_PLUS_6: Self = Self("UTC+6");
    pub const UTC_PLUS_7: Self = Self("UTC+7");
    pub const UTC_PLUS_8: Self = Self("UTC+8");
    pub const UTC_PLUS_9: Self = Self("UTC+9");
    pub const UTC_PLUS_10: Self = Self("UTC+10");
    pub const UTC_PLUS_11: Self = Self("UTC+11");
    pub const UTC_PLUS_12: Self = Self("UTC+12");
    pub const UTC_PLUS_12_45: Self = Self("UTC+12:45");
    pub const UTC_PLUS_13: Self = Self("UTC+13");

    pub const fn new(raw: &'static str) -> Self {
        Self(raw)
    }

    pub fn as_str(self) -> &'static str {
        self.0
    }

    pub fn all() -> Vec<Self> {
        vec![
            Self::ROLLING_24H,
            Self::UTC_MINUS_10,
            Self::UTC_MINUS_8,
            Self::UTC_MINUS_7,
            Self::UTC_MINUS_6,
            Self::UTC_MINUS_5,
            Self::UTC_MINUS_4,
            Self::UTC_MINUS_3,
            Self::UTC_PLUS_0,
            Self::UTC_PLUS_1,
            Self::UTC_PLUS_2,
            Self::UTC_PLUS_3,
            Self::UTC_PLUS_4,
            Self::UTC_PLUS_4_30,
            Self::UTC_PLUS_5,
            Self::UTC_PLUS_5_30,
            Self::UTC_PLUS_6,
            Self::UTC_PLUS_7,
            Self::UTC_PLUS_8,
            Self::UTC_PLUS_9,
            Self::UTC_PLUS_10,
            Self::UTC_PLUS_11,
            Self::UTC_PLUS_12,
            Self::UTC_PLUS_12_45,
            Self::UTC_PLUS_13,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MexcSpotSubscription {
    AggTrades {
        symbol: String,
        speed: MexcSpotUpdateSpeed,
    },
    IncreaseDepth {
        symbol: String,
    },
    IncreaseDepthBatch {
        symbol: String,
    },
    AggDepth {
        symbol: String,
        speed: MexcSpotUpdateSpeed,
    },
    LimitDepth {
        symbol: String,
        level: u16,
    },
    BookTicker {
        symbol: String,
        speed: MexcSpotUpdateSpeed,
    },
    BookTickerBatch {
        symbol: String,
    },
    AggBookTicker {
        symbol: String,
    },
    Kline {
        symbol: String,
        interval: MexcSpotKlineInterval,
    },
    MiniTicker {
        symbol: String,
        timezone: MexcTimezone,
    },
    MiniTickers {
        timezone: MexcTimezone,
    },
}

impl MexcSpotSubscription {
    pub fn channel(&self) -> String {
        match self {
            Self::AggTrades { symbol, speed } => {
                format!(
                    "spot@public.aggre.deals.v3.api.pb{}@{symbol}",
                    speed.as_suffix()
                )
            }
            Self::IncreaseDepth { symbol } => {
                format!("spot@public.increase.depth.v3.api.pb@{symbol}")
            }
            Self::IncreaseDepthBatch { symbol } => {
                format!("spot@public.increase.depth.batch.v3.api.pb@{symbol}")
            }
            Self::AggDepth { symbol, speed } => {
                format!(
                    "spot@public.aggre.depth.v3.api.pb{}@{symbol}",
                    speed.as_suffix()
                )
            }
            Self::LimitDepth { symbol, level } => {
                format!("spot@public.limit.depth.v3.api.pb@{symbol}@{level}")
            }
            Self::BookTicker { symbol, speed } => {
                format!(
                    "spot@public.aggre.bookTicker.v3.api.pb{}@{symbol}",
                    speed.as_suffix()
                )
            }
            Self::BookTickerBatch { symbol } => {
                format!("spot@public.bookTicker.batch.v3.api.pb@{symbol}")
            }
            Self::AggBookTicker { symbol } => {
                format!("spot@public.bookTicker.v3.api.pb@{symbol}")
            }
            Self::Kline { symbol, interval } => {
                format!("spot@public.kline.v3.api.pb@{symbol}@{}", interval.as_str())
            }
            Self::MiniTicker { symbol, timezone } => format!(
                "spot@public.miniTicker.v3.api.pb@{symbol}@{}",
                timezone.as_str()
            ),
            Self::MiniTickers { timezone } => {
                format!("spot@public.miniTickers.v3.api.pb@{}", timezone.as_str())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MexcSpotWsClient {
    endpoint: String,
    reconnect_delay: Duration,
    ping_interval: Duration,
    session_max_age: Option<Duration>,
    session_rotation_spread: Option<Duration>,
    auto_reconnect: bool,
}

impl Default for MexcSpotWsClient {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_SPOT_WS_ENDPOINT.to_string(),
            reconnect_delay: Duration::from_secs(3),
            ping_interval: Duration::from_secs(15),
            session_max_age: Some(DEFAULT_SPOT_SESSION_MAX_AGE),
            session_rotation_spread: Some(DEFAULT_SPOT_SESSION_ROTATION_SPREAD),
            auto_reconnect: true,
        }
    }
}

impl MexcSpotWsClient {
    pub fn endpoint(&self) -> &'static str {
        DEFAULT_SPOT_WS_ENDPOINT
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

    pub fn with_session_rotation_spread(mut self, session_rotation_spread: Duration) -> Self {
        self.session_rotation_spread = Some(session_rotation_spread);
        self
    }

    pub fn without_session_rotation_spread(mut self) -> Self {
        self.session_rotation_spread = None;
        self
    }

    pub fn session_rotation_spread(&self) -> Option<Duration> {
        self.session_rotation_spread
    }

    pub async fn connect(
        &self,
        subscriptions: Vec<MexcSpotSubscription>,
    ) -> Result<ReceiverStream<Result<MexcSpotWsMessage>>> {
        self.connect_with_session_max_age(subscriptions, self.session_max_age)
            .await
    }

    async fn connect_with_session_max_age(
        &self,
        subscriptions: Vec<MexcSpotSubscription>,
        session_max_age: Option<Duration>,
    ) -> Result<ReceiverStream<Result<MexcSpotWsMessage>>> {
        if subscriptions.is_empty() {
            return Err(anyhow!("at least one spot subscription is required"));
        }
        if subscriptions.len() > SPOT_SUBSCRIPTION_LIMIT {
            return Err(anyhow!(
                "spot subscription shard exceeds documented limit of {SPOT_SUBSCRIPTION_LIMIT}"
            ));
        }

        let (tx, rx) = mpsc::channel(SPOT_STREAM_CHANNEL_CAPACITY);
        let endpoint = self.endpoint.clone();
        let reconnect_delay = self.reconnect_delay;
        let ping_interval = self.ping_interval;
        let auto_reconnect = self.auto_reconnect;

        tokio::spawn(async move {
            let mut session_id = 0u64;
            loop {
                session_id += 1;
                let result = run_spot_connection(
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
                        warn!("mexc spot websocket closed, reconnecting");
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

    pub async fn connect_sharded(
        &self,
        subscriptions: Vec<MexcSpotSubscription>,
    ) -> Result<Vec<ReceiverStream<Result<MexcSpotWsMessage>>>> {
        let mut streams = Vec::new();
        let shards = shard_spot_subscriptions(subscriptions, SPOT_SUBSCRIPTION_LIMIT);
        let shard_count = shards.len();
        for (index, shard) in shards.into_iter().enumerate() {
            let effective_session_max_age = effective_session_max_age_for_shard(
                self.session_max_age,
                self.session_rotation_spread,
                index,
                shard_count,
            );
            streams.push(
                self.connect_with_session_max_age(shard, effective_session_max_age)
                    .await?,
            );
        }
        Ok(streams)
    }
}

#[derive(Debug, Clone)]
pub enum MexcSpotWsMessage {
    SessionStart(MexcWsSessionStart),
    Ack(MexcWsAck),
    RawText(Value),
    AggTrades(MexcSpotEnvelope<crate::spot_proto::PublicAggreDealsV3Api>),
    IncreaseDepth(MexcSpotEnvelope<crate::spot_proto::PublicIncreaseDepthsV3Api>),
    IncreaseDepthBatch(MexcSpotEnvelope<crate::spot_proto::PublicIncreaseDepthsBatchV3Api>),
    AggDepth(MexcSpotEnvelope<crate::spot_proto::PublicAggreDepthsV3Api>),
    LimitDepth(MexcSpotEnvelope<crate::spot_proto::PublicLimitDepthsV3Api>),
    BookTicker(MexcSpotEnvelope<crate::spot_proto::PublicBookTickerV3Api>),
    BookTickerBatch(MexcSpotEnvelope<crate::spot_proto::PublicBookTickerBatchV3Api>),
    AggBookTicker(MexcSpotEnvelope<crate::spot_proto::PublicAggreBookTickerV3Api>),
    Kline(MexcSpotEnvelope<crate::spot_proto::PublicSpotKlineV3Api>),
    MiniTicker(MexcSpotEnvelope<crate::spot_proto::PublicMiniTickerV3Api>),
    MiniTickers(MexcSpotEnvelope<crate::spot_proto::PublicMiniTickersV3Api>),
}

fn shard_spot_subscriptions(
    subscriptions: Vec<MexcSpotSubscription>,
    per_connection_limit: usize,
) -> Vec<Vec<MexcSpotSubscription>> {
    subscriptions
        .chunks(per_connection_limit)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn effective_session_max_age_for_shard(
    session_max_age: Option<Duration>,
    session_rotation_spread: Option<Duration>,
    shard_index: usize,
    shard_count: usize,
) -> Option<Duration> {
    let session_max_age = session_max_age?;
    if shard_count <= 1 {
        return Some(session_max_age);
    }

    let Some(spread) = session_rotation_spread else {
        return Some(session_max_age);
    };
    let capped_spread = std::cmp::min(
        spread,
        session_max_age.saturating_sub(Duration::from_secs(1)),
    );
    if capped_spread.is_zero() {
        return Some(session_max_age);
    }

    let earliest = session_max_age.saturating_sub(capped_spread);
    let steps = (shard_count - 1) as u128;
    let offset_nanos = capped_spread.as_nanos() * shard_index as u128 / steps;
    Some(earliest + Duration::from_nanos(offset_nanos as u64))
}

async fn run_spot_connection(
    endpoint: &str,
    subscriptions: &[MexcSpotSubscription],
    ping_interval: Duration,
    session_max_age: Option<Duration>,
    session_id: u64,
    resumed: bool,
    tx: mpsc::Sender<Result<MexcSpotWsMessage>>,
) -> Result<()> {
    let (mut socket, _) = connect_async(endpoint)
        .await
        .with_context(|| format!("connect to MEXC spot websocket {endpoint}"))?;

    let params: Vec<String> = subscriptions
        .iter()
        .map(MexcSpotSubscription::channel)
        .collect();
    let subscribe = json!({
        "method": "SUBSCRIPTION",
        "params": params,
        "id": 1u64,
    });

    socket
        .send(Message::Text(subscribe.to_string()))
        .await
        .context("send MEXC spot subscription")?;

    if tx
        .send(Ok(MexcSpotWsMessage::SessionStart(MexcWsSessionStart {
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
                warn!("mexc spot websocket session reached max age, rotating");
                return Ok(());
            }
            _ = ping.tick() => {
                socket
                    .send(Message::Text(json!({"method":"PING"}).to_string()))
                    .await
                    .context("send MEXC spot ping")?;
            }
            maybe_message = socket.next() => {
                let message = match maybe_message {
                    Some(message) => message.context("read MEXC spot websocket frame")?,
                    None => return Ok(()),
                };

                match message {
                    Message::Text(text) => {
                        debug!("mexc spot text frame: {text}");
                        let value: Value = serde_json::from_str(&text)
                            .context("decode MEXC spot text frame as JSON")?;
                        if let Ok(ack) = serde_json::from_value::<MexcWsAck>(value.clone()) {
                            if tx.send(Ok(MexcSpotWsMessage::Ack(ack))).await.is_err() {
                                return Ok(());
                            }
                        } else {
                            if tx.send(Ok(MexcSpotWsMessage::RawText(value))).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Message::Binary(bytes) => {
                        let wrapper = PushDataV3ApiWrapper::decode(bytes.as_ref())
                            .context("decode MEXC spot protobuf frame")?;
                        let event = decode_spot_wrapper(wrapper)?;
                        if tx.send(Ok(event)).await.is_err() {
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

fn envelope<T>(wrapper: PushDataV3ApiWrapper, data: T) -> MexcSpotEnvelope<T> {
    MexcSpotEnvelope {
        channel: wrapper.channel,
        symbol: wrapper.symbol,
        create_time: wrapper.create_time,
        send_time: wrapper.send_time,
        data,
    }
}

fn decode_spot_wrapper(wrapper: PushDataV3ApiWrapper) -> Result<MexcSpotWsMessage> {
    let body = if let Some(body) = wrapper.body.clone() {
        body
    } else {
        return Ok(MexcSpotWsMessage::RawText(json!({
            "channel": wrapper.channel,
            "symbol": wrapper.symbol,
            "createTime": wrapper.create_time,
            "sendTime": wrapper.send_time,
            "note": "protobuf wrapper without body"
        })));
    };

    let channel = wrapper.channel.clone();
    match body {
        Body::PublicAggreDeals(data) => Ok(MexcSpotWsMessage::AggTrades(envelope(wrapper, data))),
        Body::PublicIncreaseDepths(data) => {
            Ok(MexcSpotWsMessage::IncreaseDepth(envelope(wrapper, data)))
        }
        Body::PublicIncreaseDepthsBatch(data) => Ok(MexcSpotWsMessage::IncreaseDepthBatch(
            envelope(wrapper, data),
        )),
        Body::PublicAggreDepths(data) => Ok(MexcSpotWsMessage::AggDepth(envelope(wrapper, data))),
        Body::PublicLimitDepths(data) => Ok(MexcSpotWsMessage::LimitDepth(envelope(wrapper, data))),
        Body::PublicBookTicker(data) => {
            if is_spot_agg_book_ticker_channel(&channel) {
                Ok(MexcSpotWsMessage::AggBookTicker(envelope(
                    wrapper,
                    promote_book_ticker_to_aggre(data),
                )))
            } else {
                Ok(MexcSpotWsMessage::BookTicker(envelope(wrapper, data)))
            }
        }
        Body::PublicBookTickerBatch(data) => {
            Ok(MexcSpotWsMessage::BookTickerBatch(envelope(wrapper, data)))
        }
        Body::PublicAggreBookTicker(data) => {
            if is_spot_raw_book_ticker_channel(&channel) {
                Ok(MexcSpotWsMessage::BookTicker(envelope(
                    wrapper,
                    demote_aggre_book_ticker_to_book(data),
                )))
            } else {
                Ok(MexcSpotWsMessage::AggBookTicker(envelope(wrapper, data)))
            }
        }
        Body::PublicSpotKline(data) => Ok(MexcSpotWsMessage::Kline(envelope(wrapper, data))),
        Body::PublicMiniTicker(data) => Ok(MexcSpotWsMessage::MiniTicker(envelope(wrapper, data))),
        Body::PublicMiniTickers(data) => {
            Ok(MexcSpotWsMessage::MiniTickers(envelope(wrapper, data)))
        }
    }
}

fn is_spot_agg_book_ticker_channel(channel: &str) -> bool {
    channel.contains("spot@public.aggre.bookTicker.v3.api.pb")
}

fn is_spot_raw_book_ticker_channel(channel: &str) -> bool {
    channel.contains("spot@public.bookTicker.v3.api.pb@") && !channel.contains(".batch.")
}

fn promote_book_ticker_to_aggre(
    data: crate::spot_proto::PublicBookTickerV3Api,
) -> crate::spot_proto::PublicAggreBookTickerV3Api {
    crate::spot_proto::PublicAggreBookTickerV3Api {
        bid_price: data.bid_price,
        bid_quantity: data.bid_quantity,
        ask_price: data.ask_price,
        ask_quantity: data.ask_quantity,
        version: String::new(),
        last_order_create_time: 0,
    }
}

fn demote_aggre_book_ticker_to_book(
    data: crate::spot_proto::PublicAggreBookTickerV3Api,
) -> crate::spot_proto::PublicBookTickerV3Api {
    crate::spot_proto::PublicBookTickerV3Api {
        bid_price: data.bid_price,
        bid_quantity: data.bid_quantity,
        ask_price: data.ask_price,
        ask_quantity: data.ask_quantity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_builders_match_documented_shapes() {
        let trade = MexcSpotSubscription::AggTrades {
            symbol: "BTCUSDT".to_string(),
            speed: MexcSpotUpdateSpeed::Ms100,
        };
        assert_eq!(
            trade.channel(),
            "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT"
        );

        let kline = MexcSpotSubscription::Kline {
            symbol: "BTCUSDT".to_string(),
            interval: MexcSpotKlineInterval::Min1,
        };
        assert_eq!(kline.channel(), "spot@public.kline.v3.api.pb@BTCUSDT@Min1");

        let increase = MexcSpotSubscription::IncreaseDepthBatch {
            symbol: "BTCUSDT".to_string(),
        };
        assert_eq!(
            increase.channel(),
            "spot@public.increase.depth.batch.v3.api.pb@BTCUSDT"
        );
    }

    #[test]
    fn sharding_respects_documented_limit() {
        let subscriptions = (0..65)
            .map(|index| MexcSpotSubscription::AggTrades {
                symbol: format!("SYM{index}"),
                speed: MexcSpotUpdateSpeed::Ms100,
            })
            .collect::<Vec<_>>();
        let shards = shard_spot_subscriptions(subscriptions, 30);
        assert_eq!(shards.len(), 3);
        assert_eq!(shards[0].len(), 30);
        assert_eq!(shards[1].len(), 30);
        assert_eq!(shards[2].len(), 5);
    }

    #[test]
    fn default_session_rotation_tracks_documented_24h_limit() {
        assert_eq!(
            MexcSpotWsClient::default().session_max_age(),
            Some(DEFAULT_SPOT_SESSION_MAX_AGE)
        );
        assert_eq!(
            MexcSpotWsClient::default().session_rotation_spread(),
            Some(DEFAULT_SPOT_SESSION_ROTATION_SPREAD)
        );
    }

    #[test]
    fn shard_rotation_spreads_effective_max_age_across_window() {
        let base = Duration::from_secs(10);
        let spread = Duration::from_secs(4);
        assert_eq!(
            effective_session_max_age_for_shard(Some(base), Some(spread), 0, 3),
            Some(Duration::from_secs(6))
        );
        assert_eq!(
            effective_session_max_age_for_shard(Some(base), Some(spread), 1, 3),
            Some(Duration::from_secs(8))
        );
        assert_eq!(
            effective_session_max_age_for_shard(Some(base), Some(spread), 2, 3),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn decode_spot_wrapper_uses_channel_to_promote_book_ticker_to_aggre() {
        let wrapper = PushDataV3ApiWrapper {
            channel: "spot@public.aggre.bookTicker.v3.api.pb@100ms@BTCUSDT".to_string(),
            body: Some(Body::PublicBookTicker(
                crate::spot_proto::PublicBookTickerV3Api {
                    bid_price: "1".to_string(),
                    bid_quantity: "2".to_string(),
                    ask_price: "3".to_string(),
                    ask_quantity: "4".to_string(),
                },
            )),
            symbol: Some("BTCUSDT".to_string()),
            symbol_id: None,
            create_time: None,
            send_time: None,
        };

        let message = decode_spot_wrapper(wrapper).expect("decode");
        assert!(matches!(message, MexcSpotWsMessage::AggBookTicker(_)));
    }

    #[test]
    fn decode_spot_wrapper_uses_channel_to_demote_aggre_book_ticker_to_raw() {
        let wrapper = PushDataV3ApiWrapper {
            channel: "spot@public.bookTicker.v3.api.pb@BTCUSDT".to_string(),
            body: Some(Body::PublicAggreBookTicker(
                crate::spot_proto::PublicAggreBookTickerV3Api {
                    bid_price: "1".to_string(),
                    bid_quantity: "2".to_string(),
                    ask_price: "3".to_string(),
                    ask_quantity: "4".to_string(),
                    version: "5".to_string(),
                    last_order_create_time: 6,
                },
            )),
            symbol: Some("BTCUSDT".to_string()),
            symbol_id: None,
            create_time: None,
            send_time: None,
        };

        let message = decode_spot_wrapper(wrapper).expect("decode");
        assert!(matches!(message, MexcSpotWsMessage::BookTicker(_)));
    }
}
