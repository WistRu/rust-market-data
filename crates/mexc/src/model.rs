use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type JsonMap = BTreeMap<String, Value>;
pub type SpotKlineRecord = Vec<Value>;
pub type FuturesDepthLevel = (Value, Value, Value);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MexcWsSessionStart {
    pub session_id: u64,
    pub resumed: bool,
    pub subscription_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum MexcFlexibleString {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl MexcFlexibleString {
    pub fn as_str_lossy(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Int(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::Bool(value) => value.to_string(),
        }
    }
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

    pub fn first(&self) -> Option<&T> {
        match self {
            Self::One(item) => Some(item),
            Self::Many(items) => items.first(),
        }
    }

    pub fn last(&self) -> Option<&T> {
        match self {
            Self::One(item) => Some(item),
            Self::Many(items) => items.last(),
        }
    }
}

impl<T: Clone> OneOrMany<T> {
    pub fn to_vec(&self) -> Vec<T> {
        match self {
            Self::One(item) => vec![item.clone()],
            Self::Many(items) => items.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SpotRateLimit {
    pub rate_limit_type: String,
    pub interval: String,
    pub interval_num: u64,
    pub limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SpotExchangeInfo {
    pub timezone: String,
    pub server_time: u64,
    #[serde(default)]
    pub rate_limits: Vec<SpotRateLimit>,
    #[serde(default)]
    pub exchange_filters: Vec<Value>,
    #[serde(default)]
    pub symbols: Vec<SpotSymbolInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SpotSymbolInfo {
    pub symbol: String,
    pub status: Option<String>,
    pub base_asset: Option<String>,
    pub base_asset_precision: Option<u64>,
    pub quote_asset: Option<String>,
    pub quote_precision: Option<u64>,
    pub quote_asset_precision: Option<u64>,
    pub base_commission_precision: Option<u64>,
    pub quote_commission_precision: Option<u64>,
    #[serde(default)]
    pub order_types: Vec<String>,
    pub is_spot_trading_allowed: Option<bool>,
    pub is_margin_trading_allowed: Option<bool>,
    pub quote_amount_precision: Option<String>,
    pub base_size_precision: Option<String>,
    #[serde(default)]
    pub permissions: Vec<String>,
    #[serde(default)]
    pub filters: Vec<Value>,
    pub max_quote_amount: Option<String>,
    pub maker_commission: Option<String>,
    pub taker_commission: Option<String>,
    pub quote_amount_precision_market: Option<String>,
    pub max_quote_amount_market: Option<String>,
    pub full_name: Option<String>,
    pub trade_side_type: Option<u64>,
    pub contract_address: Option<String>,
    #[serde(default)]
    pub concept_plate_ids: Vec<Value>,
    pub st: Option<bool>,
    #[serde(flatten)]
    pub extra: JsonMap,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpotServerTime {
    #[serde(rename = "serverTime")]
    pub server_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpotDefaultSymbolsResponse {
    pub code: Option<i64>,
    #[serde(default)]
    pub data: Vec<String>,
    pub msg: Option<String>,
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SpotOfflineSymbol {
    pub symbol: String,
    pub state: i64,
    pub offline_time: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpotOfflineSymbolsResponse {
    #[serde(default)]
    pub data: Vec<SpotOfflineSymbol>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpotHistoricalDataKind {
    Kline,
    Trades,
}

impl SpotHistoricalDataKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Kline => "kline",
            Self::Trades => "trades",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpotHistoricalArchivePeriod {
    Daily,
    Monthly,
}

impl SpotHistoricalArchivePeriod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::Monthly => "monthly",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpotHistoricalKlineInterval {
    Day1,
    Hour8,
    Hour4,
    Min60,
    Min30,
    Min15,
    Min5,
    Week1,
    Month1,
}

impl SpotHistoricalKlineInterval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Day1 => "Day1",
            Self::Hour8 => "Hour8",
            Self::Hour4 => "Hour4",
            Self::Min60 => "Min60",
            Self::Min30 => "Min30",
            Self::Min15 => "Min15",
            Self::Min5 => "Min5",
            Self::Week1 => "Week1",
            Self::Month1 => "Month1",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SpotHistoricalDownloadFile {
    pub file_name: String,
    pub masked_url: String,
    pub last_modified: String,
    pub file_size: u64,
}

impl SpotHistoricalDownloadFile {
    pub fn inferred_symbol(&self) -> Option<String> {
        self.file_name
            .split_once('-')
            .map(|(symbol, _)| symbol.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum SpotHistoricalDownloadNode {
    Directory(String),
    File(SpotHistoricalDownloadFile),
}

impl SpotHistoricalDownloadNode {
    pub fn into_directory(self) -> Option<String> {
        match self {
            Self::Directory(value) => Some(value),
            Self::File(_) => None,
        }
    }

    pub fn into_file(self) -> Option<SpotHistoricalDownloadFile> {
        match self {
            Self::Directory(_) => None,
            Self::File(value) => Some(value),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotHistoricalDownloadResponse {
    #[serde(default)]
    pub data: Vec<SpotHistoricalDownloadNode>,
    pub code: i64,
    pub msg: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotHistoricalSymbolDirectory {
    pub symbol_id: String,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpotOrderBook {
    pub last_update_id: u64,
    #[serde(default)]
    pub bids: Vec<(String, String)>,
    #[serde(default)]
    pub asks: Vec<(String, String)>,
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpotTrade {
    pub id: Option<u64>,
    pub price: String,
    pub qty: String,
    pub quote_qty: String,
    pub time: u64,
    pub is_buyer_maker: bool,
    pub is_best_match: bool,
    pub trade_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotAggTrade {
    pub a: Option<u64>,
    pub f: Option<u64>,
    pub l: Option<u64>,
    pub p: String,
    pub q: String,
    #[serde(rename = "T")]
    pub t: u64,
    pub m: bool,
    #[serde(rename = "M")]
    pub m_ignore: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotAveragePrice {
    pub mins: u64,
    pub price: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpotTicker24Hr {
    pub symbol: String,
    pub price_change: String,
    pub price_change_percent: String,
    pub prev_close_price: String,
    pub last_price: String,
    pub bid_price: String,
    pub bid_qty: String,
    pub ask_price: String,
    pub ask_qty: String,
    pub open_price: String,
    pub high_price: String,
    pub low_price: String,
    pub volume: String,
    pub quote_volume: String,
    pub open_time: u64,
    pub close_time: u64,
    pub count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotPriceTicker {
    pub symbol: String,
    pub price: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpotBookTicker {
    pub symbol: String,
    pub bid_price: Option<String>,
    pub bid_qty: Option<String>,
    pub ask_price: Option<String>,
    pub ask_qty: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: serde::de::DeserializeOwned"
))]
pub struct FuturesApiEnvelope<T> {
    pub success: bool,
    pub code: i64,
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: serde::de::DeserializeOwned"
))]
pub struct FuturesPagination<T> {
    #[serde(rename = "pageSize")]
    pub page_size: u64,
    #[serde(rename = "totalCount")]
    pub total_count: u64,
    #[serde(rename = "totalPage")]
    pub total_page: u64,
    #[serde(rename = "currentPage")]
    pub current_page: u64,
    #[serde(rename = "resultList", default)]
    pub result_list: Vec<T>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesContractInfo {
    pub symbol: String,
    pub display_name: Option<String>,
    pub display_name_en: Option<String>,
    pub position_open_type: Option<i64>,
    pub base_coin: Option<String>,
    pub quote_coin: Option<String>,
    pub base_coin_name: Option<String>,
    pub quote_coin_name: Option<String>,
    pub future_type: Option<i64>,
    pub settle_coin: Option<String>,
    pub contract_size: Option<f64>,
    pub min_leverage: Option<i64>,
    pub max_leverage: Option<i64>,
    pub price_scale: Option<i64>,
    pub vol_scale: Option<i64>,
    pub amount_scale: Option<i64>,
    pub price_unit: Option<f64>,
    pub vol_unit: Option<f64>,
    pub min_vol: Option<f64>,
    pub max_vol: Option<f64>,
    pub limit_max_vol: Option<f64>,
    pub bid_limit_price_rate: Option<f64>,
    pub ask_limit_price_rate: Option<f64>,
    pub taker_fee_rate: Option<f64>,
    pub maker_fee_rate: Option<f64>,
    pub maintenance_margin_rate: Option<f64>,
    pub initial_margin_rate: Option<f64>,
    pub risk_base_vol: Option<f64>,
    pub risk_incr_vol: Option<f64>,
    pub risk_incr_mmr: Option<f64>,
    pub risk_incr_imr: Option<f64>,
    pub risk_level_limit: Option<f64>,
    pub price_coefficient_variation: Option<f64>,
    pub state: Option<i64>,
    pub is_new: Option<bool>,
    pub is_hot: Option<bool>,
    pub is_hidden: Option<bool>,
    pub trigger_protect: Option<f64>,
    pub risk_long_short_switch: Option<i64>,
    pub risk_base_vol_long: Option<f64>,
    pub risk_incr_vol_long: Option<f64>,
    pub risk_base_vol_short: Option<f64>,
    pub risk_incr_vol_short: Option<f64>,
    pub opening_countdown_option: Option<i64>,
    pub opening_time: Option<u64>,
    pub liquidation_fee_rate: Option<f64>,
    pub fee_rate_mode: Option<MexcFlexibleString>,
    pub risk_limit_mode: Option<MexcFlexibleString>,
    pub risk_limit_type: Option<MexcFlexibleString>,
    #[serde(default)]
    pub max_num_orders: Vec<i64>,
    pub tiered_deal_amount: Option<f64>,
    pub tiered_effective_day: Option<i64>,
    pub tiered_exclude_zero_fee: Option<bool>,
    pub tiered_appoint_contract: Option<bool>,
    pub tiered_exclude_contract_id: Option<bool>,
    #[serde(default)]
    pub depth_step_list: Vec<MexcFlexibleString>,
    #[serde(default)]
    pub index_origin: Vec<MexcFlexibleString>,
    #[serde(flatten)]
    pub extra: JsonMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesTicker {
    pub contract_id: Option<u64>,
    pub symbol: String,
    pub last_price: f64,
    pub bid1: Option<f64>,
    pub ask1: Option<f64>,
    pub volume24: f64,
    pub amount24: Option<f64>,
    pub hold_vol: Option<f64>,
    pub lower24_price: Option<f64>,
    pub high24_price: Option<f64>,
    pub rise_fall_rate: Option<f64>,
    pub rise_fall_value: Option<f64>,
    pub index_price: Option<f64>,
    pub fair_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub max_bid_price: Option<f64>,
    pub min_ask_price: Option<f64>,
    pub timestamp: Option<u64>,
    #[serde(flatten)]
    pub extra: JsonMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesIndexPrice {
    pub symbol: String,
    pub index_price: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesFairPrice {
    pub symbol: String,
    pub fair_price: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesFundingRate {
    pub symbol: String,
    pub funding_rate: f64,
    pub max_funding_rate: Option<f64>,
    pub min_funding_rate: Option<f64>,
    pub collect_cycle: Option<u64>,
    pub next_settle_time: Option<u64>,
    pub timestamp: Option<u64>,
    #[serde(flatten)]
    pub extra: JsonMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesKlineSeries {
    #[serde(default)]
    pub time: Vec<u64>,
    #[serde(default)]
    pub open: Vec<f64>,
    #[serde(default)]
    pub close: Vec<f64>,
    #[serde(default)]
    pub high: Vec<f64>,
    #[serde(default)]
    pub low: Vec<f64>,
    #[serde(default)]
    pub vol: Vec<f64>,
    #[serde(default)]
    pub amount: Vec<f64>,
    #[serde(rename = "realOpen", default)]
    pub real_open: Vec<f64>,
    #[serde(rename = "realClose", default)]
    pub real_close: Vec<f64>,
    #[serde(rename = "realHigh", default)]
    pub real_high: Vec<f64>,
    #[serde(rename = "realLow", default)]
    pub real_low: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesDeal {
    pub p: f64,
    pub v: f64,
    #[serde(rename = "T")]
    pub trade_type: i64,
    #[serde(rename = "O")]
    pub open_type: i64,
    #[serde(rename = "M")]
    pub position_type: i64,
    pub i: String,
    pub t: u64,
    pub cts: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesDepthSnapshot {
    pub cts: Option<u64>,
    #[serde(default)]
    pub asks: Vec<FuturesDepthLevel>,
    #[serde(default)]
    pub bids: Vec<FuturesDepthLevel>,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesDepthStepSnapshot {
    #[serde(rename = "askMarketLevelPrice")]
    pub ask_market_level_price: Option<f64>,
    #[serde(rename = "bidMarketLevelPrice")]
    pub bid_market_level_price: Option<f64>,
    #[serde(default)]
    pub asks: Vec<FuturesDepthLevel>,
    #[serde(default)]
    pub bids: Vec<FuturesDepthLevel>,
    pub version: u64,
    pub ct: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FuturesInsuranceBalance {
    pub symbol: String,
    pub currency: String,
    pub available: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesInsuranceBalanceHistoryItem {
    pub symbol: String,
    pub currency: String,
    pub available: f64,
    #[serde(rename = "snapshotTime")]
    pub snapshot_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesFundingRateHistoryItem {
    pub symbol: String,
    pub funding_rate: f64,
    pub settle_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MexcWsAck {
    pub id: Option<u64>,
    pub code: Option<i64>,
    pub msg: Option<String>,
    pub channel: Option<String>,
    #[serde(default)]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MexcSpotSubscriptionAckSummary {
    pub successful_channels: Vec<String>,
    pub failed_channels: Vec<String>,
    pub failure_reason: Option<String>,
}

impl MexcWsAck {
    pub fn parse_spot_subscription_summary(&self) -> Option<MexcSpotSubscriptionAckSummary> {
        let message = self.msg.as_deref()?;
        if !message.contains("Subscribed successful!") {
            return None;
        }

        let successful_channels = extract_bracketed_section(message, "Subscribed successful! [")
            .map(split_channel_list)
            .unwrap_or_default();
        let failed_channels = extract_bracketed_section(message, "Not Subscribed successfully! [")
            .map(split_channel_list)
            .unwrap_or_default();
        let failure_reason = message
            .split_once("Reason")
            .map(|(_, tail)| {
                tail.trim_start_matches([':', '：', ' '])
                    .trim()
                    .trim_end_matches('.')
                    .trim()
                    .to_string()
            })
            .filter(|value| !value.is_empty());

        Some(MexcSpotSubscriptionAckSummary {
            successful_channels,
            failed_channels,
            failure_reason,
        })
    }
}

fn extract_bracketed_section<'a>(haystack: &'a str, marker: &str) -> Option<&'a str> {
    let start = haystack.find(marker)? + marker.len();
    let tail = &haystack[start..];
    let end = tail.find(']')?;
    Some(&tail[..end])
}

fn split_channel_list(section: &str) -> Vec<String> {
    section
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MexcFuturesEnvelope<T> {
    pub channel: String,
    pub symbol: Option<String>,
    pub data: T,
    pub ts: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MexcSpotEnvelope<T> {
    pub channel: String,
    pub symbol: Option<String>,
    pub create_time: Option<i64>,
    pub send_time: Option<i64>,
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesWsTicker {
    pub symbol: String,
    pub timestamp: Option<u64>,
    pub last_price: f64,
    pub volume24: Option<f64>,
    pub amount24: Option<f64>,
    pub rise_fall_rate: Option<f64>,
    pub fair_price: Option<f64>,
    pub index_price: Option<f64>,
    pub max_bid_price: Option<f64>,
    pub min_ask_price: Option<f64>,
    pub bid1: Option<f64>,
    pub ask1: Option<f64>,
    pub hold_vol: Option<f64>,
    pub funding_rate: Option<f64>,
    pub lower24_price: Option<f64>,
    pub high24_price: Option<f64>,
    #[serde(flatten)]
    pub extra: JsonMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesWsPricePoint {
    pub symbol: String,
    pub price: f64,
}

#[cfg(test)]
mod ack_tests {
    use super::*;

    #[test]
    fn parse_spot_subscription_ack_summary_handles_success_and_blocked_lists() {
        let ack = MexcWsAck {
            id: None,
            code: None,
            msg: Some("Subscribed successful! [spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT,spot@public.kline.v3.api.pb@BTCUSDT@Min1]. Not Subscribed successfully! [spot@public.increase.depth.v3.api.pb@BTCUSDT].  Reason： Blocked! ".to_string()),
            channel: None,
            data: None,
        };

        let summary = ack
            .parse_spot_subscription_summary()
            .expect("summary should parse");
        assert_eq!(
            summary.successful_channels,
            vec![
                "spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT",
                "spot@public.kline.v3.api.pb@BTCUSDT@Min1",
            ]
        );
        assert_eq!(
            summary.failed_channels,
            vec!["spot@public.increase.depth.v3.api.pb@BTCUSDT"]
        );
        assert_eq!(summary.failure_reason.as_deref(), Some("Blocked!"));
    }

    #[test]
    fn parse_spot_subscription_ack_summary_returns_none_for_non_subscription_message() {
        let ack = MexcWsAck {
            id: None,
            code: None,
            msg: Some("PONG".to_string()),
            channel: None,
            data: None,
        };

        assert!(ack.parse_spot_subscription_summary().is_none());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesWsFundingRatePoint {
    pub symbol: String,
    pub rate: f64,
    #[serde(rename = "nextSettleTime")]
    pub next_settle_time: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesWsKline {
    pub symbol: String,
    pub interval: String,
    pub a: f64,
    pub q: f64,
    pub o: f64,
    pub c: f64,
    pub h: f64,
    pub l: f64,
    pub v: Option<f64>,
    pub ro: Option<f64>,
    pub rc: Option<f64>,
    pub rh: Option<f64>,
    pub rl: Option<f64>,
    pub t: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesEventContract {
    pub contract_id: MexcFlexibleString,
    pub symbol: String,
    pub base_coin: String,
    pub quote_coin: String,
    pub base_coin_name: String,
    pub quote_coin_name: String,
    pub settle_coin: String,
    pub base_coin_icon_url: Option<String>,
    pub invest_min_amount: Option<f64>,
    pub invest_max_amount: Option<f64>,
    pub amount_scale: Option<i64>,
    pub pay_rate_scale: Option<i64>,
    pub index_price_scale: Option<i64>,
    pub available_scale: Option<i64>,
    #[serde(flatten)]
    pub extra: JsonMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesWsChannelEnvelope<T> {
    pub channel: String,
    pub symbol: Option<String>,
    pub data: T,
    pub ts: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::{
        SpotHistoricalDownloadFile, SpotHistoricalDownloadNode, SpotHistoricalDownloadResponse,
    };

    #[test]
    fn spot_historical_download_response_decodes_directories() {
        let response: SpotHistoricalDownloadResponse = serde_json::from_str(
            r#"{
                "data":["daily/","monthly/"],
                "code":0,
                "msg":"success",
                "timestamp":1782612620225
            }"#,
        )
        .expect("decode directory response");

        assert_eq!(response.data.len(), 2);
        assert_eq!(
            response.data[0],
            SpotHistoricalDownloadNode::Directory("daily/".to_string())
        );
    }

    #[test]
    fn spot_historical_download_response_decodes_files() {
        let response: SpotHistoricalDownloadResponse = serde_json::from_str(
            r#"{
                "data":[
                    {
                        "fileName":"NIM_USDT-Day1-2024-05-01.csv",
                        "maskedUrl":"https://example.invalid/file.csv",
                        "lastModified":"2024-06-19T07:43:58Z",
                        "fileSize":3355
                    }
                ],
                "code":0,
                "msg":"success",
                "timestamp":1782612632632
            }"#,
        )
        .expect("decode file response");

        assert_eq!(response.data.len(), 1);
        assert_eq!(
            response.data[0],
            SpotHistoricalDownloadNode::File(SpotHistoricalDownloadFile {
                file_name: "NIM_USDT-Day1-2024-05-01.csv".to_string(),
                masked_url: "https://example.invalid/file.csv".to_string(),
                last_modified: "2024-06-19T07:43:58Z".to_string(),
                file_size: 3355,
            })
        );
    }

    #[test]
    fn spot_historical_download_file_infers_symbol_from_filename() {
        let file = SpotHistoricalDownloadFile {
            file_name: "BTC_USDT-Day1-2025-01-01.csv".to_string(),
            masked_url: "https://example.invalid/file.csv".to_string(),
            last_modified: "2025-01-02T00:00:00Z".to_string(),
            file_size: 123,
        };

        assert_eq!(file.inferred_symbol().as_deref(), Some("BTC_USDT"));
    }
}
