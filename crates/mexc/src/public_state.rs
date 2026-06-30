use crate::spot_proto::{
    PublicAggreBookTickerV3Api, PublicAggreDealsV3Api, PublicAggreDepthsV3Api,
    PublicBookTickerBatchV3Api, PublicLimitDepthsV3Api, PublicMiniTickerV3Api,
    PublicMiniTickersV3Api, PublicSpotKlineV3Api,
};
use crate::{
    FuturesContractInfo, FuturesDeal, FuturesDepthSnapshot, FuturesDepthStepSnapshot,
    FuturesEventContract, FuturesFundingRate, FuturesInsuranceBalance, FuturesTicker,
    FuturesWsFundingRatePoint, FuturesWsKline, FuturesWsPricePoint, MexcFuturesReferenceSnapshot,
    MexcFuturesWsMessage, MexcPublicBootstrapSnapshot, MexcPublicDeepBootstrapSnapshot,
    MexcPublicEvent, MexcPublicMetadataRefreshSnapshot, MexcSpotWsMessage, SpotBookTicker,
    SpotExchangeInfo, SpotOfflineSymbol, SpotPriceTicker, SpotTicker24Hr,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MexcPublicStateHandoffReport {
    pub spot_server_time: Option<u64>,
    pub futures_server_time: Option<u64>,
    pub spot_default_symbol_count: usize,
    pub spot_offline_symbol_count: usize,
    pub spot_exchange_symbol_count: usize,
    pub futures_transferable_currency_count: usize,
    pub futures_insurance_balance_count: usize,
    pub global_spot_mini_ticker_channel_count: usize,
    pub spot: MexcSpotStateCoverageReport,
    pub futures: MexcFuturesStateCoverageReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MexcSpotStateCoverageReport {
    pub total_symbols: usize,
    pub metadata_only_symbols: usize,
    pub with_ticker_24hr: usize,
    pub with_price_ticker: usize,
    pub with_book_ticker_snapshot: usize,
    pub with_agg_book_ticker: usize,
    pub with_batch_book_ticker: usize,
    pub with_latest_trades: usize,
    pub with_agg_depth: usize,
    pub with_limit_depth: usize,
    pub with_any_kline: usize,
    pub total_kline_intervals: usize,
    pub with_any_mini_ticker: usize,
    pub total_mini_ticker_channels: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MexcFuturesStateCoverageReport {
    pub total_symbols: usize,
    pub orphan_symbols: usize,
    pub orphan_symbols_with_other_state: usize,
    pub orphan_symbols_list: Vec<String>,
    pub orphan_symbols_with_other_state_list: Vec<String>,
    pub orphan_with_event_contract: usize,
    pub orphan_with_ticker: usize,
    pub orphan_with_latest_deals: usize,
    pub orphan_with_depth: usize,
    pub orphan_with_any_depth_step: usize,
    pub orphan_with_funding_rate_snapshot: usize,
    pub orphan_with_funding_rate_live: usize,
    pub orphan_with_index_price: usize,
    pub orphan_with_fair_price: usize,
    pub orphan_with_any_kline: usize,
    pub with_contract: usize,
    pub without_contract: usize,
    pub without_contract_symbols: Vec<String>,
    pub without_contract_with_other_state: usize,
    pub without_contract_with_other_state_symbols: Vec<String>,
    pub with_event_contract: usize,
    pub with_event_contract_symbols: Vec<String>,
    pub with_ticker: usize,
    pub with_latest_deals: usize,
    pub with_depth: usize,
    pub with_any_depth_step: usize,
    pub total_depth_step_channels: usize,
    pub with_funding_rate_snapshot: usize,
    pub with_funding_rate_live: usize,
    pub with_index_price: usize,
    pub with_fair_price: usize,
    pub with_any_kline: usize,
    pub total_kline_intervals: usize,
}

#[derive(Debug, Clone, Default)]
pub struct MexcSpotSymbolState {
    pub ticker_24hr: Option<SpotTicker24Hr>,
    pub price_ticker: Option<SpotPriceTicker>,
    pub book_ticker_snapshot: Option<SpotBookTicker>,
    pub agg_book_ticker: Option<PublicAggreBookTickerV3Api>,
    pub batch_book_ticker: Option<PublicBookTickerBatchV3Api>,
    pub latest_trades: Option<PublicAggreDealsV3Api>,
    pub agg_depth: Option<PublicAggreDepthsV3Api>,
    pub limit_depth: Option<PublicLimitDepthsV3Api>,
    pub latest_kline_by_interval: BTreeMap<String, PublicSpotKlineV3Api>,
    pub mini_ticker_by_channel: BTreeMap<String, PublicMiniTickerV3Api>,
}

#[derive(Debug, Clone, Default)]
pub struct MexcFuturesSymbolState {
    pub contract: Option<FuturesContractInfo>,
    pub event_contract: Option<FuturesEventContract>,
    pub ticker: Option<FuturesTicker>,
    pub latest_deals: Option<Vec<FuturesDeal>>,
    pub depth: Option<FuturesDepthSnapshot>,
    pub depth_step_by_channel: BTreeMap<String, FuturesDepthStepSnapshot>,
    pub funding_rate_snapshot: Option<FuturesFundingRate>,
    pub funding_rate_live: Option<FuturesWsFundingRatePoint>,
    pub index_price: Option<FuturesWsPricePoint>,
    pub fair_price: Option<FuturesWsPricePoint>,
    pub latest_kline_by_interval: BTreeMap<String, FuturesWsKline>,
}

impl MexcFuturesSymbolState {
    fn has_other_state(&self) -> bool {
        self.event_contract.is_some()
            || self.ticker.is_some()
            || self.latest_deals.is_some()
            || self.depth.is_some()
            || !self.depth_step_by_channel.is_empty()
            || self.funding_rate_snapshot.is_some()
            || self.funding_rate_live.is_some()
            || self.index_price.is_some()
            || self.fair_price.is_some()
            || !self.latest_kline_by_interval.is_empty()
    }

    fn clear_non_contract_state(&mut self) {
        self.event_contract = None;
        self.ticker = None;
        self.latest_deals = None;
        self.depth = None;
        self.depth_step_by_channel.clear();
        self.funding_rate_snapshot = None;
        self.funding_rate_live = None;
        self.index_price = None;
        self.fair_price = None;
        self.latest_kline_by_interval.clear();
    }

    fn absorb(&mut self, other: Self) {
        if self.contract.is_none() {
            self.contract = other.contract;
        }
        if self.event_contract.is_none() {
            self.event_contract = other.event_contract;
        }
        if self.ticker.is_none() {
            self.ticker = other.ticker;
        }
        if self.latest_deals.is_none() {
            self.latest_deals = other.latest_deals;
        }
        if self.depth.is_none() {
            self.depth = other.depth;
        }
        self.depth_step_by_channel
            .extend(other.depth_step_by_channel);
        if self.funding_rate_snapshot.is_none() {
            self.funding_rate_snapshot = other.funding_rate_snapshot;
        }
        if self.funding_rate_live.is_none() {
            self.funding_rate_live = other.funding_rate_live;
        }
        if self.index_price.is_none() {
            self.index_price = other.index_price;
        }
        if self.fair_price.is_none() {
            self.fair_price = other.fair_price;
        }
        self.latest_kline_by_interval
            .extend(other.latest_kline_by_interval);
    }
}

impl MexcSpotSymbolState {
    fn absorb(&mut self, other: Self) {
        if self.ticker_24hr.is_none() {
            self.ticker_24hr = other.ticker_24hr;
        }
        if self.price_ticker.is_none() {
            self.price_ticker = other.price_ticker;
        }
        if self.book_ticker_snapshot.is_none() {
            self.book_ticker_snapshot = other.book_ticker_snapshot;
        }
        if self.agg_book_ticker.is_none() {
            self.agg_book_ticker = other.agg_book_ticker;
        }
        if self.batch_book_ticker.is_none() {
            self.batch_book_ticker = other.batch_book_ticker;
        }
        if self.latest_trades.is_none() {
            self.latest_trades = other.latest_trades;
        }
        if self.agg_depth.is_none() {
            self.agg_depth = other.agg_depth;
        }
        if self.limit_depth.is_none() {
            self.limit_depth = other.limit_depth;
        }
        self.latest_kline_by_interval
            .extend(other.latest_kline_by_interval);
        self.mini_ticker_by_channel
            .extend(other.mini_ticker_by_channel);
    }
}

#[derive(Debug, Clone, Default)]
pub struct MexcPublicState {
    pub spot_server_time: Option<u64>,
    pub spot_default_symbols: Vec<String>,
    pub spot_offline_symbols: Vec<SpotOfflineSymbol>,
    pub spot_exchange_info: Option<SpotExchangeInfo>,
    pub futures_server_time: Option<u64>,
    pub futures_transferable_currencies: Vec<String>,
    pub futures_insurance_balances: Vec<FuturesInsuranceBalance>,
    pub global_spot_mini_tickers_by_channel: BTreeMap<String, PublicMiniTickersV3Api>,
    pub spot_symbols: BTreeMap<String, MexcSpotSymbolState>,
    pub spot_metadata_only_symbols: BTreeMap<String, MexcSpotSymbolState>,
    pub futures_symbols: BTreeMap<String, MexcFuturesSymbolState>,
    pub futures_orphan_symbols: BTreeMap<String, MexcFuturesSymbolState>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MexcPublicMetadataRefreshReport {
    pub spot_server_time_before: Option<u64>,
    pub spot_server_time_after: u64,
    pub spot_server_time_changed: bool,
    pub spot_default_symbols_changed: bool,
    pub spot_default_symbol_count_before: usize,
    pub spot_default_symbol_count_after: usize,
    pub spot_offline_symbols_changed: bool,
    pub spot_offline_symbol_count_before: usize,
    pub spot_offline_symbol_count_after: usize,
    pub spot_exchange_info_changed: bool,
    pub spot_exchange_symbol_count_before: usize,
    pub spot_exchange_symbol_count_after: usize,
    pub futures_server_time_before: Option<u64>,
    pub futures_server_time_after: u64,
    pub futures_server_time_changed: bool,
    pub futures_transferable_currencies_changed: bool,
    pub futures_transferable_currency_count_before: usize,
    pub futures_transferable_currency_count_after: usize,
    pub futures_insurance_balances_changed: bool,
    pub futures_insurance_balance_count_before: usize,
    pub futures_insurance_balance_count_after: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MexcFuturesContractChange {
    pub symbol: String,
    pub kind: MexcFuturesContractChangeKind,
    pub changed_fields: Vec<String>,
    pub field_changes: Vec<MexcFuturesContractFieldChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MexcFuturesContractFieldChange {
    pub field: String,
    pub before: Value,
    pub after: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MexcFuturesContractChangeKind {
    Added,
    Updated,
    Removed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct MexcFuturesContractSnapshotDelta {
    pub refreshed_contracts: usize,
    pub added: usize,
    pub added_symbols: Vec<String>,
    pub updated: usize,
    pub updated_symbols: Vec<String>,
    pub removed: usize,
    pub removed_symbols: Vec<String>,
    pub unchanged: usize,
    pub changes: Vec<MexcFuturesContractChange>,
}

impl MexcFuturesContractSnapshotDelta {
    pub fn has_changes(&self) -> bool {
        self.added > 0 || self.updated > 0 || self.removed > 0
    }

    pub fn is_noop(&self) -> bool {
        !self.has_changes()
    }

    pub fn change_count(&self) -> usize {
        self.changes.len()
    }
}

impl MexcPublicState {
    fn promote_spot_symbol(&mut self, symbol: &str) {
        if let Some(orphan) = self.spot_metadata_only_symbols.remove(symbol) {
            self.spot_symbols
                .entry(symbol.to_string())
                .and_modify(|current| current.absorb(orphan.clone()))
                .or_insert(orphan);
        } else {
            self.spot_symbols.entry(symbol.to_string()).or_default();
        }
    }

    fn spot_symbol_state_for_live_update(&mut self, symbol: &str) -> &mut MexcSpotSymbolState {
        if self.spot_symbols.contains_key(symbol) {
            return self
                .spot_symbols
                .get_mut(symbol)
                .expect("spot symbol present");
        }
        self.spot_metadata_only_symbols
            .entry(symbol.to_string())
            .or_default()
    }

    fn promote_futures_symbol(&mut self, symbol: &str) {
        if let Some(orphan) = self.futures_orphan_symbols.remove(symbol) {
            self.futures_symbols
                .entry(symbol.to_string())
                .and_modify(|current| current.absorb(orphan.clone()))
                .or_insert(orphan);
        } else {
            self.futures_symbols.entry(symbol.to_string()).or_default();
        }
    }

    fn futures_symbol_state_for_live_update(
        &mut self,
        symbol: &str,
    ) -> &mut MexcFuturesSymbolState {
        if self.futures_symbols.contains_key(symbol) {
            return self
                .futures_symbols
                .get_mut(symbol)
                .expect("futures symbol present");
        }
        self.futures_orphan_symbols
            .entry(symbol.to_string())
            .or_default()
    }

    pub fn handoff_report(&self) -> MexcPublicStateHandoffReport {
        let mut spot = MexcSpotStateCoverageReport {
            total_symbols: self.spot_symbols.len(),
            metadata_only_symbols: self.spot_metadata_only_symbols.len(),
            with_ticker_24hr: 0,
            with_price_ticker: 0,
            with_book_ticker_snapshot: 0,
            with_agg_book_ticker: 0,
            with_batch_book_ticker: 0,
            with_latest_trades: 0,
            with_agg_depth: 0,
            with_limit_depth: 0,
            with_any_kline: 0,
            total_kline_intervals: 0,
            with_any_mini_ticker: 0,
            total_mini_ticker_channels: 0,
        };
        for symbol in self.spot_symbols.values() {
            spot.with_ticker_24hr += usize::from(symbol.ticker_24hr.is_some());
            spot.with_price_ticker += usize::from(symbol.price_ticker.is_some());
            spot.with_book_ticker_snapshot += usize::from(symbol.book_ticker_snapshot.is_some());
            spot.with_agg_book_ticker += usize::from(symbol.agg_book_ticker.is_some());
            spot.with_batch_book_ticker += usize::from(symbol.batch_book_ticker.is_some());
            spot.with_latest_trades += usize::from(symbol.latest_trades.is_some());
            spot.with_agg_depth += usize::from(symbol.agg_depth.is_some());
            spot.with_limit_depth += usize::from(symbol.limit_depth.is_some());
            spot.with_any_kline += usize::from(!symbol.latest_kline_by_interval.is_empty());
            spot.total_kline_intervals += symbol.latest_kline_by_interval.len();
            spot.with_any_mini_ticker += usize::from(!symbol.mini_ticker_by_channel.is_empty());
            spot.total_mini_ticker_channels += symbol.mini_ticker_by_channel.len();
        }

        let mut futures = MexcFuturesStateCoverageReport {
            total_symbols: self.futures_symbols.len(),
            orphan_symbols: self.futures_orphan_symbols.len(),
            orphan_symbols_with_other_state: 0,
            orphan_symbols_list: self.futures_orphan_symbols.keys().cloned().collect(),
            orphan_symbols_with_other_state_list: Vec::new(),
            orphan_with_event_contract: 0,
            orphan_with_ticker: 0,
            orphan_with_latest_deals: 0,
            orphan_with_depth: 0,
            orphan_with_any_depth_step: 0,
            orphan_with_funding_rate_snapshot: 0,
            orphan_with_funding_rate_live: 0,
            orphan_with_index_price: 0,
            orphan_with_fair_price: 0,
            orphan_with_any_kline: 0,
            with_contract: 0,
            without_contract: 0,
            without_contract_symbols: Vec::new(),
            without_contract_with_other_state: 0,
            without_contract_with_other_state_symbols: Vec::new(),
            with_event_contract: 0,
            with_event_contract_symbols: Vec::new(),
            with_ticker: 0,
            with_latest_deals: 0,
            with_depth: 0,
            with_any_depth_step: 0,
            total_depth_step_channels: 0,
            with_funding_rate_snapshot: 0,
            with_funding_rate_live: 0,
            with_index_price: 0,
            with_fair_price: 0,
            with_any_kline: 0,
            total_kline_intervals: 0,
        };
        for (symbol_name, symbol) in &self.futures_symbols {
            let has_contract = symbol.contract.is_some();
            futures.with_contract += usize::from(has_contract);
            futures.without_contract += usize::from(!has_contract);
            if !has_contract {
                futures.without_contract_symbols.push(symbol_name.clone());
            }
            let without_contract_with_other_state = !has_contract && symbol.has_other_state();
            futures.without_contract_with_other_state +=
                usize::from(without_contract_with_other_state);
            if without_contract_with_other_state {
                futures
                    .without_contract_with_other_state_symbols
                    .push(symbol_name.clone());
            }
            let has_event_contract = symbol.event_contract.is_some();
            futures.with_event_contract += usize::from(has_event_contract);
            if has_event_contract {
                futures
                    .with_event_contract_symbols
                    .push(symbol_name.clone());
            }
            futures.with_ticker += usize::from(symbol.ticker.is_some());
            futures.with_latest_deals += usize::from(symbol.latest_deals.is_some());
            futures.with_depth += usize::from(symbol.depth.is_some());
            futures.with_any_depth_step += usize::from(!symbol.depth_step_by_channel.is_empty());
            futures.total_depth_step_channels += symbol.depth_step_by_channel.len();
            futures.with_funding_rate_snapshot +=
                usize::from(symbol.funding_rate_snapshot.is_some());
            futures.with_funding_rate_live += usize::from(symbol.funding_rate_live.is_some());
            futures.with_index_price += usize::from(symbol.index_price.is_some());
            futures.with_fair_price += usize::from(symbol.fair_price.is_some());
            futures.with_any_kline += usize::from(!symbol.latest_kline_by_interval.is_empty());
            futures.total_kline_intervals += symbol.latest_kline_by_interval.len();
        }
        for (symbol_name, symbol) in &self.futures_orphan_symbols {
            let has_other_state = symbol.has_other_state();
            futures.orphan_symbols_with_other_state += usize::from(has_other_state);
            if has_other_state {
                futures
                    .orphan_symbols_with_other_state_list
                    .push(symbol_name.clone());
            }
            futures.orphan_with_event_contract += usize::from(symbol.event_contract.is_some());
            futures.orphan_with_ticker += usize::from(symbol.ticker.is_some());
            futures.orphan_with_latest_deals += usize::from(symbol.latest_deals.is_some());
            futures.orphan_with_depth += usize::from(symbol.depth.is_some());
            futures.orphan_with_any_depth_step +=
                usize::from(!symbol.depth_step_by_channel.is_empty());
            futures.orphan_with_funding_rate_snapshot +=
                usize::from(symbol.funding_rate_snapshot.is_some());
            futures.orphan_with_funding_rate_live +=
                usize::from(symbol.funding_rate_live.is_some());
            futures.orphan_with_index_price += usize::from(symbol.index_price.is_some());
            futures.orphan_with_fair_price += usize::from(symbol.fair_price.is_some());
            futures.orphan_with_any_kline +=
                usize::from(!symbol.latest_kline_by_interval.is_empty());
        }

        MexcPublicStateHandoffReport {
            spot_server_time: self.spot_server_time,
            futures_server_time: self.futures_server_time,
            spot_default_symbol_count: self.spot_default_symbols.len(),
            spot_offline_symbol_count: self.spot_offline_symbols.len(),
            spot_exchange_symbol_count: self
                .spot_exchange_info
                .as_ref()
                .map(|info| info.symbols.len())
                .unwrap_or(0),
            futures_transferable_currency_count: self.futures_transferable_currencies.len(),
            futures_insurance_balance_count: self.futures_insurance_balances.len(),
            global_spot_mini_ticker_channel_count: self.global_spot_mini_tickers_by_channel.len(),
            spot,
            futures,
        }
    }

    pub fn from_snapshot(snapshot: &MexcPublicBootstrapSnapshot) -> Self {
        let mut state = Self {
            spot_server_time: Some(snapshot.spot.server_time.server_time),
            spot_default_symbols: snapshot.spot.default_symbols.data.clone(),
            spot_offline_symbols: snapshot.spot.offline_symbols.data.clone(),
            spot_exchange_info: Some(snapshot.spot.exchange_info.clone()),
            futures_server_time: Some(snapshot.futures.server_time),
            futures_transferable_currencies: snapshot.futures.transferable_currencies.clone(),
            futures_insurance_balances: snapshot.futures.insurance_balances.clone(),
            ..Self::default()
        };

        for symbol in &snapshot.spot.default_symbols.data {
            state
                .spot_metadata_only_symbols
                .entry(symbol.clone())
                .or_default();
        }
        for symbol in &snapshot.spot.offline_symbols.data {
            state
                .spot_metadata_only_symbols
                .entry(symbol.symbol.clone())
                .or_default();
        }
        for symbol in &snapshot.spot.exchange_info.symbols {
            state.promote_spot_symbol(&symbol.symbol);
        }
        for ticker in &snapshot.spot.ticker_24hr {
            state
                .spot_symbol_state_for_live_update(&ticker.symbol)
                .ticker_24hr = Some(ticker.clone());
        }
        for ticker in &snapshot.spot.price_tickers {
            state
                .spot_symbol_state_for_live_update(&ticker.symbol)
                .price_ticker = Some(ticker.clone());
        }
        for ticker in &snapshot.spot.book_tickers {
            state
                .spot_symbol_state_for_live_update(&ticker.symbol)
                .book_ticker_snapshot = Some(ticker.clone());
        }
        for contract in &snapshot.futures.contracts {
            state.promote_futures_symbol(&contract.symbol);
            state
                .futures_symbols
                .get_mut(&contract.symbol)
                .expect("promoted futures symbol present")
                .contract = Some(contract.clone());
        }
        for ticker in &snapshot.futures.tickers {
            let symbol_state = state.futures_symbol_state_for_live_update(&ticker.symbol);
            symbol_state.ticker = Some(ticker.clone());
            symbol_state.hydrate_from_snapshot_ticker(ticker);
        }
        for insurance_balance in &snapshot.futures.insurance_balances {
            if !state
                .futures_symbols
                .contains_key(&insurance_balance.symbol)
            {
                state
                    .futures_orphan_symbols
                    .entry(insurance_balance.symbol.clone())
                    .or_default();
            }
        }

        state
    }

    pub fn from_deep_snapshot(snapshot: &MexcPublicDeepBootstrapSnapshot) -> Self {
        let mut state = Self::from_snapshot(&snapshot.base);
        state.hydrate_futures_reference_snapshot(&snapshot.futures_reference);
        state
    }

    pub fn apply_event(&mut self, event: &MexcPublicEvent) {
        match event {
            MexcPublicEvent::Spot(message) => self.apply_spot_message(message),
            MexcPublicEvent::Futures(message) => self.apply_futures_message(message),
        }
    }

    pub fn hydrate_futures_reference_snapshot(&mut self, snapshot: &MexcFuturesReferenceSnapshot) {
        for index_price in &snapshot.index_prices {
            self.futures_symbol_state_for_live_update(&index_price.symbol)
                .index_price = Some(FuturesWsPricePoint {
                symbol: index_price.symbol.clone(),
                price: index_price.index_price,
            });
        }
        for fair_price in &snapshot.fair_prices {
            self.futures_symbol_state_for_live_update(&fair_price.symbol)
                .fair_price = Some(FuturesWsPricePoint {
                symbol: fair_price.symbol.clone(),
                price: fair_price.fair_price,
            });
        }
        for funding_rate in &snapshot.funding_rates {
            self.futures_symbol_state_for_live_update(&funding_rate.symbol)
                .funding_rate_snapshot = Some(funding_rate.clone());
        }
    }

    pub fn hydrate_public_metadata_snapshot(
        &mut self,
        snapshot: &MexcPublicMetadataRefreshSnapshot,
    ) -> MexcPublicMetadataRefreshReport {
        let report = MexcPublicMetadataRefreshReport {
            spot_server_time_before: self.spot_server_time,
            spot_server_time_after: snapshot.spot_server_time.server_time,
            spot_server_time_changed: self.spot_server_time
                != Some(snapshot.spot_server_time.server_time),
            spot_default_symbols_changed: self.spot_default_symbols
                != snapshot.spot_default_symbols.data,
            spot_default_symbol_count_before: self.spot_default_symbols.len(),
            spot_default_symbol_count_after: snapshot.spot_default_symbols.data.len(),
            spot_offline_symbols_changed: self.spot_offline_symbols
                != snapshot.spot_offline_symbols.data,
            spot_offline_symbol_count_before: self.spot_offline_symbols.len(),
            spot_offline_symbol_count_after: snapshot.spot_offline_symbols.data.len(),
            spot_exchange_info_changed: self.spot_exchange_info.as_ref()
                != Some(&snapshot.spot_exchange_info),
            spot_exchange_symbol_count_before: self
                .spot_exchange_info
                .as_ref()
                .map(|info| info.symbols.len())
                .unwrap_or(0),
            spot_exchange_symbol_count_after: snapshot.spot_exchange_info.symbols.len(),
            futures_server_time_before: self.futures_server_time,
            futures_server_time_after: snapshot.futures_server_time,
            futures_server_time_changed: self.futures_server_time
                != Some(snapshot.futures_server_time),
            futures_transferable_currencies_changed: self.futures_transferable_currencies
                != snapshot.futures_transferable_currencies,
            futures_transferable_currency_count_before: self.futures_transferable_currencies.len(),
            futures_transferable_currency_count_after: snapshot
                .futures_transferable_currencies
                .len(),
            futures_insurance_balances_changed: self.futures_insurance_balances
                != snapshot.futures_insurance_balances,
            futures_insurance_balance_count_before: self.futures_insurance_balances.len(),
            futures_insurance_balance_count_after: snapshot.futures_insurance_balances.len(),
        };

        self.spot_server_time = Some(snapshot.spot_server_time.server_time);
        self.spot_default_symbols = snapshot.spot_default_symbols.data.clone();
        self.spot_offline_symbols = snapshot.spot_offline_symbols.data.clone();
        self.spot_exchange_info = Some(snapshot.spot_exchange_info.clone());
        self.futures_server_time = Some(snapshot.futures_server_time);
        self.futures_transferable_currencies = snapshot.futures_transferable_currencies.clone();
        self.futures_insurance_balances = snapshot.futures_insurance_balances.clone();

        for symbol in &self.spot_default_symbols {
            self.spot_metadata_only_symbols
                .entry(symbol.clone())
                .or_default();
        }
        for symbol in &self.spot_offline_symbols {
            self.spot_metadata_only_symbols
                .entry(symbol.symbol.clone())
                .or_default();
        }
        if let Some(exchange_info) = &self.spot_exchange_info {
            let symbols = exchange_info
                .symbols
                .iter()
                .map(|symbol| symbol.symbol.clone())
                .collect::<Vec<_>>();
            for symbol in symbols {
                self.promote_spot_symbol(&symbol);
            }
        }
        for insurance_balance in &self.futures_insurance_balances {
            if !self.futures_symbols.contains_key(&insurance_balance.symbol) {
                self.futures_orphan_symbols
                    .entry(insurance_balance.symbol.clone())
                    .or_default();
            }
        }

        report
    }

    pub fn hydrate_futures_contract_snapshot(
        &mut self,
        contracts: &[FuturesContractInfo],
    ) -> MexcFuturesContractSnapshotDelta {
        let active_symbols = contracts
            .iter()
            .map(|contract| contract.symbol.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let mut report = MexcFuturesContractSnapshotDelta {
            refreshed_contracts: contracts.len(),
            ..Default::default()
        };

        for contract in contracts {
            self.promote_futures_symbol(&contract.symbol);
            let symbol_state = self
                .futures_symbols
                .get_mut(&contract.symbol)
                .expect("promoted futures symbol present");
            match &symbol_state.contract {
                None => {
                    report.added += 1;
                    report.added_symbols.push(contract.symbol.clone());
                    report.changes.push(MexcFuturesContractChange {
                        symbol: contract.symbol.clone(),
                        kind: MexcFuturesContractChangeKind::Added,
                        changed_fields: Vec::new(),
                        field_changes: Vec::new(),
                    });
                }
                Some(existing) if existing == contract => report.unchanged += 1,
                Some(existing) => {
                    let field_changes = diff_contract_fields(existing, contract);
                    report.updated += 1;
                    report.updated_symbols.push(contract.symbol.clone());
                    report.changes.push(MexcFuturesContractChange {
                        symbol: contract.symbol.clone(),
                        kind: MexcFuturesContractChangeKind::Updated,
                        changed_fields: field_changes
                            .iter()
                            .map(|item| item.field.clone())
                            .collect(),
                        field_changes,
                    });
                }
            }
            symbol_state.contract = Some(contract.clone());
        }

        for (symbol, symbol_state) in &mut self.futures_symbols {
            if !active_symbols.contains(symbol) {
                if symbol_state.contract.is_some() {
                    report.removed += 1;
                    report.removed_symbols.push(symbol.clone());
                    report.changes.push(MexcFuturesContractChange {
                        symbol: symbol.clone(),
                        kind: MexcFuturesContractChangeKind::Removed,
                        changed_fields: Vec::new(),
                        field_changes: Vec::new(),
                    });
                }
                symbol_state.contract = None;
            }
        }
        for symbol_state in self.futures_orphan_symbols.values_mut() {
            symbol_state.clear_non_contract_state();
        }

        report
    }

    pub fn hydrate_futures_contract_updates(
        &mut self,
        contracts: &[FuturesContractInfo],
    ) -> MexcFuturesContractSnapshotDelta {
        let mut report = MexcFuturesContractSnapshotDelta {
            refreshed_contracts: contracts.len(),
            ..Default::default()
        };

        for contract in contracts {
            self.promote_futures_symbol(&contract.symbol);
            let symbol_state = self
                .futures_symbols
                .get_mut(&contract.symbol)
                .expect("promoted futures symbol present");
            match &symbol_state.contract {
                None => {
                    report.added += 1;
                    report.added_symbols.push(contract.symbol.clone());
                    report.changes.push(MexcFuturesContractChange {
                        symbol: contract.symbol.clone(),
                        kind: MexcFuturesContractChangeKind::Added,
                        changed_fields: Vec::new(),
                        field_changes: Vec::new(),
                    });
                }
                Some(existing) if existing == contract => report.unchanged += 1,
                Some(existing) => {
                    let field_changes = diff_contract_fields(existing, contract);
                    report.updated += 1;
                    report.updated_symbols.push(contract.symbol.clone());
                    report.changes.push(MexcFuturesContractChange {
                        symbol: contract.symbol.clone(),
                        kind: MexcFuturesContractChangeKind::Updated,
                        changed_fields: field_changes
                            .iter()
                            .map(|item| item.field.clone())
                            .collect(),
                        field_changes,
                    });
                }
            }
            symbol_state.contract = Some(contract.clone());
        }

        report
    }

    pub fn reset_after_spot_session_restart(&mut self) {
        self.global_spot_mini_tickers_by_channel.clear();
        for symbol_state in self.spot_symbols.values_mut() {
            symbol_state.agg_book_ticker = None;
            symbol_state.batch_book_ticker = None;
            symbol_state.latest_trades = None;
            symbol_state.agg_depth = None;
            symbol_state.limit_depth = None;
            symbol_state.latest_kline_by_interval.clear();
            symbol_state.mini_ticker_by_channel.clear();
        }
        for symbol_state in self.spot_metadata_only_symbols.values_mut() {
            symbol_state.agg_book_ticker = None;
            symbol_state.batch_book_ticker = None;
            symbol_state.latest_trades = None;
            symbol_state.agg_depth = None;
            symbol_state.limit_depth = None;
            symbol_state.latest_kline_by_interval.clear();
            symbol_state.mini_ticker_by_channel.clear();
        }
    }

    pub fn reset_after_futures_session_restart(&mut self) {
        for symbol_state in self.futures_symbols.values_mut() {
            symbol_state.event_contract = None;
            symbol_state.latest_deals = None;
            symbol_state.depth = None;
            symbol_state.depth_step_by_channel.clear();
            symbol_state.funding_rate_live = None;
            symbol_state.latest_kline_by_interval.clear();
        }
        for symbol_state in self.futures_orphan_symbols.values_mut() {
            symbol_state.event_contract = None;
            symbol_state.latest_deals = None;
            symbol_state.depth = None;
            symbol_state.depth_step_by_channel.clear();
            symbol_state.funding_rate_live = None;
            symbol_state.latest_kline_by_interval.clear();
        }
    }

    fn apply_spot_message(&mut self, message: &MexcSpotWsMessage) {
        match message {
            MexcSpotWsMessage::SessionStart(_)
            | MexcSpotWsMessage::Ack(_)
            | MexcSpotWsMessage::RawText(_) => {}
            MexcSpotWsMessage::AggTrades(event) => {
                if let Some(symbol) = &event.symbol {
                    self.spot_symbol_state_for_live_update(symbol).latest_trades =
                        Some(event.data.clone());
                }
            }
            MexcSpotWsMessage::IncreaseDepth(_) | MexcSpotWsMessage::IncreaseDepthBatch(_) => {}
            MexcSpotWsMessage::AggDepth(event) => {
                if let Some(symbol) = &event.symbol {
                    self.spot_symbol_state_for_live_update(symbol).agg_depth =
                        Some(event.data.clone());
                }
            }
            MexcSpotWsMessage::LimitDepth(event) => {
                if let Some(symbol) = &event.symbol {
                    self.spot_symbol_state_for_live_update(symbol).limit_depth =
                        Some(event.data.clone());
                }
            }
            MexcSpotWsMessage::BookTicker(event) => {
                if let Some(symbol) = &event.symbol {
                    self.spot_symbol_state_for_live_update(symbol)
                        .book_ticker_snapshot = Some(SpotBookTicker {
                        symbol: symbol.clone(),
                        bid_price: Some(event.data.bid_price.clone()),
                        bid_qty: Some(event.data.bid_quantity.clone()),
                        ask_price: Some(event.data.ask_price.clone()),
                        ask_qty: Some(event.data.ask_quantity.clone()),
                    });
                }
            }
            MexcSpotWsMessage::BookTickerBatch(event) => {
                if let Some(symbol) = &event.symbol {
                    self.spot_symbol_state_for_live_update(symbol)
                        .batch_book_ticker = Some(event.data.clone());
                }
            }
            MexcSpotWsMessage::AggBookTicker(event) => {
                if let Some(symbol) = &event.symbol {
                    self.spot_symbol_state_for_live_update(symbol)
                        .agg_book_ticker = Some(event.data.clone());
                }
            }
            MexcSpotWsMessage::Kline(event) => {
                if let Some(symbol) = &event.symbol {
                    self.spot_symbol_state_for_live_update(symbol)
                        .latest_kline_by_interval
                        .insert(event.data.interval.clone(), event.data.clone());
                }
            }
            MexcSpotWsMessage::MiniTicker(event) => {
                if let Some(symbol) = &event.symbol {
                    self.spot_symbol_state_for_live_update(symbol)
                        .mini_ticker_by_channel
                        .insert(event.channel.clone(), event.data.clone());
                }
            }
            MexcSpotWsMessage::MiniTickers(event) => {
                self.global_spot_mini_tickers_by_channel
                    .insert(event.channel.clone(), event.data.clone());
                for item in &event.data.items {
                    self.spot_symbol_state_for_live_update(&item.symbol)
                        .mini_ticker_by_channel
                        .insert(event.channel.clone(), item.clone());
                }
            }
        }
    }

    fn apply_futures_message(&mut self, message: &MexcFuturesWsMessage) {
        match message {
            MexcFuturesWsMessage::SessionStart(_)
            | MexcFuturesWsMessage::Ack(_)
            | MexcFuturesWsMessage::Raw(_) => {}
            MexcFuturesWsMessage::Tickers(event) => {
                for item in &event.data {
                    self.futures_symbol_state_for_live_update(&item.symbol)
                        .ticker = Some(item.clone());
                }
            }
            MexcFuturesWsMessage::Ticker(event) => {
                if let Some(symbol) = &event.symbol {
                    self.futures_symbol_state_for_live_update(symbol).ticker =
                        Some(event.data.clone());
                }
            }
            MexcFuturesWsMessage::Deals(event) => {
                if let Some(symbol) = &event.symbol {
                    self.futures_symbol_state_for_live_update(symbol)
                        .latest_deals = Some(event.data.to_vec());
                }
            }
            MexcFuturesWsMessage::Depth(event) => {
                if let Some(symbol) = &event.symbol {
                    self.futures_symbol_state_for_live_update(symbol).depth =
                        Some(event.data.clone());
                }
            }
            MexcFuturesWsMessage::DepthStep(event) => {
                if let Some(symbol) = &event.symbol {
                    self.futures_symbol_state_for_live_update(symbol)
                        .depth_step_by_channel
                        .insert(event.channel.clone(), event.data.clone());
                }
            }
            MexcFuturesWsMessage::DepthFull(event) => {
                if let Some(symbol) = &event.symbol {
                    self.futures_symbol_state_for_live_update(symbol).depth =
                        Some(event.data.clone());
                }
            }
            MexcFuturesWsMessage::FundingRate(event) => {
                if let Some(symbol) = &event.symbol {
                    self.futures_symbol_state_for_live_update(symbol)
                        .funding_rate_live = Some(event.data.clone());
                }
            }
            MexcFuturesWsMessage::IndexPrice(event) => {
                if let Some(symbol) = &event.symbol {
                    self.futures_symbol_state_for_live_update(symbol)
                        .index_price = Some(event.data.clone());
                }
            }
            MexcFuturesWsMessage::FairPrice(event) => {
                if let Some(symbol) = &event.symbol {
                    self.futures_symbol_state_for_live_update(symbol).fair_price =
                        Some(event.data.clone());
                }
            }
            MexcFuturesWsMessage::Kline(event) => {
                if let Some(symbol) = &event.symbol {
                    self.futures_symbol_state_for_live_update(symbol)
                        .latest_kline_by_interval
                        .insert(event.data.interval.clone(), event.data.clone());
                }
            }
            MexcFuturesWsMessage::Contract(event) => {
                if let Some(symbol) = &event.symbol {
                    self.promote_futures_symbol(symbol);
                    self.futures_symbols
                        .get_mut(symbol)
                        .expect("promoted futures symbol present")
                        .contract = Some(event.data.clone());
                }
            }
            MexcFuturesWsMessage::EventContract(event) => {
                self.futures_symbol_state_for_live_update(&event.data.symbol)
                    .event_contract = Some(event.data.clone());
            }
        }
    }
}

impl MexcFuturesSymbolState {
    fn hydrate_from_snapshot_ticker(&mut self, ticker: &FuturesTicker) {
        if let Some(index_price) = ticker.index_price {
            self.index_price = Some(FuturesWsPricePoint {
                symbol: ticker.symbol.clone(),
                price: index_price,
            });
        }
        if let Some(fair_price) = ticker.fair_price {
            self.fair_price = Some(FuturesWsPricePoint {
                symbol: ticker.symbol.clone(),
                price: fair_price,
            });
        }
        if let Some(funding_rate) = ticker.funding_rate {
            self.funding_rate_snapshot = Some(FuturesFundingRate {
                symbol: ticker.symbol.clone(),
                funding_rate,
                max_funding_rate: None,
                min_funding_rate: None,
                collect_cycle: None,
                next_settle_time: None,
                timestamp: ticker.timestamp,
                extra: BTreeMap::new(),
            });
        }
    }
}

fn diff_contract_fields(
    left: &FuturesContractInfo,
    right: &FuturesContractInfo,
) -> Vec<MexcFuturesContractFieldChange> {
    let left = serde_json::to_value(left).unwrap_or(Value::Null);
    let right = serde_json::to_value(right).unwrap_or(Value::Null);
    let (Value::Object(left), Value::Object(right)) = (left, right) else {
        return Vec::new();
    };

    let mut changed = left
        .keys()
        .chain(right.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter_map(|key| {
            let before = left.get(&key).cloned().unwrap_or(Value::Null);
            let after = right.get(&key).cloned().unwrap_or(Value::Null);
            (before != after).then_some(MexcFuturesContractFieldChange {
                field: key,
                before,
                after,
            })
        })
        .collect::<Vec<_>>();
    changed.sort_by(|left, right| left.field.cmp(&right.field));
    changed
}

#[cfg(test)]
mod tests {
    use super::{
        MexcFuturesContractChange, MexcFuturesContractChangeKind, MexcFuturesContractFieldChange,
        MexcFuturesContractSnapshotDelta, MexcFuturesSymbolState, MexcPublicMetadataRefreshReport,
        MexcPublicState,
    };
    use crate::{
        FuturesContractInfo, FuturesInsuranceBalance, FuturesTicker, MexcFuturesBootstrapSnapshot,
        MexcPublicBootstrapSnapshot, MexcPublicMetadataRefreshSnapshot, MexcSpotBootstrapSnapshot,
        SpotDefaultSymbolsResponse, SpotExchangeInfo, SpotOfflineSymbol,
        SpotOfflineSymbolsResponse, SpotServerTime,
    };
    use serde_json::Value;
    use std::collections::BTreeMap;

    #[test]
    fn from_snapshot_hydrates_futures_reference_fields_from_bulk_tickers() {
        let snapshot = MexcPublicBootstrapSnapshot {
            spot: MexcSpotBootstrapSnapshot {
                server_time: SpotServerTime { server_time: 1 },
                default_symbols: SpotDefaultSymbolsResponse {
                    code: Some(0),
                    data: Vec::new(),
                    msg: Some("ok".to_string()),
                    timestamp: Some(1),
                },
                offline_symbols: SpotOfflineSymbolsResponse { data: Vec::new() },
                exchange_info: SpotExchangeInfo {
                    timezone: "UTC".to_string(),
                    server_time: 1,
                    rate_limits: Vec::new(),
                    exchange_filters: Vec::new(),
                    symbols: Vec::new(),
                },
                ticker_24hr: Vec::new(),
                price_tickers: Vec::new(),
                book_tickers: Vec::new(),
            },
            futures: MexcFuturesBootstrapSnapshot {
                server_time: 2,
                contracts: vec![FuturesContractInfo {
                    symbol: "BTC_USDT".to_string(),
                    ..FuturesContractInfo::default()
                }],
                tickers: vec![FuturesTicker {
                    contract_id: Some(1),
                    symbol: "BTC_USDT".to_string(),
                    last_price: 60000.0,
                    bid1: Some(59999.0),
                    ask1: Some(60001.0),
                    volume24: 123.0,
                    amount24: Some(456.0),
                    hold_vol: Some(789.0),
                    lower24_price: Some(58000.0),
                    high24_price: Some(61000.0),
                    rise_fall_rate: Some(0.01),
                    rise_fall_value: Some(600.0),
                    index_price: Some(59998.5),
                    fair_price: Some(59999.5),
                    funding_rate: Some(0.0001),
                    max_bid_price: Some(62000.0),
                    min_ask_price: Some(57000.0),
                    timestamp: Some(1710000000000),
                    extra: BTreeMap::new(),
                }],
                transferable_currencies: vec!["USDT".to_string()],
                insurance_balances: vec![FuturesInsuranceBalance {
                    symbol: "BTC_USDT".to_string(),
                    currency: "USDT".to_string(),
                    available: 42.0,
                    timestamp: 1710000000000,
                }],
            },
        };

        let state = MexcPublicState::from_snapshot(&snapshot);
        let symbol_state = state.futures_symbols.get("BTC_USDT").unwrap();

        assert_eq!(symbol_state.ticker.as_ref().unwrap().last_price, 60000.0);
        assert_eq!(symbol_state.index_price.as_ref().unwrap().price, 59998.5);
        assert_eq!(symbol_state.fair_price.as_ref().unwrap().price, 59999.5);
        assert_eq!(
            symbol_state
                .funding_rate_snapshot
                .as_ref()
                .unwrap()
                .funding_rate,
            0.0001
        );
        assert_eq!(
            symbol_state
                .funding_rate_snapshot
                .as_ref()
                .unwrap()
                .timestamp,
            Some(1710000000000)
        );
        assert_eq!(state.spot_server_time, Some(1));
        assert_eq!(state.futures_server_time, Some(2));
    }

    #[test]
    fn reconnect_reset_clears_stream_derived_spot_state() {
        let mut state = MexcPublicState::default();
        let spot = state.spot_symbols.entry("BTCUSDT".to_string()).or_default();
        spot.latest_trades = Some(crate::spot_proto::PublicAggreDealsV3Api {
            deals: Vec::new(),
            event_type: "spot@public.aggre.deals.v3.api.pb@100ms".to_string(),
        });
        spot.agg_depth = Some(crate::spot_proto::PublicAggreDepthsV3Api::default());
        spot.limit_depth = Some(crate::spot_proto::PublicLimitDepthsV3Api::default());
        spot.mini_ticker_by_channel.insert(
            "spot@public.miniTicker.v3.api.pb@BTCUSDT@24H".to_string(),
            crate::spot_proto::PublicMiniTickerV3Api::default(),
        );
        state.global_spot_mini_tickers_by_channel.insert(
            "spot@public.miniTickers.v3.api.pb@24H".to_string(),
            crate::spot_proto::PublicMiniTickersV3Api::default(),
        );

        state.reset_after_spot_session_restart();

        let spot = state.spot_symbols.get("BTCUSDT").unwrap();
        assert!(spot.latest_trades.is_none());
        assert!(spot.agg_depth.is_none());
        assert!(spot.limit_depth.is_none());
        assert!(spot.mini_ticker_by_channel.is_empty());
        assert!(state.global_spot_mini_tickers_by_channel.is_empty());
    }

    #[test]
    fn reconnect_reset_clears_stream_derived_futures_state() {
        let mut state = MexcPublicState::default();
        let futures = state
            .futures_symbols
            .entry("BTC_USDT".to_string())
            .or_default();
        futures.event_contract = Some(crate::FuturesEventContract {
            contract_id: crate::MexcFlexibleString::String("1".to_string()),
            symbol: "BTC_USDT".to_string(),
            base_coin: "BTC".to_string(),
            quote_coin: "USDT".to_string(),
            base_coin_name: "Bitcoin".to_string(),
            quote_coin_name: "Tether".to_string(),
            settle_coin: "USDT".to_string(),
            base_coin_icon_url: None,
            invest_min_amount: None,
            invest_max_amount: None,
            amount_scale: None,
            pay_rate_scale: None,
            index_price_scale: None,
            available_scale: None,
            extra: BTreeMap::new(),
        });
        futures.latest_deals = Some(Vec::new());
        futures.depth = Some(crate::FuturesDepthSnapshot {
            cts: None,
            asks: Vec::new(),
            bids: Vec::new(),
            version: 1,
        });
        futures.depth_step_by_channel.insert(
            "push.depth.step".to_string(),
            crate::FuturesDepthStepSnapshot {
                ask_market_level_price: None,
                bid_market_level_price: None,
                asks: Vec::new(),
                bids: Vec::new(),
                version: 1,
                ct: None,
            },
        );
        futures.funding_rate_live = Some(crate::FuturesWsFundingRatePoint {
            symbol: "BTC_USDT".to_string(),
            rate: 0.0,
            next_settle_time: None,
        });

        state.reset_after_futures_session_restart();

        let futures = state.futures_symbols.get("BTC_USDT").unwrap();
        assert!(futures.event_contract.is_none());
        assert!(futures.latest_deals.is_none());
        assert!(futures.depth.is_none());
        assert!(futures.depth_step_by_channel.is_empty());
        assert!(futures.funding_rate_live.is_none());
    }

    #[test]
    fn hydrate_futures_contract_snapshot_clears_missing_contracts() {
        let mut state = MexcPublicState::default();
        state.futures_symbols.insert(
            "OLD_USDT".to_string(),
            MexcFuturesSymbolState {
                contract: Some(FuturesContractInfo {
                    symbol: "OLD_USDT".to_string(),
                    ..FuturesContractInfo::default()
                }),
                ..Default::default()
            },
        );

        let report = state.hydrate_futures_contract_snapshot(&[FuturesContractInfo {
            symbol: "BTC_USDT".to_string(),
            ..FuturesContractInfo::default()
        }]);

        assert!(
            state
                .futures_symbols
                .get("BTC_USDT")
                .and_then(|symbol| symbol.contract.as_ref())
                .is_some()
        );
        assert!(
            state
                .futures_symbols
                .get("OLD_USDT")
                .and_then(|symbol| symbol.contract.as_ref())
                .is_none()
        );
        assert_eq!(
            report,
            MexcFuturesContractSnapshotDelta {
                refreshed_contracts: 1,
                added: 1,
                added_symbols: vec!["BTC_USDT".to_string()],
                updated: 0,
                updated_symbols: Vec::new(),
                removed: 1,
                removed_symbols: vec!["OLD_USDT".to_string()],
                unchanged: 0,
                changes: vec![
                    MexcFuturesContractChange {
                        symbol: "BTC_USDT".to_string(),
                        kind: MexcFuturesContractChangeKind::Added,
                        changed_fields: Vec::new(),
                        field_changes: Vec::new(),
                    },
                    MexcFuturesContractChange {
                        symbol: "OLD_USDT".to_string(),
                        kind: MexcFuturesContractChangeKind::Removed,
                        changed_fields: Vec::new(),
                        field_changes: Vec::new(),
                    },
                ],
            }
        );
    }

    #[test]
    fn hydrate_futures_contract_snapshot_clears_transient_orphan_state() {
        let mut state = MexcPublicState::default();
        state.futures_orphan_symbols.insert(
            "STALE_USDT".to_string(),
            MexcFuturesSymbolState {
                funding_rate_live: Some(crate::FuturesWsFundingRatePoint {
                    symbol: "STALE_USDT".to_string(),
                    rate: 0.0,
                    next_settle_time: None,
                }),
                ..Default::default()
            },
        );

        state.hydrate_futures_contract_snapshot(&[FuturesContractInfo {
            symbol: "BTC_USDT".to_string(),
            ..FuturesContractInfo::default()
        }]);
        let report = state.handoff_report();

        assert!(state.futures_orphan_symbols.contains_key("STALE_USDT"));
        assert_eq!(report.futures.orphan_symbols, 1);
        assert_eq!(report.futures.orphan_symbols_with_other_state, 0);
        assert!(
            !state
                .futures_orphan_symbols
                .get("STALE_USDT")
                .expect("stale orphan is retained")
                .has_other_state()
        );
    }

    #[test]
    fn hydrate_futures_contract_snapshot_reports_changed_fields_for_updates() {
        let mut state = MexcPublicState::default();
        state.futures_symbols.insert(
            "BTC_USDT".to_string(),
            MexcFuturesSymbolState {
                contract: Some(FuturesContractInfo {
                    symbol: "BTC_USDT".to_string(),
                    max_leverage: Some(100),
                    state: Some(1),
                    ..FuturesContractInfo::default()
                }),
                ..Default::default()
            },
        );

        let report = state.hydrate_futures_contract_snapshot(&[FuturesContractInfo {
            symbol: "BTC_USDT".to_string(),
            max_leverage: Some(125),
            state: Some(0),
            ..FuturesContractInfo::default()
        }]);

        assert_eq!(report.updated, 1);
        assert_eq!(report.updated_symbols, vec!["BTC_USDT".to_string()]);
        assert_eq!(
            report.changes,
            vec![MexcFuturesContractChange {
                symbol: "BTC_USDT".to_string(),
                kind: MexcFuturesContractChangeKind::Updated,
                changed_fields: vec!["maxLeverage".to_string(), "state".to_string()],
                field_changes: vec![
                    MexcFuturesContractFieldChange {
                        field: "maxLeverage".to_string(),
                        before: Value::from(100),
                        after: Value::from(125),
                    },
                    MexcFuturesContractFieldChange {
                        field: "state".to_string(),
                        before: Value::from(1),
                        after: Value::from(0),
                    },
                ],
            }]
        );
    }

    #[test]
    fn handoff_report_counts_futures_without_contract_with_other_state() {
        let mut state = MexcPublicState::default();
        state.futures_symbols.insert(
            "BTC_USDT".to_string(),
            MexcFuturesSymbolState {
                contract: Some(FuturesContractInfo {
                    symbol: "BTC_USDT".to_string(),
                    ..FuturesContractInfo::default()
                }),
                ..Default::default()
            },
        );
        state.futures_orphan_symbols.insert(
            "ETH_USDT".to_string(),
            MexcFuturesSymbolState {
                ticker: Some(FuturesTicker {
                    contract_id: None,
                    symbol: "ETH_USDT".to_string(),
                    last_price: 1.0,
                    bid1: None,
                    ask1: None,
                    volume24: 0.0,
                    amount24: None,
                    hold_vol: None,
                    lower24_price: None,
                    high24_price: None,
                    rise_fall_rate: None,
                    rise_fall_value: None,
                    index_price: None,
                    fair_price: None,
                    funding_rate: None,
                    max_bid_price: None,
                    min_ask_price: None,
                    timestamp: None,
                    extra: BTreeMap::new(),
                }),
                ..Default::default()
            },
        );
        state
            .futures_orphan_symbols
            .insert("SOL_USDT".to_string(), MexcFuturesSymbolState::default());

        let report = state.handoff_report();

        assert_eq!(report.futures.total_symbols, 1);
        assert_eq!(report.futures.orphan_symbols, 2);
        assert_eq!(
            report.futures.orphan_symbols_list,
            vec!["ETH_USDT".to_string(), "SOL_USDT".to_string()]
        );
        assert_eq!(report.futures.with_contract, 1);
        assert_eq!(report.futures.without_contract, 0);
        assert_eq!(
            report.futures.without_contract_symbols,
            Vec::<String>::new()
        );
        assert_eq!(report.futures.orphan_symbols_with_other_state, 1);
        assert_eq!(
            report.futures.orphan_symbols_with_other_state_list,
            vec!["ETH_USDT".to_string()]
        );
        assert_eq!(report.futures.with_ticker, 0);
    }

    #[test]
    fn handoff_report_lists_symbols_with_event_contract() {
        let mut state = MexcPublicState::default();
        state.futures_symbols.insert(
            "BTC_USDT".to_string(),
            MexcFuturesSymbolState {
                event_contract: Some(crate::FuturesEventContract {
                    contract_id: crate::MexcFlexibleString::String("1".to_string()),
                    symbol: "BTC_USDT".to_string(),
                    base_coin: "BTC".to_string(),
                    quote_coin: "USDT".to_string(),
                    base_coin_name: "Bitcoin".to_string(),
                    quote_coin_name: "Tether".to_string(),
                    settle_coin: "USDT".to_string(),
                    base_coin_icon_url: None,
                    invest_min_amount: None,
                    invest_max_amount: None,
                    amount_scale: None,
                    pay_rate_scale: None,
                    index_price_scale: None,
                    available_scale: None,
                    extra: BTreeMap::new(),
                }),
                ..Default::default()
            },
        );
        state
            .futures_symbols
            .insert("ETH_USDT".to_string(), MexcFuturesSymbolState::default());

        let report = state.handoff_report();

        assert_eq!(report.futures.with_event_contract, 1);
        assert_eq!(
            report.futures.with_event_contract_symbols,
            vec!["BTC_USDT".to_string()]
        );
    }

    #[test]
    fn from_snapshot_keeps_metadata_only_and_orphan_symbols_out_of_tradable_maps() {
        let snapshot = MexcPublicBootstrapSnapshot {
            spot: MexcSpotBootstrapSnapshot {
                server_time: SpotServerTime { server_time: 1 },
                default_symbols: SpotDefaultSymbolsResponse {
                    code: Some(0),
                    data: vec!["BTCUSDT".to_string()],
                    msg: Some("ok".to_string()),
                    timestamp: Some(1),
                },
                offline_symbols: SpotOfflineSymbolsResponse {
                    data: vec![SpotOfflineSymbol {
                        symbol: "OFFLINEUSDT".to_string(),
                        state: 2,
                        offline_time: Some(1),
                    }],
                },
                exchange_info: SpotExchangeInfo {
                    timezone: "UTC".to_string(),
                    server_time: 1,
                    rate_limits: Vec::new(),
                    exchange_filters: Vec::new(),
                    symbols: vec![crate::SpotSymbolInfo {
                        symbol: "BTCUSDT".to_string(),
                        status: Some("1".to_string()),
                        base_asset: None,
                        base_asset_precision: None,
                        quote_asset: None,
                        quote_precision: None,
                        quote_asset_precision: None,
                        base_commission_precision: None,
                        quote_commission_precision: None,
                        order_types: Vec::new(),
                        is_spot_trading_allowed: None,
                        is_margin_trading_allowed: None,
                        quote_amount_precision: None,
                        base_size_precision: None,
                        permissions: Vec::new(),
                        filters: Vec::new(),
                        max_quote_amount: None,
                        maker_commission: None,
                        taker_commission: None,
                        quote_amount_precision_market: None,
                        max_quote_amount_market: None,
                        full_name: None,
                        trade_side_type: None,
                        contract_address: None,
                        concept_plate_ids: Vec::new(),
                        st: None,
                        extra: BTreeMap::new(),
                    }],
                },
                ticker_24hr: Vec::new(),
                price_tickers: Vec::new(),
                book_tickers: Vec::new(),
            },
            futures: MexcFuturesBootstrapSnapshot {
                server_time: 2,
                contracts: vec![FuturesContractInfo {
                    symbol: "BTC_USDT".to_string(),
                    ..FuturesContractInfo::default()
                }],
                tickers: vec![
                    FuturesTicker {
                        contract_id: None,
                        symbol: "BTC_USDT".to_string(),
                        last_price: 1.0,
                        bid1: None,
                        ask1: None,
                        volume24: 0.0,
                        amount24: None,
                        hold_vol: None,
                        lower24_price: None,
                        high24_price: None,
                        rise_fall_rate: None,
                        rise_fall_value: None,
                        index_price: None,
                        fair_price: None,
                        funding_rate: None,
                        max_bid_price: None,
                        min_ask_price: None,
                        timestamp: None,
                        extra: BTreeMap::new(),
                    },
                    FuturesTicker {
                        contract_id: None,
                        symbol: "ETH_USDT".to_string(),
                        last_price: 1.0,
                        bid1: None,
                        ask1: None,
                        volume24: 0.0,
                        amount24: None,
                        hold_vol: None,
                        lower24_price: None,
                        high24_price: None,
                        rise_fall_rate: None,
                        rise_fall_value: None,
                        index_price: None,
                        fair_price: None,
                        funding_rate: None,
                        max_bid_price: None,
                        min_ask_price: None,
                        timestamp: None,
                        extra: BTreeMap::new(),
                    },
                ],
                transferable_currencies: vec!["USDT".to_string()],
                insurance_balances: vec![FuturesInsuranceBalance {
                    symbol: "ETH_USDT".to_string(),
                    currency: "USDT".to_string(),
                    available: 1.0,
                    timestamp: 2,
                }],
            },
        };

        let state = MexcPublicState::from_snapshot(&snapshot);

        assert!(state.spot_symbols.contains_key("BTCUSDT"));
        assert!(!state.spot_symbols.contains_key("OFFLINEUSDT"));
        assert!(state.spot_metadata_only_symbols.contains_key("OFFLINEUSDT"));
        assert!(state.futures_symbols.contains_key("BTC_USDT"));
        assert!(!state.futures_symbols.contains_key("ETH_USDT"));
        assert!(state.futures_orphan_symbols.contains_key("ETH_USDT"));
    }

    #[test]
    fn hydrate_futures_contract_updates_does_not_clear_unmentioned_contracts() {
        let mut state = MexcPublicState::default();
        state.futures_symbols.insert(
            "BTC_USDT".to_string(),
            MexcFuturesSymbolState {
                contract: Some(FuturesContractInfo {
                    symbol: "BTC_USDT".to_string(),
                    ..FuturesContractInfo::default()
                }),
                ..Default::default()
            },
        );
        state.futures_symbols.insert(
            "ETH_USDT".to_string(),
            MexcFuturesSymbolState {
                contract: Some(FuturesContractInfo {
                    symbol: "ETH_USDT".to_string(),
                    max_leverage: Some(50),
                    ..FuturesContractInfo::default()
                }),
                ..Default::default()
            },
        );

        let report = state.hydrate_futures_contract_updates(&[FuturesContractInfo {
            symbol: "BTC_USDT".to_string(),
            max_leverage: Some(125),
            ..FuturesContractInfo::default()
        }]);

        assert_eq!(report.refreshed_contracts, 1);
        assert_eq!(report.updated, 1);
        assert_eq!(report.removed, 0);
        assert_eq!(
            state
                .futures_symbols
                .get("ETH_USDT")
                .and_then(|symbol| symbol.contract.as_ref())
                .map(|item| item.max_leverage),
            Some(Some(50))
        );
        assert_eq!(
            state
                .futures_symbols
                .get("BTC_USDT")
                .and_then(|symbol| symbol.contract.as_ref())
                .map(|item| item.max_leverage),
            Some(Some(125))
        );
    }

    #[test]
    fn hydrate_public_metadata_snapshot_updates_global_state_and_seeds_symbols() {
        let mut state = MexcPublicState::default();
        let report = state.hydrate_public_metadata_snapshot(&MexcPublicMetadataRefreshSnapshot {
            spot_server_time: SpotServerTime { server_time: 10 },
            spot_default_symbols: SpotDefaultSymbolsResponse {
                code: Some(0),
                data: vec!["BTCUSDT".to_string()],
                msg: Some("ok".to_string()),
                timestamp: Some(10),
            },
            spot_offline_symbols: SpotOfflineSymbolsResponse {
                data: vec![SpotOfflineSymbol {
                    symbol: "OFFLINEUSDT".to_string(),
                    state: 2,
                    offline_time: Some(11),
                }],
            },
            spot_exchange_info: SpotExchangeInfo {
                timezone: "UTC".to_string(),
                server_time: 10,
                rate_limits: Vec::new(),
                exchange_filters: Vec::new(),
                symbols: vec![crate::SpotSymbolInfo {
                    symbol: "ETHUSDT".to_string(),
                    status: Some("1".to_string()),
                    base_asset: None,
                    base_asset_precision: None,
                    quote_asset: None,
                    quote_precision: None,
                    quote_asset_precision: None,
                    base_commission_precision: None,
                    quote_commission_precision: None,
                    order_types: Vec::new(),
                    is_spot_trading_allowed: None,
                    is_margin_trading_allowed: None,
                    quote_amount_precision: None,
                    base_size_precision: None,
                    permissions: Vec::new(),
                    filters: Vec::new(),
                    max_quote_amount: None,
                    maker_commission: None,
                    taker_commission: None,
                    quote_amount_precision_market: None,
                    max_quote_amount_market: None,
                    full_name: None,
                    trade_side_type: None,
                    contract_address: None,
                    concept_plate_ids: Vec::new(),
                    st: None,
                    extra: BTreeMap::new(),
                }],
            },
            futures_server_time: 12,
            futures_transferable_currencies: vec!["USDT".to_string()],
            futures_insurance_balances: vec![FuturesInsuranceBalance {
                symbol: "BTC_USDT".to_string(),
                currency: "USDT".to_string(),
                available: 42.0,
                timestamp: 12,
            }],
        });

        assert_eq!(
            report,
            MexcPublicMetadataRefreshReport {
                spot_server_time_before: None,
                spot_server_time_after: 10,
                spot_server_time_changed: true,
                spot_default_symbols_changed: true,
                spot_default_symbol_count_before: 0,
                spot_default_symbol_count_after: 1,
                spot_offline_symbols_changed: true,
                spot_offline_symbol_count_before: 0,
                spot_offline_symbol_count_after: 1,
                spot_exchange_info_changed: true,
                spot_exchange_symbol_count_before: 0,
                spot_exchange_symbol_count_after: 1,
                futures_server_time_before: None,
                futures_server_time_after: 12,
                futures_server_time_changed: true,
                futures_transferable_currencies_changed: true,
                futures_transferable_currency_count_before: 0,
                futures_transferable_currency_count_after: 1,
                futures_insurance_balances_changed: true,
                futures_insurance_balance_count_before: 0,
                futures_insurance_balance_count_after: 1,
            }
        );
        assert_eq!(state.spot_server_time, Some(10));
        assert_eq!(state.futures_server_time, Some(12));
        assert!(state.spot_symbols.contains_key("ETHUSDT"));
        assert!(!state.spot_symbols.contains_key("BTCUSDT"));
        assert!(state.spot_metadata_only_symbols.contains_key("BTCUSDT"));
        assert!(state.spot_metadata_only_symbols.contains_key("OFFLINEUSDT"));
        assert!(!state.futures_symbols.contains_key("BTC_USDT"));
        assert!(state.futures_orphan_symbols.contains_key("BTC_USDT"));
    }
}
