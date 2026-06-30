use crate::model::{
    FuturesApiEnvelope, FuturesContractInfo, FuturesDeal, FuturesDepthSnapshot, FuturesFairPrice,
    FuturesFundingRate, FuturesFundingRateHistoryItem, FuturesIndexPrice, FuturesInsuranceBalance,
    FuturesInsuranceBalanceHistoryItem, FuturesKlineSeries, FuturesPagination, FuturesTicker,
    FuturesWsPricePoint, OneOrMany, SpotAggTrade, SpotAveragePrice, SpotBookTicker,
    SpotDefaultSymbolsResponse, SpotExchangeInfo, SpotHistoricalArchivePeriod,
    SpotHistoricalDataKind, SpotHistoricalDownloadFile, SpotHistoricalDownloadNode,
    SpotHistoricalDownloadResponse, SpotHistoricalKlineInterval, SpotHistoricalSymbolDirectory,
    SpotKlineRecord, SpotOfflineSymbolsResponse, SpotOrderBook, SpotPriceTicker, SpotServerTime,
    SpotTicker24Hr, SpotTrade,
};
use anyhow::{Context, Result, anyhow};
use futures::{StreamExt, TryStreamExt, stream};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

const SPOT_BASE_URL: &str = "https://api.mexc.com";
const FUTURES_BASE_URL: &str = "https://api.mexc.com";
const SPOT_HISTORY_BASE_URL: &str = "https://www.mexc.co";
const FUTURES_REFERENCE_SEQUENTIAL_DELAY_MS: u64 = 25;

#[derive(Clone)]
pub struct MexcPublicRestClient {
    http: Client,
    spot_base_url: String,
    futures_base_url: String,
    spot_history_base_url: String,
}

impl Default for MexcPublicRestClient {
    fn default() -> Self {
        let http = Client::builder()
            .user_agent("rust-market-data/mexc")
            .build()
            .expect("reqwest client");
        Self {
            http,
            spot_base_url: SPOT_BASE_URL.to_string(),
            futures_base_url: FUTURES_BASE_URL.to_string(),
            spot_history_base_url: SPOT_HISTORY_BASE_URL.to_string(),
        }
    }
}

impl MexcPublicRestClient {
    pub fn new(
        spot_base_url: impl Into<String>,
        futures_base_url: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            http: Client::builder()
                .user_agent("rust-market-data/mexc")
                .build()
                .context("build reqwest client")?,
            spot_base_url: spot_base_url.into(),
            futures_base_url: futures_base_url.into(),
            spot_history_base_url: SPOT_HISTORY_BASE_URL.to_string(),
        })
    }

    pub fn with_spot_history_base_url(mut self, spot_history_base_url: impl Into<String>) -> Self {
        self.spot_history_base_url = spot_history_base_url.into();
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

    async fn get_spot_one_or_many<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<OneOrMany<T>> {
        let url = format!("{}{}", self.spot_base_url, path);
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

        let value = serde_json::from_str::<Value>(&body).with_context(|| {
            let snippet = body.chars().take(300).collect::<String>();
            format!("decode JSON response body: {snippet}")
        })?;

        match value {
            Value::Array(items) => serde_json::from_value::<Vec<T>>(Value::Array(items))
                .map(OneOrMany::Many)
                .with_context(|| {
                    let snippet = body.chars().take(300).collect::<String>();
                    format!("decode JSON response body as array: {snippet}")
                }),
            Value::Object(map) => serde_json::from_value::<T>(Value::Object(map))
                .map(OneOrMany::One)
                .with_context(|| {
                    let snippet = body.chars().take(300).collect::<String>();
                    format!("decode JSON response body as object: {snippet}")
                }),
            _ => {
                let snippet = body.chars().take(300).collect::<String>();
                Err(anyhow!(
                    "decode JSON response body as object-or-array: {snippet}"
                ))
            }
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

    fn validated_concurrency(concurrency: usize) -> usize {
        concurrency.max(1)
    }

    async fn retry_futures_reference_call<T, F, Fut>(
        &self,
        symbol: String,
        label: &'static str,
        fetcher: F,
    ) -> Result<T>
    where
        F: Fn(MexcPublicRestClient, String) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut last_error = None;
        for attempt in 1..=3u64 {
            match fetcher(self.clone(), symbol.clone()).await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let is_rate_limit = error.to_string().contains("Requests are too frequent");
                    last_error = Some(error);
                    if attempt < 3 {
                        let delay_ms = if is_rate_limit {
                            1000 * attempt
                        } else {
                            200 * attempt
                        };
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }

        Err(last_error.expect("retry must capture an error"))
            .with_context(|| format!("fetch {label} for {symbol} after retries"))
    }

    pub async fn spot_ping(&self) -> Result<()> {
        let _: serde_json::Value = self.get_spot("/api/v3/ping", &[]).await?;
        Ok(())
    }

    pub async fn spot_server_time(&self) -> Result<SpotServerTime> {
        self.get_spot("/api/v3/time", &[]).await
    }

    pub async fn spot_default_symbols(&self) -> Result<SpotDefaultSymbolsResponse> {
        self.get_spot("/api/v3/defaultSymbols", &[]).await
    }

    pub async fn spot_offline_symbols(&self) -> Result<SpotOfflineSymbolsResponse> {
        self.get_spot("/api/v3/symbol/offline", &[]).await
    }

    pub async fn spot_exchange_info(&self) -> Result<SpotExchangeInfo> {
        self.get_spot("/api/v3/exchangeInfo", &[]).await
    }

    pub async fn spot_order_book(&self, symbol: &str, limit: Option<u16>) -> Result<SpotOrderBook> {
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
    ) -> Result<Vec<SpotTrade>> {
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
    ) -> Result<Vec<SpotAggTrade>> {
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
    ) -> Result<Vec<SpotKlineRecord>> {
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
        self.get_spot("/api/v3/klines", &query).await
    }

    pub async fn spot_average_price(&self, symbol: &str) -> Result<SpotAveragePrice> {
        self.get_spot("/api/v3/avgPrice", &[("symbol", symbol.to_string())])
            .await
    }

    pub async fn spot_ticker_24hr(
        &self,
        symbol: Option<&str>,
    ) -> Result<OneOrMany<SpotTicker24Hr>> {
        let mut query = Vec::new();
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        self.get_spot_one_or_many("/api/v3/ticker/24hr", &query)
            .await
    }

    pub async fn spot_ticker_24hr_all(&self) -> Result<Vec<SpotTicker24Hr>> {
        match self.spot_ticker_24hr(None).await? {
            OneOrMany::One(item) => Ok(vec![item]),
            OneOrMany::Many(items) => Ok(items),
        }
    }

    pub async fn spot_price_ticker(
        &self,
        symbol: Option<&str>,
    ) -> Result<OneOrMany<SpotPriceTicker>> {
        let mut query = Vec::new();
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        self.get_spot_one_or_many("/api/v3/ticker/price", &query)
            .await
    }

    pub async fn spot_price_ticker_all(&self) -> Result<Vec<SpotPriceTicker>> {
        match self.spot_price_ticker(None).await? {
            OneOrMany::One(item) => Ok(vec![item]),
            OneOrMany::Many(items) => Ok(items),
        }
    }

    pub async fn spot_book_ticker(
        &self,
        symbol: Option<&str>,
    ) -> Result<OneOrMany<SpotBookTicker>> {
        let mut query = Vec::new();
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        self.get_spot_one_or_many("/api/v3/ticker/bookTicker", &query)
            .await
    }

    pub async fn spot_book_ticker_all(&self) -> Result<Vec<SpotBookTicker>> {
        match self.spot_book_ticker(None).await? {
            OneOrMany::One(item) => Ok(vec![item]),
            OneOrMany::Many(items) => Ok(items),
        }
    }

    pub async fn spot_history_download_entries(
        &self,
        file_path: &str,
    ) -> Result<Vec<SpotHistoricalDownloadNode>> {
        let url = format!("{}/file-svc/history/download", self.spot_history_base_url);
        for attempt in 1..=4u64 {
            let response = self
                .http
                .get(&url)
                .query(&[("filePath", file_path.to_string())])
                .send()
                .await
                .with_context(|| format!("request MEXC spot history path {file_path}"))?;
            let status = response.status();
            let body = response
                .text()
                .await
                .with_context(|| format!("read MEXC spot history response body for {file_path}"))?;

            if status.is_success() {
                let parsed: SpotHistoricalDownloadResponse = serde_json::from_str(&body)
                    .with_context(|| {
                        let snippet = body.chars().take(300).collect::<String>();
                        format!("decode MEXC spot history response body: {snippet}")
                    })?;
                return Ok(parsed.data);
            }

            let retriable =
                matches!(status.as_u16(), 403 | 408 | 425 | 429) || status.is_server_error();
            if retriable && attempt < 4 {
                tokio::time::sleep(Duration::from_millis(250 * attempt)).await;
                continue;
            }

            let snippet = body.chars().take(300).collect::<String>();
            return Err(anyhow!(
                "unexpected HTTP status {status} for MEXC spot history path {file_path}: {snippet}"
            ));
        }

        Err(anyhow!(
            "exhausted retries for MEXC spot history path {file_path}"
        ))
    }

    pub async fn spot_history_directories(&self, file_path: &str) -> Result<Vec<String>> {
        Ok(self
            .spot_history_download_entries(file_path)
            .await?
            .into_iter()
            .filter_map(SpotHistoricalDownloadNode::into_directory)
            .map(|value| value.trim_end_matches('/').to_string())
            .collect())
    }

    pub async fn spot_history_files_at_path(
        &self,
        file_path: &str,
    ) -> Result<Vec<SpotHistoricalDownloadFile>> {
        Ok(self
            .spot_history_download_entries(file_path)
            .await?
            .into_iter()
            .filter_map(SpotHistoricalDownloadNode::into_file)
            .collect())
    }

    pub async fn spot_history_symbol_ids(
        &self,
        kind: SpotHistoricalDataKind,
    ) -> Result<Vec<String>> {
        self.spot_history_directories(&format!("SPOT2/{}/", kind.as_str()))
            .await
    }

    pub async fn spot_history_symbol_periods(
        &self,
        kind: SpotHistoricalDataKind,
        symbol_id: &str,
    ) -> Result<Vec<String>> {
        self.spot_history_directories(&format!("SPOT2/{}/{symbol_id}/", kind.as_str()))
            .await
    }

    pub async fn spot_history_symbol_intervals(
        &self,
        kind: SpotHistoricalDataKind,
        symbol_id: &str,
        period: SpotHistoricalArchivePeriod,
    ) -> Result<Vec<String>> {
        self.spot_history_directories(&format!(
            "SPOT2/{}/{symbol_id}/{}/",
            kind.as_str(),
            period.as_str()
        ))
        .await
    }

    pub async fn spot_history_kline_files(
        &self,
        symbol_id: &str,
        period: SpotHistoricalArchivePeriod,
        interval: SpotHistoricalKlineInterval,
    ) -> Result<Vec<SpotHistoricalDownloadFile>> {
        self.spot_history_files_at_path(&format!(
            "SPOT2/{}/{symbol_id}/{}/{}/",
            SpotHistoricalDataKind::Kline.as_str(),
            period.as_str(),
            interval.as_str()
        ))
        .await
    }

    pub async fn spot_history_trade_files(
        &self,
        symbol_id: &str,
        period: SpotHistoricalArchivePeriod,
    ) -> Result<Vec<SpotHistoricalDownloadFile>> {
        self.spot_history_files_at_path(&format!(
            "SPOT2/{}/{symbol_id}/{}/",
            SpotHistoricalDataKind::Trades.as_str(),
            period.as_str()
        ))
        .await
    }

    pub async fn spot_history_symbol_directories(
        &self,
        concurrency: usize,
    ) -> Result<Vec<SpotHistoricalSymbolDirectory>> {
        let symbol_ids = self
            .spot_history_symbol_ids(SpotHistoricalDataKind::Kline)
            .await?;
        let concurrency = Self::validated_concurrency(concurrency);

        stream::iter(symbol_ids.into_iter().map(|symbol_id| {
            let client = self.clone();
            async move { client.spot_history_probe_symbol_directory(symbol_id).await }
        }))
        .buffer_unordered(concurrency)
        .try_collect()
        .await
    }

    pub async fn spot_history_symbols_for_symbol_id(
        &self,
        symbol_id: &str,
    ) -> Result<SpotHistoricalSymbolDirectory> {
        self.spot_history_probe_symbol_directory(symbol_id.to_string())
            .await
    }

    pub async fn spot_history_symbol_id_map(
        &self,
        concurrency: usize,
    ) -> Result<BTreeMap<String, String>> {
        let mut symbols_by_id = BTreeMap::new();
        for entry in self.spot_history_symbol_directories(concurrency).await? {
            for symbol in entry.symbols {
                symbols_by_id
                    .entry(symbol)
                    .or_insert_with(|| entry.symbol_id.clone());
            }
        }
        Ok(symbols_by_id)
    }

    pub async fn futures_contracts(
        &self,
        symbol: Option<&str>,
    ) -> Result<OneOrMany<FuturesContractInfo>> {
        let mut query = Vec::new();
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        let response: FuturesApiEnvelope<OneOrMany<FuturesContractInfo>> =
            match self.get_futures("/api/v1/contract/detail", &query).await {
                Ok(response) => response,
                Err(primary_error) => self
                    .get_futures("/api/v1/contract/detail/country", &query)
                    .await
                    .with_context(|| {
                        format!(
                            "fetch futures contracts from /api/v1/contract/detail after primary /detail error: {primary_error:#}"
                        )
                    })?,
            };
        Ok(response.data)
    }

    pub async fn futures_server_time(&self) -> Result<u64> {
        let response: FuturesApiEnvelope<u64> =
            self.get_futures("/api/v1/contract/ping", &[]).await?;
        Ok(response.data)
    }

    pub async fn futures_ping(&self) -> Result<u64> {
        self.futures_server_time().await
    }

    pub async fn futures_contracts_all(&self) -> Result<Vec<FuturesContractInfo>> {
        match self.futures_contracts(None).await? {
            OneOrMany::One(item) => Ok(vec![item]),
            OneOrMany::Many(items) => Ok(items),
        }
    }

    pub async fn futures_transferable_currencies(&self) -> Result<Vec<String>> {
        let response: FuturesApiEnvelope<Vec<String>> = self
            .get_futures("/api/v1/contract/support_currencies", &[])
            .await?;
        Ok(response.data)
    }

    pub async fn futures_support_currencies(&self) -> Result<Vec<String>> {
        self.futures_transferable_currencies().await
    }

    pub async fn futures_depth(&self, symbol: &str) -> Result<FuturesDepthSnapshot> {
        let response: FuturesApiEnvelope<FuturesDepthSnapshot> = self
            .get_futures(&format!("/api/v1/contract/depth/{symbol}"), &[])
            .await?;
        Ok(response.data)
    }

    pub async fn futures_depth_commits(
        &self,
        symbol: &str,
        limit: u16,
    ) -> Result<Vec<FuturesDepthSnapshot>> {
        let response: FuturesApiEnvelope<Vec<FuturesDepthSnapshot>> = self
            .get_futures(
                &format!("/api/v1/contract/depth_commits/{symbol}/{limit}"),
                &[],
            )
            .await?;
        Ok(response.data)
    }

    pub async fn futures_index_price(&self, symbol: &str) -> Result<FuturesIndexPrice> {
        let response: FuturesApiEnvelope<FuturesIndexPrice> = self
            .get_futures(&format!("/api/v1/contract/index_price/{symbol}"), &[])
            .await?;
        Ok(response.data)
    }

    pub async fn futures_index_price_for_symbols(
        &self,
        symbols: &[String],
        concurrency: usize,
    ) -> Result<Vec<FuturesIndexPrice>> {
        let concurrency = Self::validated_concurrency(concurrency);
        if concurrency == 1 {
            let mut values = Vec::with_capacity(symbols.len());
            for (index, symbol) in symbols.iter().enumerate() {
                if index > 0 {
                    tokio::time::sleep(Duration::from_millis(
                        FUTURES_REFERENCE_SEQUENTIAL_DELAY_MS,
                    ))
                    .await;
                }
                values.push(
                    self.retry_futures_reference_call(
                        symbol.clone(),
                        "futures index price",
                        |client, symbol| async move { client.futures_index_price(&symbol).await },
                    )
                    .await?,
                );
            }
            return Ok(values);
        }
        stream::iter(symbols.iter().cloned().map(|symbol| {
            let client = self.clone();
            async move {
                client
                    .retry_futures_reference_call(
                        symbol,
                        "futures index price",
                        |client, symbol| async move { client.futures_index_price(&symbol).await },
                    )
                    .await
            }
        }))
        .buffer_unordered(concurrency)
        .try_collect()
        .await
    }

    pub async fn futures_index_price_all(
        &self,
        concurrency: usize,
    ) -> Result<Vec<FuturesIndexPrice>> {
        let symbols = self.all_futures_symbols().await?;
        self.futures_index_price_for_symbols(&symbols, concurrency)
            .await
    }

    pub async fn futures_fair_price(&self, symbol: &str) -> Result<FuturesFairPrice> {
        let response: FuturesApiEnvelope<FuturesFairPrice> = self
            .get_futures(&format!("/api/v1/contract/fair_price/{symbol}"), &[])
            .await?;
        Ok(response.data)
    }

    pub async fn futures_fair_price_for_symbols(
        &self,
        symbols: &[String],
        concurrency: usize,
    ) -> Result<Vec<FuturesFairPrice>> {
        let concurrency = Self::validated_concurrency(concurrency);
        if concurrency == 1 {
            let mut values = Vec::with_capacity(symbols.len());
            for (index, symbol) in symbols.iter().enumerate() {
                if index > 0 {
                    tokio::time::sleep(Duration::from_millis(
                        FUTURES_REFERENCE_SEQUENTIAL_DELAY_MS,
                    ))
                    .await;
                }
                values.push(
                    self.retry_futures_reference_call(
                        symbol.clone(),
                        "futures fair price",
                        |client, symbol| async move { client.futures_fair_price(&symbol).await },
                    )
                    .await?,
                );
            }
            return Ok(values);
        }
        stream::iter(symbols.iter().cloned().map(|symbol| {
            let client = self.clone();
            async move {
                client
                    .retry_futures_reference_call(
                        symbol,
                        "futures fair price",
                        |client, symbol| async move { client.futures_fair_price(&symbol).await },
                    )
                    .await
            }
        }))
        .buffer_unordered(concurrency)
        .try_collect()
        .await
    }

    pub async fn futures_fair_price_all(
        &self,
        concurrency: usize,
    ) -> Result<Vec<FuturesFairPrice>> {
        let symbols = self.all_futures_symbols().await?;
        self.futures_fair_price_for_symbols(&symbols, concurrency)
            .await
    }

    pub async fn futures_funding_rate(&self, symbol: &str) -> Result<FuturesFundingRate> {
        let response: FuturesApiEnvelope<FuturesFundingRate> = self
            .get_futures(&format!("/api/v1/contract/funding_rate/{symbol}"), &[])
            .await?;
        Ok(response.data)
    }

    pub async fn futures_funding_rate_for_symbols(
        &self,
        symbols: &[String],
        concurrency: usize,
    ) -> Result<Vec<FuturesFundingRate>> {
        let concurrency = Self::validated_concurrency(concurrency);
        if concurrency == 1 {
            let mut values = Vec::with_capacity(symbols.len());
            for (index, symbol) in symbols.iter().enumerate() {
                if index > 0 {
                    tokio::time::sleep(Duration::from_millis(
                        FUTURES_REFERENCE_SEQUENTIAL_DELAY_MS,
                    ))
                    .await;
                }
                values.push(
                    self.retry_futures_reference_call(
                        symbol.clone(),
                        "futures funding rate",
                        |client, symbol| async move { client.futures_funding_rate(&symbol).await },
                    )
                    .await?,
                );
            }
            return Ok(values);
        }
        stream::iter(symbols.iter().cloned().map(|symbol| {
            let client = self.clone();
            async move {
                client
                    .retry_futures_reference_call(
                        symbol,
                        "futures funding rate",
                        |client, symbol| async move { client.futures_funding_rate(&symbol).await },
                    )
                    .await
            }
        }))
        .buffer_unordered(concurrency)
        .try_collect()
        .await
    }

    pub async fn futures_funding_rate_all(
        &self,
        concurrency: usize,
    ) -> Result<Vec<FuturesFundingRate>> {
        let symbols = self.all_futures_symbols().await?;
        self.futures_funding_rate_for_symbols(&symbols, concurrency)
            .await
    }

    pub async fn futures_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<FuturesKlineSeries> {
        let mut query = vec![("interval", interval.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(start_time) = start_time {
            query.push(("start", start_time.to_string()));
        }
        if let Some(end_time) = end_time {
            query.push(("end", end_time.to_string()));
        }
        let response: FuturesApiEnvelope<FuturesKlineSeries> = self
            .get_futures(&format!("/api/v1/contract/kline/{symbol}"), &query)
            .await?;
        Ok(response.data)
    }

    pub async fn futures_index_price_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<FuturesKlineSeries> {
        let mut query = vec![("interval", interval.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(start_time) = start_time {
            query.push(("start", start_time.to_string()));
        }
        if let Some(end_time) = end_time {
            query.push(("end", end_time.to_string()));
        }
        let response: FuturesApiEnvelope<FuturesKlineSeries> = self
            .get_futures(
                &format!("/api/v1/contract/kline/index_price/{symbol}"),
                &query,
            )
            .await?;
        Ok(response.data)
    }

    pub async fn futures_fair_price_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u16>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<FuturesKlineSeries> {
        let mut query = vec![("interval", interval.to_string())];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(start_time) = start_time {
            query.push(("start", start_time.to_string()));
        }
        if let Some(end_time) = end_time {
            query.push(("end", end_time.to_string()));
        }
        let response: FuturesApiEnvelope<FuturesKlineSeries> = self
            .get_futures(
                &format!("/api/v1/contract/kline/fair_price/{symbol}"),
                &query,
            )
            .await?;
        Ok(response.data)
    }

    pub async fn futures_recent_deals(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<Vec<FuturesDeal>> {
        let mut query = Vec::new();
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        let response: FuturesApiEnvelope<Vec<FuturesDeal>> = self
            .get_futures(&format!("/api/v1/contract/deals/{symbol}"), &query)
            .await?;
        Ok(response.data)
    }

    pub async fn futures_deals(
        &self,
        symbol: &str,
        limit: Option<u16>,
    ) -> Result<Vec<FuturesDeal>> {
        self.futures_recent_deals(symbol, limit).await
    }

    pub async fn futures_ticker(&self, symbol: Option<&str>) -> Result<OneOrMany<FuturesTicker>> {
        let mut query = Vec::new();
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol.to_string()));
        }
        let response: FuturesApiEnvelope<OneOrMany<FuturesTicker>> =
            self.get_futures("/api/v1/contract/ticker", &query).await?;
        Ok(response.data)
    }

    pub async fn futures_trend_data(
        &self,
        symbol: Option<&str>,
    ) -> Result<OneOrMany<FuturesTicker>> {
        self.futures_ticker(symbol).await
    }

    pub async fn futures_ticker_all(&self) -> Result<Vec<FuturesTicker>> {
        match self.futures_ticker(None).await? {
            OneOrMany::One(item) => Ok(vec![item]),
            OneOrMany::Many(items) => Ok(items),
        }
    }

    pub async fn futures_insurance_fund_balance(&self) -> Result<Vec<FuturesInsuranceBalance>> {
        let response: FuturesApiEnvelope<Vec<FuturesInsuranceBalance>> = self
            .get_futures("/api/v1/contract/risk_reverse", &[])
            .await?;
        Ok(response.data)
    }

    pub async fn futures_risk_reverse(&self) -> Result<Vec<FuturesInsuranceBalance>> {
        self.futures_insurance_fund_balance().await
    }

    pub async fn futures_insurance_fund_balance_history(
        &self,
        symbol: &str,
        page_num: u64,
        page_size: u64,
    ) -> Result<FuturesPagination<FuturesInsuranceBalanceHistoryItem>> {
        let response: FuturesApiEnvelope<FuturesPagination<FuturesInsuranceBalanceHistoryItem>> =
            self.get_futures(
                "/api/v1/contract/risk_reverse/history",
                &[
                    ("symbol", symbol.to_string()),
                    ("page_num", page_num.to_string()),
                    ("page_size", page_size.to_string()),
                ],
            )
            .await?;
        Ok(response.data)
    }

    pub async fn futures_risk_reverse_history(
        &self,
        symbol: &str,
        page_num: u64,
        page_size: u64,
    ) -> Result<FuturesPagination<FuturesInsuranceBalanceHistoryItem>> {
        self.futures_insurance_fund_balance_history(symbol, page_num, page_size)
            .await
    }

    pub async fn futures_funding_rate_history(
        &self,
        symbol: &str,
        page_num: u64,
        page_size: u64,
    ) -> Result<FuturesPagination<FuturesFundingRateHistoryItem>> {
        let response: FuturesApiEnvelope<FuturesPagination<FuturesFundingRateHistoryItem>> = self
            .get_futures(
                "/api/v1/contract/funding_rate/history",
                &[
                    ("symbol", symbol.to_string()),
                    ("page_num", page_num.to_string()),
                    ("page_size", page_size.to_string()),
                ],
            )
            .await?;
        Ok(response.data)
    }

    pub async fn all_spot_symbols(&self) -> Result<Vec<String>> {
        Ok(self
            .spot_exchange_info()
            .await?
            .symbols
            .into_iter()
            .map(|symbol| symbol.symbol)
            .collect())
    }

    pub async fn all_futures_symbols(&self) -> Result<Vec<String>> {
        let contracts = self.futures_contracts(None).await?;
        let list = match contracts {
            OneOrMany::One(contract) => vec![contract.symbol],
            OneOrMany::Many(contracts) => contracts
                .into_iter()
                .map(|contract| contract.symbol)
                .collect(),
        };
        Ok(list)
    }

    pub async fn all_public_symbols(&self) -> Result<(Vec<String>, Vec<String>)> {
        Ok((
            self.all_spot_symbols().await?,
            self.all_futures_symbols().await?,
        ))
    }

    pub async fn futures_index_price_point(&self, symbol: &str) -> Result<FuturesWsPricePoint> {
        let price = self.futures_index_price(symbol).await?;
        Ok(FuturesWsPricePoint {
            symbol: price.symbol,
            price: price.index_price,
        })
    }

    pub async fn futures_fair_price_point(&self, symbol: &str) -> Result<FuturesWsPricePoint> {
        let price = self.futures_fair_price(symbol).await?;
        Ok(FuturesWsPricePoint {
            symbol: price.symbol,
            price: price.fair_price,
        })
    }

    async fn spot_history_probe_symbol_directory(
        &self,
        symbol_id: String,
    ) -> Result<SpotHistoricalSymbolDirectory> {
        let candidate_paths = [
            format!(
                "SPOT2/{}/{symbol_id}/{}/{}/",
                SpotHistoricalDataKind::Kline.as_str(),
                SpotHistoricalArchivePeriod::Monthly.as_str(),
                SpotHistoricalKlineInterval::Day1.as_str()
            ),
            format!(
                "SPOT2/{}/{symbol_id}/{}/{}/",
                SpotHistoricalDataKind::Kline.as_str(),
                SpotHistoricalArchivePeriod::Daily.as_str(),
                SpotHistoricalKlineInterval::Day1.as_str()
            ),
        ];

        let mut symbols = BTreeSet::new();
        for path in candidate_paths {
            let files = match self.spot_history_files_at_path(&path).await {
                Ok(files) => files,
                Err(_) => continue,
            };
            for file in files {
                if let Some(symbol) = file.inferred_symbol() {
                    symbols.insert(symbol);
                }
            }
            if !symbols.is_empty() {
                break;
            }
        }

        Ok(SpotHistoricalSymbolDirectory {
            symbol_id,
            symbols: symbols.into_iter().collect(),
        })
    }
}
