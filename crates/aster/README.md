# aster

Public market-data module for Aster DEX inside `rust-market-data`.

This crate targets Aster V3 public APIs:

- spot REST: `https://sapi.asterdex.com`
- futures REST: `https://fapi.asterdex.com`
- spot WS: `wss://sstream.asterdex.com`
- futures WS: `wss://fstream.asterdex.com`

## What is included

- Public REST client for spot and USDT futures market endpoints.
- Binance-style WS stream-name builder for spot and futures public market data.
- Combined-stream WS reader returning raw JSON payloads.
- Full-universe coverage planner from live `exchangeInfo` symbols.
- `MarketDataConnector` implementation for the shared `common` trait.
- Smoke examples that can be run without API keys.

## Main types

- `AsterConnector`
- `AsterPublicRestClient`
- `AsterWsClient`
- `AsterWsSubscription`
- `AsterMarket`
- `AsterSpotCoverageConfig`
- `AsterFuturesCoverageConfig`
- `AsterExchangeInfo`
- `AsterOrderBook`

## Quick smoke

```bash
cargo run -p aster --example public_smoke
```

## REST matrix

```bash
cargo run -p aster --example public_rest_matrix
```

Optional environment:

- `ASTER_SPOT_SYMBOL=BTCUSDT`
- `ASTER_FUTURES_SYMBOL=BTCUSDT`

## WS matrix

```bash
cargo run -p aster --example public_ws_matrix
```

Optional environment:

- `ASTER_SPOT_SYMBOL=BTCUSDT`
- `ASTER_FUTURES_SYMBOL=BTCUSDT`
- `ASTER_WS_MATRIX_SECONDS=20`

The matrix subscribes to a small representative set:

- spot `aggTrade`, `bookTicker`, `depth@100ms`
- futures `aggTrade`, `bookTicker`, `markPrice@1s`, `depth@100ms`

## Full-universe coverage

```bash
cargo run -p aster --example public_universe_coverage_report
```

This report uses live `exchangeInfo` as the authoritative universe, builds an
exhaustive WS subscription plan, and fails if any `exchangeInfo` symbol is not
represented in the plan. It also checks live all-symbol REST visibility for
`TRADING` symbols.

## Live proof

Validated from `/opt/rust-market-data` on 2026-06-30 UTC:

```text
cargo check -p aster --examples
ok

cargo run -q -p aster --example public_rest_matrix
ok spot_depth bids=5
ok spot_trades 2
ok spot_klines 2
ok futures_depth bids=5
ok futures_trades 2
ok futures_klines 2
ok futures_funding_rate 2
ok futures_book_ticker 1

cargo run -q -p aster --example public_universe_coverage_report
spot_exhaustive_ws_plan subscriptions=1627 symbols_covered=58/58 coverage_pct=100.00 missing=[]
futures_exhaustive_ws_plan subscriptions=17380 symbols_covered=511/511 coverage_pct=100.00 missing=[]
spot_price_ticker trading_symbols_seen=58/58 coverage_pct=100.00 missing=[]
spot_book_ticker trading_symbols_seen=58/58 coverage_pct=100.00 missing=[]
futures_price_ticker trading_symbols_seen=498/498 coverage_pct=100.00 missing=[]
futures_book_ticker_effective trading_symbols_seen=498/498 coverage_pct=100.00 missing=[]
futures_24hr_ticker trading_symbols_seen=498/498 coverage_pct=100.00 missing=[]
futures_premium_index trading_symbols_seen=498/498 coverage_pct=100.00 missing=[]

ASTER_WS_MATRIX_SECONDS=12 cargo run -q -p aster --example public_ws_matrix
spot_seen:
  btcusdt@aggTrade 1
  btcusdt@bookTicker 27
  btcusdt@depth@100ms 77
futures_seen:
  btcusdt@aggTrade 15
  btcusdt@bookTicker 123
  btcusdt@depth@100ms 92
  btcusdt@markPrice@1s 12

cargo run -q -p aster --example public_smoke
ok rest_ping spot+futures
ok spot_depth bids=5
ok futures_depth bids=5
ok futures_ws_sample "btcusdt@bookTicker"
```

## Embed shape

```rust
use aster::{AsterPublicRestClient, AsterWsClient, AsterWsSubscription};

let rest = AsterPublicRestClient::default();
let depth = rest.futures_order_book("BTCUSDT", Some(20)).await?;

let mut stream = AsterWsClient::futures()
    .connect_streams(vec![
        AsterWsSubscription::BookTicker {
            symbol: "BTCUSDT".to_string(),
        },
        AsterWsSubscription::DiffDepth {
            symbol: "BTCUSDT".to_string(),
            speed_ms: Some(100),
        },
    ])
    .await?;
```

## Handoff notes

- Aster futures symbols use `BTCUSDT` form, not MEXC-style `BTC_USDT`.
- Futures `exchangeInfo` includes non-live-market statuses such as
  `PENDING_TRADING` and `SETTLING`; REST market-data calls for those symbols can
  return `-4108`, so live-data checks should be scoped to `status=TRADING`.
- Some all-symbol ticker endpoints are not a complete proof source by
  themselves: `spot 24hr` can omit quiet/test symbols, and futures all-symbol
  `bookTicker` can omit a symbol that still works through the per-symbol
  endpoint. The coverage report treats those as exchange endpoint behavior and
  verifies effective coverage with stricter sources/fallbacks.
- REST response structs intentionally keep `extra` or raw `serde_json::Value`
  fields where Aster returns Binance-compatible but fast-moving payload shapes.
- WS stream names are lower-case symbol names, for example `btcusdt@bookTicker`.
- For order-book consumers, use REST `depth` as the snapshot source and then
  apply WS `depth` events according to Aster's documented snapshot/update flow.
