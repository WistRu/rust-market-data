# binance

Public market-data module for Binance Spot and USD-M Futures inside
`rust-market-data`.

This crate targets official Binance public APIs:

- spot REST: `https://api.binance.com`
- USD-M futures REST: `https://fapi.binance.com`
- spot WS: `wss://stream.binance.com:9443`
- USD-M futures WS: `wss://fstream.binance.com`

## What is included

- Public REST client for spot and USD-M futures market-data endpoints.
- Binance-style WS stream-name builder for spot and futures public market data.
- Combined-stream WS reader returning raw JSON payloads.
- Full-universe coverage planner from live `exchangeInfo` symbols.
- `MarketDataConnector` implementation for the shared `common` trait.
- Smoke examples that can be run without API keys.

## Quick smoke

```bash
cargo run -p binance --example binance_public_smoke
```

## REST matrix

```bash
cargo run -p binance --example binance_public_rest_matrix
```

Optional environment:

- `BINANCE_SPOT_SYMBOL=BTCUSDT`
- `BINANCE_FUTURES_SYMBOL=BTCUSDT`

## WS matrix

```bash
cargo run -p binance --example binance_public_ws_matrix
```

Optional environment:

- `BINANCE_SPOT_SYMBOL=BTCUSDT`
- `BINANCE_FUTURES_SYMBOL=BTCUSDT`
- `BINANCE_WS_MATRIX_SECONDS=20`

## Full-universe coverage

```bash
cargo run -p binance --example binance_public_universe_coverage_report
```

This report uses live `exchangeInfo` as the authoritative universe, builds an
exhaustive WS subscription plan, and fails if any `exchangeInfo` symbol is not
represented in the plan. It also checks live all-symbol REST visibility for
`TRADING` symbols.

## Live proof

Validated from `/opt/rust-market-data` on 2026-06-30 UTC:

```text
cargo check -p binance --examples
ok

cargo run -q -p binance --example binance_public_universe_coverage_report
spot_exhaustive_ws_plan subscriptions=126636 symbols_covered=3618/3618 coverage_pct=100.00 missing_count=0
futures_exhaustive_ws_plan subscriptions=26669 symbols_covered=808/808 coverage_pct=100.00 missing_count=0
spot_price_ticker trading_symbols_seen=1361/1361 coverage_pct=100.00 missing_count=0
spot_book_ticker trading_symbols_seen=1361/1361 coverage_pct=100.00 missing_count=0
spot_24hr_ticker trading_symbols_seen=1361/1361 coverage_pct=100.00 missing_count=0
futures_price_ticker trading_symbols_seen=682/682 coverage_pct=100.00 missing_count=0
futures_book_ticker trading_symbols_seen=682/682 coverage_pct=100.00 missing_count=0
futures_24hr_ticker trading_symbols_seen=682/682 coverage_pct=100.00 missing_count=0
futures_premium_index trading_symbols_seen=682/682 coverage_pct=100.00 missing_count=0

cargo run -q -p binance --example binance_public_rest_matrix
ok spot_historical_trades 2
ok futures_premium_index_klines 2
ok futures_price_ticker_v2 1
ok futures_index_info 3
ok futures_asset_index 1
ok futures_constituents {...}
ok futures_symbol_adl_risk 1
ok futures_basis 2
skip futures_historical_trades unexpected HTTP status
```

## Handoff notes

- USD-M futures symbols use `BTCUSDT` form.
- Spot and futures share many stream names, but the endpoint hosts differ.
- Spot `historicalTrades` and `historicalBlockTrades` are documented as
  `MARKET_DATA`; they worked without a key during the latest live check for
  `historicalTrades`, but consumers should treat that as exchange behavior, not
  a private-key guarantee.
- USD-M futures `historicalTrades` is implemented but returned `401
  Unauthorized` in the no-key live matrix; keep it outside no-key readiness
  gates.
- Some high-weight all-symbol REST calls should not be used in tight loops; the
  WS streams are the intended live feed.
