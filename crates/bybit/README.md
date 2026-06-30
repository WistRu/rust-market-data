# bybit

Public market-data module for Bybit V5 inside `rust-market-data`.

This crate targets Bybit public no-key market data:

- REST: `https://api.bybit.com`
- spot WS: `wss://stream.bybit.com/v5/public/spot`
- linear WS: `wss://stream.bybit.com/v5/public/linear`

## What is included

- Public REST client for V5 market time, instruments, order book, tickers,
  recent trades, and klines.
- Public WS topic builder and combined reader for spot and linear streams.
- Coverage planner for ticker, public trade, order book, and kline topics.
- `MarketDataConnector` implementation for the shared `common` trait.
- Acceptance-report integration used by the workspace connector factory.

## Quick smoke

```bash
cargo run -p bybit --example bybit_public_smoke
```

Optional environment:

- `BYBIT_SPOT_SYMBOL=BTCUSDT`
- `BYBIT_LINEAR_SYMBOL=BTCUSDT`

## Full-universe coverage / acceptance report

```bash
cargo run -p bybit --example bybit_public_universe_coverage_report
```

The report uses live V5 `instruments-info` as the authoritative universe and
builds spot/linear public WS subscription plans from that universe.

## WS matrix

```bash
BYBIT_WS_MATRIX_SECONDS=20 cargo run -p bybit --example bybit_public_ws_matrix
```

The matrix subscribes to representative spot and linear public topics:

- `tickers.BTCUSDT`
- `publicTrade.BTCUSDT`
- `orderbook.50.BTCUSDT`

## Live proof

Validated from `/opt/rust-market-data` on 2026-06-30 UTC:

```text
cargo run -q -p bybit --example bybit_public_smoke
ok time 1782845007
ok spot_instruments 598
ok linear_instruments 500
ok spot_orderbook bids=50
ok linear_orderbook bids=50
ok spot_tickers
ok linear_tickers

cargo run -q -p acceptance -- report bybit --json
status=handoff-ready
spot_ws_plan 598/598 coverage_pct=100.00
linear_ws_plan 500/500 coverage_pct=100.00

BYBIT_WS_MATRIX_SECONDS=8 cargo run -q -p bybit --example bybit_public_ws_matrix
spot_seen: tickers.BTCUSDT, publicTrade.BTCUSDT, orderbook.50.BTCUSDT
linear_seen: tickers.BTCUSDT, publicTrade.BTCUSDT, orderbook.50.BTCUSDT
```

## Handoff notes

- Bybit V5 REST and WS surfaces are category-scoped. Spot and linear must be
  checked separately.
- Public market-data readiness intentionally excludes private trading, account,
  signed request, balance, and order endpoints.
- WS topics use Bybit V5 topic names such as `orderbook.50.BTCUSDT`.
