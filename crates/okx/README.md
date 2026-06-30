# okx

Public market-data module for OKX V5 inside `rust-market-data`.

This crate targets official OKX public no-key market-data APIs:

- REST: `https://www.okx.com`
- public WS: `wss://ws.okx.com:8443/ws/v5/public`
- docs: `https://www.okx.com/docs-v5/en/`

## What is included

- Public REST client for instruments, tickers, order book, and trades.
- Explicit `instType` + `instId` instrument identity. `BTC-USDT` and
  `BTC-USDT-SWAP` are treated as different instruments.
- Public WS subscription builder for `tickers`, `trades`, and `books5`.
- Coverage planner for the public ticker-visible SPOT and SWAP universe.
- `MarketDataConnector` implementation for the shared `common` trait.
- Acceptance-report integration used by the workspace connector factory.

## Scope

The handoff-ready scope is public no-key SPOT and SWAP market data. Coverage is
scoped to instruments visible from OKX public all-tickers endpoints for each
instrument type, while REST instrument counts are recorded as live context.

Private trading, account auth, signed requests, balances, orders, positions,
and execution are out of scope.

## REST matrix

```bash
cargo run -p okx --example okx_public_rest_matrix
```

Optional environment:

- `OKX_SPOT_INST_ID=BTC-USDT`
- `OKX_SWAP_INST_ID=BTC-USDT-SWAP`

## WS matrix

```bash
OKX_WS_MATRIX_SECONDS=20 cargo run -p okx --example okx_public_ws_matrix
```

The matrix subscribes to representative SPOT and SWAP public channels:

- `tickers`
- `trades`
- `books5`

## Full-universe coverage / acceptance report

```bash
cargo run -p okx --example okx_public_universe_coverage_report
```

This report builds subscription plans for the public ticker-visible SPOT and
SWAP universe and includes a short live `books5` WebSocket sample.

## Live proof

Validated from `/opt/rust-market-data` on 2026-06-30 UTC:

```text
cargo run -q -p okx --example okx_public_rest_matrix
ok spot_instruments 1278
ok swap_instruments 403
ok identity BTC-USDT SPOT
ok identity BTC-USDT-SWAP SWAP
ok spot_orderbook bids=5
ok swap_orderbook bids=5
ok spot_ticker last=58580.5
ok swap_ticker last=58555.1
ok spot_trades 5
ok swap_trades 5

OKX_WS_MATRIX_SECONDS=12 cargo run -q -p okx --example okx_public_ws_matrix
seen:
  books5:BTC-USDT 59
  books5:BTC-USDT-SWAP 83
  tickers:BTC-USDT 79
  tickers:BTC-USDT-SWAP 78
  trades:BTC-USDT 10
  trades:BTC-USDT-SWAP 30

cargo run -q -p acceptance -- report okx --json
status=handoff-ready
spot_ticker_visible_ws_plan 1278/1278 coverage_pct=100.00
swap_ticker_visible_ws_plan 402/402 coverage_pct=100.00
ws_live_books5_sample pass
```

The exact counts and prices are exchange-side live data and will move.

## Handoff notes

- OKX REST and WS use `instId` strings such as `BTC-USDT` and
  `BTC-USDT-SWAP`; do not strip suffixes into a single normalized symbol.
- OKX public WS subscribe frames use object args such as
  `{"channel":"books5","instId":"BTC-USDT"}` rather than Binance-style stream
  names.
- All readiness gates remain public market-data only.
