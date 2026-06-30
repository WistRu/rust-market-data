# Phase 2 OKX Handoff Completion Evidence

Date: 2026-06-30 UTC

This artifact records the Phase 2 connector-factory milestone for OKX public
market data in `rust-market-data`.

## Scope

The OKX handoff scope is public no-key SPOT and SWAP market data:

- REST instruments
- REST tickers
- REST trades
- REST order book
- public WebSocket `tickers`
- public WebSocket `trades`
- public WebSocket `books5`
- acceptance inventory/report/drift-audit integration
- downstream consumer smoke proof

Out of scope:

- private trading APIs
- account authentication
- signed requests
- balances
- orders
- positions
- execution
- a fully normalized cross-exchange instrument model

## Implementation

- Parent PRD: GitHub issue `#16`
- Child implementation issues: `#17` through `#21`
- OKX implementation commit: the commit containing this artifact and closing
  GitHub issues `#17` through `#21`
- Venue selected: OKX
- Fallback venue: Bitget remains a future/fallback venue, not part of this
  completed OKX scope.

## Handoff-Ready Connectors

The current handoff-ready connector set is:

- `mexc`
- `aster`
- `binance`
- `bybit`
- `okx`

Current scaffold-only connectors remain:

- `bitget`
- `gateio`
- `kucoin`
- `coinbase`
- `crypto_com`
- `deribit`
- `hyperliquid`
- `kraken`
- `bitunix`

## OKX Evidence Commands

Run these OKX-specific checks:

```bash
cargo run -q -p okx --example okx_public_rest_matrix
OKX_WS_MATRIX_SECONDS=20 cargo run -q -p okx --example okx_public_ws_matrix
cargo run -q -p okx --example okx_public_universe_coverage_report
```

Run these workspace handoff checks:

```bash
cargo fmt --all -- --check
cargo check --workspace --examples
cargo test --workspace
cargo run -q -p acceptance -- inventory --json
cargo run -q -p acceptance -- report okx --json
cargo run -q -p acceptance -- drift-audit --json
cargo run -q -p handoff-consumer -- --json
```

## Live Evidence

Live evidence captured on 2026-06-30 UTC during Phase 2 OKX hardening:

- OKX REST SPOT instruments loaded: 1278 ticker-visible instruments.
- OKX REST SWAP instruments loaded: 402 ticker-visible instruments.
- `BTC-USDT` is represented as `SPOT`.
- `BTC-USDT-SWAP` is represented as `SWAP`.
- OKX REST order book, ticker, and trades worked for both sample instruments.
- OKX public WS `books5` live sample worked for both sample instruments through
  the acceptance report: 29 SPOT events and 43 SWAP events in the latest
  drift-audit run.
- OKX WS matrix saw `tickers`, `trades`, and `books5` payloads for both
  `BTC-USDT` and `BTC-USDT-SWAP`.
- OKX coverage is scoped to the public ticker-visible SPOT and SWAP universe:
  1278/1278 SPOT and 402/402 SWAP.

## Downstream Consumer Proof

The downstream-style smoke command is:

```bash
cargo run -q -p handoff-consumer -- --json
```

It includes OKX through the public `MarketDataConnector` interface and builds
OKX subscriptions with `BTC-USDT` instrument identity.

## Handoff Notes

- OKX identity is `instType` plus `instId`; do not collapse `BTC-USDT` and
  `BTC-USDT-SWAP`.
- OKX WS subscription args are JSON objects, not Binance-style stream names.
- The next venue should continue through `docs/connector-factory.md` rather than
  treating an endpoint string as readiness proof.
