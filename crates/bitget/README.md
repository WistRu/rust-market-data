# Bitget Public Market Data

This crate is a handoff-ready public no-key Bitget connector for the workspace
connector factory. It covers public market-data only:

- SPOT symbols, tickers, recent trades, order book, and public WS channels.
- USDT-FUTURES contracts, tickers, recent trades, order book, and public WS
  channels.
- Instrument identity as `instType + symbol`, so `BTCUSDT` on SPOT and
  `BTCUSDT` on USDT-FUTURES are distinct instruments.

Out of scope: auth, signed requests, balances, orders, positions, account data,
copy trading, margin trading, deposits, withdrawals, and execution.

## Live Commands

```bash
cargo run -q -p bitget --example bitget_public_rest_matrix
BITGET_WS_MATRIX_SECONDS=20 cargo run -q -p bitget --example bitget_public_ws_matrix
cargo run -q -p bitget --example bitget_public_universe_coverage_report
cargo run -q -p acceptance -- report bitget --json
```

The default sample symbol is `BTCUSDT`. Override it with `BITGET_SYMBOL`.

## Scope

The acceptance scope is public ticker-visible SPOT plus USDT-FUTURES market
data. Coverage rows compare the online/normal REST universe to live ticker
visibility and then prove that the WS subscription plan covers that public
ticker-visible scope.

## Known Quirks

- Bitget V2 public WS subscriptions use JSON args shaped like
  `{"instType":"SPOT","channel":"books5","instId":"BTCUSDT"}`.
- Spot and USDT-FUTURES both use symbols such as `BTCUSDT`; consumers must keep
  `instType` with the symbol when product identity matters.
- Spot order book REST requires `type=step0`; the WS book proof uses `books5`.
- Futures REST scope in this crate is USDT-FUTURES only.
