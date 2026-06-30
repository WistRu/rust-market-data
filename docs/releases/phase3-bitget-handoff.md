# Phase 3 Handoff Evidence: External Package Path + Bitget

Date: 2026-06-30

Parent PRD: #22

## Ready Set

The handoff-ready connector set after Phase 3 is:

- MEXC
- Aster
- Binance
- Bybit
- OKX
- Bitget

Scaffold-only connectors remain Gate.io, KuCoin, Coinbase, Crypto.com,
Deribit, Hyperliquid, Kraken, and Bitunix.

## External Package Handoff

Clean downstream proof starts from the public connector surface:

```bash
cargo run -q -p handoff-consumer -- --json
```

Verified result on 2026-06-30: the command reported six ready connectors and
subscription labels for MEXC, Aster, Binance, Bybit, OKX, and Bitget.

The top-level README documents this path and the public compatibility contract:
no-key market data, `MarketDataConnector` subscription-building surface,
machine-readable acceptance status, and no private/auth/trading scope.

## Bitget Scope

Bitget is handoff-ready for public no-key market data only:

- SPOT symbols, tickers, recent trades, order book, and public WS channels.
- USDT-FUTURES contracts, tickers, recent trades, order book, and public WS
  channels.
- Instrument identity is `instType + symbol`; `BTCUSDT` SPOT and `BTCUSDT`
  USDT-FUTURES are distinct instruments.

Out of scope: auth, signed requests, balances, orders, positions, account data,
copy trading, margin trading, deposits, withdrawals, and execution.

Known quirks:

- Bitget V2 public WS args use `instType`, `channel`, and `instId`.
- Spot order book REST requires `type=step0`.
- WS book proof uses `books5`.
- Futures scope is USDT-FUTURES only.

Official Bitget planning references:

- https://www.bitget.com/api-doc/common/intro
- https://www.bitget.com/api-doc/spot/market/Get-Symbols
- https://www.bitget.com/api-doc/contract/market/Get-All-Symbols-Contracts

## Evidence Commands

Deterministic gates:

```bash
cargo fmt --all -- --check
cargo check --workspace --examples
cargo test --workspace
cargo run -q -p acceptance -- inventory --json
cargo run -q -p handoff-consumer -- --json
```

Verified result on 2026-06-30:

- formatting check passed
- workspace example compilation passed
- workspace tests passed
- inventory reported Bitget as `handoff-ready`
- handoff consumer reported six ready connectors including Bitget

Bitget live proof:

```bash
cargo run -q -p bitget --example bitget_public_rest_matrix
BITGET_WS_MATRIX_SECONDS=8 cargo run -q -p bitget --example bitget_public_ws_matrix
cargo run -q -p acceptance -- report bitget --json
```

Verified result on 2026-06-30:

- REST matrix loaded 1174 online spot symbols and 675 normal USDT-FUTURES
  contracts
- REST order book sample returned 5 spot bids and 5 futures bids
- REST trades sample returned 5 spot trades and 5 futures trades
- WS sample received live `ticker`, `trade`, and `books5` events for both SPOT
  and USDT-FUTURES `BTCUSDT`
- acceptance report status was `handoff-ready`
- coverage rows were 100 percent with zero missing symbols for:
  `spot_online_ticker_coverage`, `usdt_futures_normal_ticker_coverage`,
  `spot_ticker_visible_ws_plan`, and
  `usdt_futures_ticker_visible_ws_plan`

Ready-set live drift audit:

```bash
cargo run -q -p acceptance -- drift-audit --json
```

Verified result on 2026-06-30: live drift audit completed for MEXC, Aster,
Binance, Bybit, OKX, and Bitget. All six reports returned `handoff-ready`.

## CI Status

GitHub Actions is still subject to the repository/account billing lock. Treat
that as an external account state, not a code failure and not a green remote CI
signal. Local gates above are the authoritative Phase 3 evidence until billing
is resolved.

## Next Venue Standard

The next scaffold-only venue should follow the same package-handoff standard:

1. Keep it `scaffold-only` until public proof exists.
2. Add no-key REST proof for metadata, ticker, trades, and order book.
3. Preserve exchange-native product/instrument identity.
4. Add public WS planning and live sample proof.
5. Add coverage/scope rows with explicit missing-symbol evidence.
6. Promote to `handoff-ready` only after acceptance report, drift-audit
   eligibility, README commands, and handoff-consumer proof are all present.
