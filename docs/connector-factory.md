# Connector Factory

The workspace treats an exchange crate as handoff-ready only after public
market-data behavior is proven through the acceptance gate. A crate with a
WebSocket endpoint string is not ready by itself.

Use the project glossary in `CONTEXT.md` for readiness language. The production
readiness model is summarized in `docs/readiness-state-model.md`: promotion
state is separate from drift overlay, and `drift-warning` is display readiness
over a handoff-ready connector.

## Readiness Status

- `scaffold-only`: the crate exists but has no repeatable public REST/WS proof.
- `partial`: some public market-data behavior is implemented, but the connector
  does not yet pass the full acceptance gate.
- `handoff-ready`: public REST, public WS, coverage planning, runnable examples,
  known quirks, and README proof are all present.

## Acceptance Path

1. Add the exchange to the status inventory.
2. Implement public no-key REST smoke behavior.
3. Add a REST matrix or acceptance report row.
4. Implement public WS topic/subscription planning.
5. Add universe coverage or document the narrower coverage scope.
6. Preserve exchange-specific quirks in the report.
7. Add README live-proof commands.
8. Run `cargo run -p acceptance -- report <exchange>`.
9. Use `cargo run -p acceptance -- drift-audit` for the current ready set.
10. Prove downstream-style consumption with
    `cargo run -p handoff-consumer`.

## Release Gates

Deterministic CI should run formatting, workspace example compilation,
workspace tests, and `acceptance inventory`. Live exchange checks belong in the
separate live acceptance gate because public APIs can drift, rate-limit, or fail
independently of this repository.

The deterministic gate is `.github/workflows/ci.yml`. The live gate is
`.github/workflows/live-acceptance.yml` and is intentionally manual/scheduled,
not a required pull-request check.

Use these commands before tagging a handoff release:

```bash
cargo fmt --all -- --check
cargo check --workspace --examples
cargo test --workspace
cargo run -p acceptance -- inventory --json
cargo run -p handoff-consumer
```

Use this live gate when exchange-side evidence is needed:

```bash
cargo run -p acceptance -- drift-audit --json
```

## Current Factory Examples

Bybit is the first factory-built connector. OKX is the Phase 2 connector that
proves the same path on an exchange with explicit `instType` plus `instId`
instrument identity. Bitget is the Phase 3 connector that proves the same path
on a venue where `instType + symbol` distinguishes SPOT from USDT-FUTURES even
when both surfaces use `BTCUSDT`.

Use their public REST clients, WS topic builders, coverage examples, and READMEs
as shapes for the next venue, but do not blindly copy venue-specific category,
product type, or instrument behavior into another exchange.

## External Compatibility Contract

The public handoff surface is intentionally small:

- A ready connector implements `MarketDataConnector` for exchange name, public
  WS endpoint, and subscription-label construction.
- Ready examples and acceptance reports run without API keys.
- Readiness is machine-readable through `acceptance inventory`, `acceptance
  report <exchange>`, `acceptance drift-audit`, and `handoff-consumer`.
- Exchange-specific identity is preserved when a symbol alone is ambiguous.
- Private authentication, account state, signed requests, balances, positions,
  orders, and execution are outside the contract.

For a clean downstream smoke, use:

```bash
cargo run -q -p handoff-consumer -- --json
```

For a live ready-set audit, use:

```bash
cargo run -q -p acceptance -- drift-audit --json
```

GitHub Actions billing lock is an external account state. Until it is fixed,
local gates are the authoritative evidence and remote CI should not be described
as green.

## Out Of Scope

Private trading, account auth, signed requests, balances, and order execution
are outside this connector factory path. Keep public market-data readiness
finished before widening the product surface.
