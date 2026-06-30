# Connector Factory

The workspace treats an exchange crate as handoff-ready only after public
market-data behavior is proven through the acceptance gate. A crate with a
WebSocket endpoint string is not ready by itself.

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

## Current Factory Example

Bybit is the first factory-built connector. Use its public REST client, WS topic
builder, coverage example, and README as the shape for the next venue, but do
not blindly copy Bybit-specific category behavior into another exchange.

## Out Of Scope

Private trading, account auth, signed requests, balances, and order execution
are outside this connector factory path. Keep public market-data readiness
finished before widening the product surface.
