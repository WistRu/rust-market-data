# ADR 0001: Separate Tradable Universe From Metadata-Only State

- Status: Accepted
- Date: 2026-06-28

## Context

`MexcPublicState` had been using one symbol map per market to hold both:

- the tradable universe from authoritative manifests
- extra symbols observed in metadata and live feeds

For spot, the authoritative tradable universe comes from `exchangeInfo.symbols`. For futures, it comes from `contract/detail`.

At runtime on 2026-06-28, the mixed model produced large false gaps:

- spot: `exchangeInfo=2229`, `defaultSymbols=2110`, `symbol/offline=7683`, union `=9911`
- spot: `offline_minus_exchange=7682`
- futures: `contract/detail=922`, `ticker=929`, `insurance=929`
- futures extras outside `contract/detail`: `MXSOL_USDT`, `MX_USDT`, `STETH_USDT`, `TON_USDT`, `USD1_USDT`, `USDE_USDT`, `WBTC_USDT`

This meant coverage reports and health checks were penalizing the system for symbols that were never part of the tradable manifest. It also made the real problem harder to isolate: MEXC publishes metadata and live state for symbols that are not present in the current tradable contract manifest.

## Decision

We separate the state into two layers:

- `spot_symbols` and `futures_symbols` contain only the tradable universe.
- `spot_metadata_only_symbols` stores spot symbols learned from `defaultSymbols`, `symbol/offline`, or live messages before they are confirmed by `exchangeInfo`.
- `futures_orphan_symbols` stores futures symbols learned from `ticker`, `insurance_balances`, or live messages before they are confirmed by `contract/detail` or contract events.

Promotion rules:

- Spot symbols are promoted into `spot_symbols` only when they appear in `exchangeInfo.symbols`.
- Futures symbols are promoted into `futures_symbols` only when they appear in `contract/detail` or a contract payload that is treated as authoritative enough to establish the contract.
- Existing orphan or metadata-only state is merged into the promoted tradable symbol so that live observations are preserved.

Reporting rules:

- `handoff_report()` reports tradable coverage from `spot_symbols` and `futures_symbols`.
- metadata-only spot symbols and orphan futures symbols are reported separately.
- contract gap refresh logic targets only futures orphan symbols that already carry other live state, because those are the symbols that indicate manifest drift rather than idle metadata noise.

## Consequences

Positive:

- Coverage metrics now reflect the real tradable universe.
- Grafana alerts stop treating metadata-only symbols as broken trading coverage.
- Orphan state remains visible for diagnosis and targeted healing.

Trade-offs:

- Consumers of `handoff_report()` must interpret metadata-only and orphan counts explicitly instead of assuming a single flat symbol universe.
- MEXC manifest drift is now modeled as a first-class signal rather than being hidden inside generic coverage counts.

## Rejected Alternative

Keep one flat symbol map and filter at alert time.

This was rejected because the mixed state was already leaking into health reports, gap refresh decisions, and operator reasoning. The boundary belongs in the state model, not only in dashboard code.
