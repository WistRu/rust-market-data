# MEXC Completion Audit

Date: 2026-06-28

This file is a current-state audit for the `mexc` crate under the goal:

- full public market-data coverage
- all documented public REST endpoints
- all documented public websocket channels
- all instruments at once
- handoff-ready runtime for an external screener

It is intentionally strict. "Implemented" is not enough. Each line below is marked by current evidence quality.

## Current conclusion

Status: complete for the currently available public MEXC market-data surface.

The crate is already strong on:

- public REST coverage
- broad WS coverage
- all-symbol runtime bootstrap
- stateful runtime
- reconnect/reset visibility
- managed rebuild/self-heal orchestration
- local spot and futures order-book helpers

Remaining caveats are now mostly exchange-behavior caveats, not crate gaps:

- some documented spot channels are currently exchange-blocked on live subscribe
- rare change-only channels can stay silent in short windows even when the transport and parser are correct

## Strongly proved

### Public REST coverage

Evidence:

- `examples/mexc_public_rest_matrix.rs`
- `examples/public_futures_kline_window_smoke.rs`
- live run previously confirmed:
  - `spot_offline_symbols`
  - `futures_server_time`
  - spot/futures market endpoints across the documented matrix
  - docs-aligned `futures_contracts` path via `/api/v1/contract/detail`
  - futures kline window params via `start` / `end`

Observed live evidence on 2026-06-28:

- `mexc_public_rest_matrix`:
  - `ok futures_ping 1782647141343`
  - `ok futures_support_currencies 44`
  - `ok futures_deals 2`
  - `ok futures_trend_data BTC_USDT`
  - `ok futures_risk_reverse 929`
  - `ok futures_risk_reverse_history 2`
  - `ok futures_contracts BTC_USDT`
  - `ok futures_klines 2`
  - `ok futures_index_price_klines 2`
  - `ok futures_fair_price_klines 2`
- `public_futures_kline_window_smoke`:
  - `window start=1782641071 end=1782641371 contract_points=5 index_points=5 fair_points=5`

Current assessment:

- official public spot market REST coverage is proved
- official public futures market REST coverage is proved
- futures contract-universe fetch now prefers the docs-aligned `/api/v1/contract/detail` path
- futures kline REST family now exposes documented time-window parameters instead of limit-only wrappers
- official docs naming and client naming are now better aligned for the public futures REST surface:
  - `futures_ping`
  - `futures_support_currencies`
  - `futures_deals`
  - `futures_trend_data`
  - `futures_risk_reverse`
  - `futures_risk_reverse_history`

### Spot historical market-download service

Evidence:

- `examples/spot_historical_market_data_smoke.rs`
- official docs page:
  - `Download Historical Market Data`

Previously observed live evidence:

- `spot_history_kline_symbol_ids=9157`
- `spot_history_symbol_periods 00002342913a4d85ae340b52952c97d8 ["daily","monthly"]`
- `spot_history_symbol_intervals 00002342913a4d85ae340b52952c97d8 ["Day1","Hour4","Hour8","Min15","Min30","Min5","Min60","Month1","Week1"]`
- `spot_history_kline_files ... first=NIM1_USDT-Week1-2024-08-01.csv`
- `spot_history_trade_symbol_ids=0` on 2026-06-28 live root listing
- `spot_history_symbols_for_symbol_id ... ["NIM1_USDT","NIM_USDT"]`

Current assessment:

- official spot historical download tree is proved live
- per-symbol-id file listing and symbol inference are proved live
- current live historical root exposes `SPOT2/kline/` and returns an empty `SPOT2/trades/` listing
- full-universe symbol-id map build exists, but it is operationally a heavy crawl rather than a lightweight startup path

### Full-universe public runtime

Evidence:

- `examples/public_all_symbols_bootstrap.rs`
- `examples/public_startup_report.rs`
- `examples/public_runtime_watch.rs`

Previously observed live evidence:

- `spot_symbols=2226`
- `futures_symbols=913`
- balanced manifest:
  - `spot_connections=297`
  - `futures_connections=37`
  - `spot_subscriptions=8906`
  - `futures_subscriptions=7307`

Current assessment:

- all-symbol balanced runtime is proved live

### Deep bootstrap for futures reference data

Evidence:

- `examples/public_deep_snapshot.rs`
- `examples/public_deep_handoff.rs`
- `examples/public_startup_report.rs`

Previously observed live evidence:

- `reference_modes index=bulk_only fair=bulk_only funding=bulk_only`
- `reference_sources index_bulk=913 index_endpoint=0 fair_bulk=913 fair_endpoint=0 funding_bulk=913 funding_endpoint=0`

Current assessment:

- full-universe futures reference bootstrap is proved live
- current live MEXC can be satisfied from bulk ticker snapshot without per-symbol fallback in the observed runs

### Stateful runtime and reconnect-aware state semantics

Evidence:

- `src/public.rs`
- `src/public_state.rs`
- unit tests in `public::tests::*`
- unit tests in `public_state::tests::*`

Current assessment:

- stateful runtime exists and is tested
- reconnect produces explicit `SessionStart`
- resumed sessions clear reconnect-sensitive state
- spot transport now has proactive max-age rotation before the documented `24h` websocket lifetime limit
- default sharded spot transport now spreads planned rotation across a window to avoid reconnect herd
- pending reset, recovery helper, health helper, managed heal and cooldown paths are implemented and unit-tested

### Machine-readable state handoff artifact

Evidence:

- `examples/public_state_handoff_report.rs`

Observed live evidence on 2026-06-28 (`/tmp/mexc-state-handoff-report.json`, 8s run):

- `applied_events=2831`
- `live_applied_events=600`
- `spot.total_symbols=9911`
- `futures.total_symbols=929`
- `spot.with_agg_depth=165`
- `futures.with_contract=922`
- `futures.without_contract=7`
- `futures.without_contract_with_other_state=7`
- `futures.without_contract_symbols=["MXSOL_USDT","MX_USDT","STETH_USDT","TON_USDT","USD1_USDT","USDE_USDT","WBTC_USDT"]`
- `futures.without_contract_with_other_state_symbols=["MXSOL_USDT","MX_USDT","STETH_USDT","TON_USDT","USD1_USDT","USDE_USDT","WBTC_USDT"]`
- `futures.with_event_contract=0`
- `futures.with_event_contract_symbols=[]`
- `futures.with_ticker=929`
- `futures.with_index_price=929`
- `futures.with_fair_price=929`

Current assessment:

- there is now a machine-readable handoff artifact for the current `MexcPublicState`
- it exposes operational truth about current hydration coverage, not just raw symbol counts
- it now also makes explicit when futures symbols already carry other state but still lack a current `contract` snapshot entry
- it now exposes the exact symbol lists for those contract gaps, not only counters
- this is useful for external screeners that need to inspect runtime readiness before relying on specific state slices

### Machine-readable state change feed

Evidence:

- `examples/public_state_change_feed.rs`
- `MexcManagedStatefulPublicRuntime::state_handoff_report()`

Observed live evidence on 2026-06-28 (`/tmp/mexc-state-change-feed.ndjson`, 8s run):

- summary:
  - `emitted_snapshots=5`
  - `total_live_events=54038`
  - `total_rebuilds=0`
  - `total_spot_resets=0`
  - `total_futures_resets=0`
- final snapshot:
  - `spot.total_symbols=9911`
  - `futures.total_symbols=976`
  - `futures.with_contract=922`
  - `futures.without_contract=54`
  - `futures.without_contract_with_other_state=54`
  - `futures.with_depth=917`
  - `futures.with_latest_deals=557`
  - `futures.with_any_kline=558`

Current assessment:

- there is now a durable NDJSON-style change feed for the managed public state itself, not only for contract refresh deltas
- external consumers can follow state hydration over time instead of polling a point-in-time artifact
- the feed makes visible an important runtime truth: live futures channels can populate symbols beyond the current contract snapshot coverage
- this materially improves handoff-readiness for external screeners that need machine-readable readiness and gap telemetry

### Managed rebuild and managed self-heal

Evidence:

- `examples/public_managed_rebuild_smoke.rs`
- `examples/public_managed_self_heal_smoke.rs`
- `examples/public_forced_reconnect_report.rs`
- `examples/public_session_rotation_smoke.rs`
- `examples/public_runtime_watch.rs`

Previously observed live evidence:

- rebuild path:
  - `startup#1 ready=true`
  - `startup#2 ready=true`
  - `contract_gap_warmup_passes=1`
  - `contract_gap_initial=7`
  - `contract_gap_final=0`
  - `contract_gap_stop_reason=Converged`
  - `rebuilds=1`
- self-heal path:
  - `self_heal_triggered rebuilds=1 ...`
  - post-heal state still hydrated for `BTC_USDT`
- forced reconnect path:
  - `initial_startup_ready=true`
  - `spot_resets_after_cut=297`
  - `futures_resets_after_cut=37`
  - `recovered_after_cut=true`
  - `total_rebuilds=0`
- planned rotation path with short max-age on 2026-06-28:
  - `startup ready=true coverage=3/3`
  - repeated spot rotation resets observed under `MEXC_ROTATE_AFTER_SECONDS=5`
  - eventual joint reset:
    - `spot_resets=2`
    - `futures_resets=1`
  - `rotation_recovery ready=true coverage=3/3`
- post-fix deep handoff on 2026-06-28:
  - `manifest spot_connections=298 futures_connections=37`
  - `reference_modes index=bulk_only fair=bulk_only funding=bulk_only`
  - startup succeeded even though live all-symbol `ticker/bookTicker` contained at least one symbol with `null` bid/ask fields

Current assessment:

- managed rebuild is proved live
- managed self-heal trigger path is proved live
- real socket-break reconnect and recovery on the same runtime instance is proved live
- planned session rotation and recovery on the same runtime instance is proved live
- cooldown anti-flap behavior is unit-tested
- rebuild path now also re-runs futures contract gap warmup before handing control back to the consumer

### Consumer-ready one-call startup helper

Evidence:

- `MexcConnector::connect_managed_balanced_ready()`
- `MexcConnector::connect_managed_exhaustive_ready()`
- `MexcPublicRuntimeBuilder::connect_managed_stateful_ready_with_deep_snapshot()`
- `examples/public_managed_ready_smoke.rs`

Observed live evidence on 2026-06-28:

- `public_managed_ready_smoke`:
  - `startup_ready=true`
  - `coverage=3/3`
  - `observed_live_events=483`
  - `contract_gap_warmup_passes=1`
  - `contract_gap_initial=7`
  - `contract_gap_final=0`
  - `contract_gap_stop_reason=Converged`
  - `spot_connections=298`
  - `futures_connections=37`
  - `spot_subscriptions=8926`
  - `futures_subscriptions=7379`

Current assessment:

- there is now a reusable one-call embed entrypoint for external consumers
- external screeners no longer need to copy the common `connect_managed_stateful_with_deep_snapshot(...) + await_balanced_startup(...)` glue from examples
- the helper now also performs startup futures contract gap warmup until convergence/stall, instead of leaving known contract gaps to later cooldown-based housekeeping
- this materially improves handoff-readiness of the crate as a library, not just as a set of examples

### Managed futures contract refresh fallback

Evidence:

- `examples/public_contract_refresh_smoke.rs`
- `examples/public_contract_refresh_policy_smoke.rs`
- `examples/futures_contract_change_feed.rs`
- `MexcManagedStatefulPublicRuntime::refresh_futures_contract_snapshot()`
- `MexcManagedStatefulPublicRuntime::refresh_futures_contract_snapshot_with_report()`
- `MexcManagedRuntimePolicy.contract_refresh_interval`

Current assessment:

- managed runtime now has an explicit REST refresh path for futures contract snapshot state
- managed runtime now also has a policy-driven interval refresh path via `next_step`
- managed runtime now also has a symbol-targeted refresh path for concrete futures contracts
- refresh paths now expose machine-readable delta counts: `added / updated / removed / unchanged`
- refresh paths now also expose symbol-level delta arrays:
  - `added_symbols`
  - `updated_symbols`
  - `removed_symbols`
- refresh paths now also expose field-level change detail for updated contracts:
  - `changes[*].symbol`
  - `changes[*].kind`
  - `changes[*].changed_fields`
  - `changes[*].field_changes[*].field`
  - `changes[*].field_changes[*].before`
  - `changes[*].field_changes[*].after`
- refresh reports now also expose:
  - `cause` (`interval` / `targeted` / `gap_backfill`)
  - `requested_symbols`
  - `unresolved_requested_symbols`
  - `used_full_snapshot_fallback`
- there is now a dedicated NDJSON-style contract change feed example for external consumers
- operator watch and soak report now surface refresh telemetry as live text / durable JSON evidence
- operator watch and soak report now distinguish `interval` vs `targeted` vs `gap_backfill` refreshes in their telemetry
- this is a production fallback for the still-rare `push.contract` channel, even though it is now live-proved

Observed operator evidence on 2026-06-28:

- `public_runtime_watch` with `MEXC_CONTRACT_REFRESH_INTERVAL_SECONDS=3`:
  - `contract_refresh cause=Interval requested_symbols=[] refreshed_contracts=922 ...`
- short soak report `/tmp/mexc-soak-report.json` with the same interval:
  - `contract_refresh_count=2`
  - `contract_refresh_interval_count=2`
  - `contract_refresh_targeted_count=0`
  - `contract_refresh_requested_symbol_total=0`
- gap-backfill soak report on 2026-06-28 (`70s`, `checkpoint=30s`):
  - `contract_refresh_count=2`
  - both refreshes had `cause=GapBackfill`
  - first gap batch:
    - `added=12`
    - `used_full_snapshot_fallback=false`
    - `unresolved_requested_symbols=["AEON_USDT","ASTEROID1_USDT","ASTEROIDSOL_USDT","FAI_USDT"]`
  - second gap batch:
    - `added=9`
    - `used_full_snapshot_fallback=false`
    - `unresolved_requested_symbols=["AEON_USDT","ASTEROID1_USDT","ASTEROIDSOL_USDT","FAI_USDT","MAGAALIENS_USDT","NAT_USDT","POD_USDT"]`
  - final handoff:
    - `with_contract=943`
    - `without_contract=33`

### Targeted contract refresh path

Evidence:

- `examples/public_contract_targeted_refresh_smoke.rs`
- `MexcManagedStatefulPublicRuntime::refresh_futures_contract_symbols_with_report()`
- `MexcPublicRuntimeBuilder::refresh_futures_contract_symbols()`
- `MexcPublicState::hydrate_futures_contract_updates()`
- unit tests in `public::tests::*`
- unit tests in `public_state::tests::*`

Observed live evidence on 2026-06-28:

- targeted refresh smoke:
  - `startup_ready=true`
  - `coverage=3/3`
  - `symbol=BTC_USDT`
  - `cause=targeted`
  - `requested_symbols=["BTC_USDT"]`
  - `refreshed_contracts=1`
  - `added=1`
  - `updated=0`
  - `removed=0`
  - `unchanged=0`
  - `after_has_contract=true`

Current assessment:

- a deterministic symbol-targeted contract refresh path is now live-proved
- targeted refresh updates only the requested symbols and does not clear unrelated contract entries
- managed runtime can now queue contract-refresh symbols from typed and raw contract signals before resolving them through the targeted REST path
- managed runtime can now also run best-effort gap backfill for futures symbols that already have other live state but still lack `contract`
- for those backfills, unresolved symbols are now surfaced explicitly instead of being hidden behind a blind full-snapshot fallback
- this materially reduces operational dependence on natural `push.contract` payload delivery for keeping contract state correct

### Managed public metadata refresh path

Evidence:

- `examples/public_metadata_refresh_smoke.rs`
- `examples/public_runtime_watch.rs`
- `examples/public_soak_report.rs`
- `MexcManagedStatefulPublicRuntime::refresh_public_metadata_snapshot()`
- `MexcManagedStatefulPublicRuntime::refresh_public_metadata_snapshot_with_report()`
- `MexcManagedRuntimePolicy.public_metadata_refresh_interval`

Observed live evidence on 2026-06-28:

- `public_metadata_refresh_smoke`:
  - `spot_symbols_before=9911`
  - `spot_symbols_after=9911`
  - `futures_symbols_before=929`
  - `futures_symbols_after=929`
  - `spot_exchange_symbols_before=2231`
  - `spot_exchange_symbols_after=2231`
  - `spot_server_time=1782640986389=>1782640987676`
  - `spot_default_symbols=2111=>2111`
  - `spot_offline_symbols=7681=>7681`
  - `spot_exchange_symbols=2231=>2231`
  - `futures_server_time=1782640986749=>1782640987888`
  - `futures_transferable_currencies=44=>44`
  - `futures_insurance_balances=929=>929`
- short soak report `/tmp/mexc-public-metadata-soak.json`:
  - `public_metadata_refresh_count=3`
  - `public_metadata_default_symbols_changed_count=0`
  - `public_metadata_offline_symbols_changed_count=0`
  - `public_metadata_exchange_info_changed_count=1`
  - `public_metadata_transferable_currencies_changed_count=0`
  - `public_metadata_insurance_balances_changed_count=3`
  - three `public_metadata_refresh` timeline entries at roughly 3s cadence

Current assessment:

- managed runtime now keeps public global metadata inside `MexcPublicState`, not only in bootstrap snapshots
- managed runtime has both manual and interval-driven metadata refresh paths
- operator-style watch and durable soak report both expose live metadata refresh evidence

### Spot snapshot drift tolerance

Evidence:

- `examples/public_deep_handoff.rs`
- direct live inspection of `GET /api/v3/ticker/bookTicker`

Previously observed live evidence:

- all-symbol `ticker/bookTicker` array length `2231`
- `PARKUSDT` arrived with:
  - `bidPrice=null`
  - `bidQty=null`
  - `askPrice=null`
  - `askQty=null`
- post-fix `public_deep_handoff` succeeded live instead of failing during bootstrap decode

Current assessment:

- spot all-symbol snapshot bootstrap now tolerates observed nullability drift in live `ticker/bookTicker`

### Local order books

Evidence:

- `examples/spot_order_book_smoke.rs`
- `examples/futures_order_book_smoke.rs`
- order-book unit tests

Previously observed live evidence:

- spot local order book bootstrapped and advanced live
- futures local order book required recovery via `depth_commits` and handled it

Current assessment:

- spot local order book helper is proved live
- futures local order book helper is proved live

### Crate verification

Evidence:

- `cargo test -q -p mexc`

Observed local evidence on 2026-06-28:

- `72 passed; 0 failed`

Current assessment:

- the full crate test suite currently passes end-to-end

### Public WS channel coverage

Evidence:

- `examples/mexc_public_ws_matrix.rs`
- `examples/futures_funding_rate_smoke.rs`
- `examples/futures_contract_channels_smoke.rs`
- `examples/futures_deals_raw_smoke.rs`
- `examples/futures_depth_modes_smoke.rs`
- `examples/futures_contract_channel_report.rs`
- `examples/futures_contract_observer_report.rs`
- `examples/futures_contract_signal_feed.rs`
- parser tests in `futures_ws::tests::*`
- parser tests in `spot_ws::tests::*`

Observed live evidence on 2026-06-28:

- `mexc_public_ws_matrix` on `BTCUSDT / BTC_USDT`:
  - all major spot live channels were `acked + seen`
  - `spot@public.increase.depth.v3.api.pb@BTCUSDT` was `blocked`
  - `spot@public.bookTicker.v3.api.pb@BTCUSDT` was `blocked`
  - `spot@public.aggre.bookTicker.v3.api.pb@100ms@BTCUSDT` was `acked + seen`
  - all major futures live channels were `acked + seen`, including:
    - `futures.depthFull`
    - `futures.fundingRate`
    - natural `futures.contract`
- `mexc_public_ws_matrix` on `ETHUSDT / ETH_USDT`:
  - same spot pattern repeated:
    - `increase.depth` blocked
    - raw `bookTicker.v3` blocked
    - `increase.depth.batch`, `aggre.depth`, `aggre.bookTicker`, `bookTicker.batch`, `kline`, `miniTicker`, `miniTickers` all `acked + seen`
- `mexc_public_ws_matrix` on `SOLUSDT / SOL_USDT`:
  - same spot pattern repeated again:
    - `increase.depth` blocked
    - raw `bookTicker.v3` blocked
    - available spot channels still `acked + seen`
- `futures_contract_channel_report` (`35s`):
  - `acked_contract=true`
  - `acked_event_contract=true`
  - `contract_payloads=2`
  - `event_contract_payloads=0`
  - `first_contract_payload_after_ms=1562`
  - `last_contract_symbol="SILVER_USD1"`
- `futures_contract_observer_report` (`35s`):
  - `contract_payloads=1`
  - `event_contract_payloads=0`
  - `first_contract_payload_after_ms=20064`
  - REST refresh delta path remained healthy while live contract payloads also arrived
- earlier long-running signal/soak evidence already proved natural `futures.eventContract` delivery and state hydration

Current assessment:

- documented spot/futures websocket surfaces are implemented in the crate
- currently available spot live channels are proved across multiple major symbols
- two documented spot channels are currently exchange-blocked on live subscribe across `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`:
  - `spot@public.increase.depth.v3.api.pb`
  - `spot@public.bookTicker.v3.api.pb`
- the crate now surfaces those blocked acks explicitly instead of silently treating them as generic missing coverage
- balanced/exhaustive runtime defaults already avoid depending on those blocked spot channels
- documented raw futures deal mode (`sub.deal` with `compress=false`) is implemented and live-proved
- documented futures depth knobs are implemented and live-proved:
  - `sub.depth` with `compress=false`
  - `sub.depth.full` with `limit`
- natural live `push.contract` is now proved in a normal short observation window
- natural live `push.event.contract` is also proved from longer live evidence
- parser drift on `push.contract` / `push.event.contract` is non-fatal because unknown frames still fall back to `Raw`
- spot protobuf book-ticker decode now uses `wrapper.channel` to disambiguate real wire drift between raw vs aggre book-ticker payload tags

### Long-duration soak proof

Evidence currently available:

- short live watches succeeded
- event rates and health summaries look healthy over short windows
- machine-readable soak evidence tool now exists:
  - `examples/public_soak_report.rs`
- upgraded report path now includes:
  - `kind` / `complete` phase markers
  - `state_handoff` snapshot of the current `MexcPublicState`
  - manifest snapshot
  - deep bootstrap snapshot
  - periodic checkpoints
  - compact transition timeline
- checkpoint file is now written during the run itself when `MEXC_SOAK_REPORT_PATH` is set,
  not only at process exit
- short live proof on 2026-06-28 (`8s`) showed the new state coverage surface directly inside
  soak JSON:
  - `state_handoff.spot.total_symbols=9911`
  - `state_handoff.futures.total_symbols=976`
  - `state_handoff.futures.with_contract=922`
  - `state_handoff.futures.without_contract=54`
  - `state_handoff.futures.with_event_contract=0`
  - `state_handoff.futures.with_ticker=976`
  - `state_handoff.futures.with_depth=912`
- later long-running checkpoint on 2026-06-28 upgraded this further:
  - `state_handoff.futures.with_event_contract=1`
  - proving live `push.event.contract` hydration reaches `MexcPublicState`, not only raw transport counters
- post-fix medium-duration live soak on 2026-06-28:
  - `watch_seconds=180`
  - `total_live_events=1608716`
  - `total_pending_resets=0`
  - `heal_trigger_count=0`
  - `health.healthy=true`
  - all `30s/60s/90s/120s/150s/180s` checkpoints stayed `healthy=true`
  - `kind_counts` included `futures.eventContract=2`
- long-duration live soak on 2026-06-28:
  - `watch_seconds=900`
  - `total_live_events=7512451`
  - `total_pending_resets=0`
  - `heal_trigger_count=0`
  - `heal_suppressed_count=0`
  - `health.healthy=true`
  - all `60s`-spaced checkpoints stayed `healthy=true`
  - `timeline` stayed empty
  - `kind_counts` included `futures.eventContract=1`

Current assessment:

- passive soak health is strong enough for the stated embed/screener goal
- forced reconnect, planned rotation, managed rebuild, and managed self-heal were all separately live-proved earlier, so the absence of a naturally occurring passive incident during soak is a residual operational caveat rather than a blocker

## Residual caveats

1. Two documented spot channels are currently exchange-blocked on live subscribe across multiple major symbols, so they are exposed by the crate but not relied on by the default runtime plans.
2. Rare change-only channels can still be silent in short windows; the crate now distinguishes `blocked`, `acked-but-silent`, and `seen`, which is the practical behavior an external consumer needs.
