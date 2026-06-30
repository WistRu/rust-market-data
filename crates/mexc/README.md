# mexc

Публичный market data модуль для MEXC внутри `rust-market-data`.

Текущее строгое состояние prove/unproved для goal смотри в:

`COMPLETION_AUDIT.md`

## Что уже есть

- Public REST client с полным покрытием официальных spot/futures market endpoints.
- Official spot historical market-download client поверх `file-svc/history/download`.
- Spot websocket client c protobuf-декодированием.
- Futures websocket client c JSON/gzip-декодированием.
- Coverage planner для генерации подписок по всем spot/futures инструментам.
- Unified public runtime с fan-in поверх нескольких spot/futures соединений.
- Локальный spot order book helper: snapshot + buffered aggDepth catch-up + strict live continuity checks.
- Live smoke example для быстрой проверки REST + WS.

## Основные типы

- `MexcPublicRestClient`
- `MexcSpotWsClient`
- `MexcFuturesWsClient`
- `MexcSpotCoverageConfig`
- `MexcFuturesCoverageConfig`
- `SpotHistoricalDataKind`
- `SpotHistoricalArchivePeriod`
- `SpotHistoricalKlineInterval`
- `MexcPublicRuntimeBuilder`
- `MexcPublicRuntimeConfig`
- `MexcSpotOrderBook`
- `MexcSpotOrderBookBootstrap`
- `MexcFuturesOrderBook`
- `MexcFuturesOrderBookBootstrap`

## Быстрый smoke

```bash
cargo run -p mexc --example mexc_public_smoke
```

## Матрицы проверки

```bash
cargo run -p mexc --example mexc_public_rest_matrix
cargo run -p mexc --example public_futures_kline_window_smoke
cargo run -p mexc --example mexc_public_ws_matrix
cargo run -p mexc --example public_snapshot
cargo run -p mexc --example public_deep_snapshot
cargo run -p mexc --example public_deep_handoff
cargo run -p mexc --example public_runtime_watch
cargo run -p mexc --example public_managed_rebuild_smoke
cargo run -p mexc --example public_managed_self_heal_smoke
cargo run -p mexc --example public_contract_refresh_smoke
cargo run -p mexc --example public_contract_refresh_policy_smoke
cargo run -p mexc --example public_contract_targeted_refresh_smoke
cargo run -p mexc --example public_managed_ready_smoke
cargo run -p mexc --example public_metadata_refresh_smoke
cargo run -p mexc --example public_soak_report
cargo run -p mexc --example public_forced_reconnect_report
cargo run -p mexc --example public_session_rotation_smoke
cargo run -p mexc --example futures_contract_channel_report
cargo run -p mexc --example futures_contract_observer_report
cargo run -p mexc --example futures_contract_signal_feed
cargo run -p mexc --example spot_historical_market_data_smoke
cargo run -p mexc --example public_wait_for_kind
cargo run -p mexc --example public_startup_report
cargo run -p mexc --example public_change_only_watch
cargo run -p mexc --example public_all_symbols_bootstrap
cargo run -p mexc --example public_state_handoff
cargo run -p mexc --example public_state_handoff_report
cargo run -p mexc --example public_state_change_feed
cargo run -p mexc --example spot_order_book_smoke
cargo run -p mexc --example futures_order_book_smoke
cargo run -p mexc --example futures_funding_rate_smoke
cargo run -p mexc --example futures_contract_channels_smoke
cargo run -p mexc --example futures_deals_raw_smoke
cargo run -p mexc --example futures_depth_modes_smoke
cargo run -p mexc --example futures_contract_change_feed
```

`mexc_public_ws_matrix` теперь не просто ждёт payload-ы, а даёт capability-style report по
каждому каналу:

- `ack_status=acked`
- `ack_status=blocked`
- `live_status=seen`
- `live_status=missing`

Полезные env-переменные:

- `MEXC_SPOT_SYMBOL=BTCUSDT`
- `MEXC_FUTURES_SYMBOL=BTC_USDT`
- `MEXC_WS_MATRIX_SECONDS=30`

Live evidence on 2026-06-28:

- `BTCUSDT`, `ETHUSDT`, `SOLUSDT`:
  - `spot@public.increase.depth.v3.api.pb@<symbol>` returned `ack_status=blocked`
  - `spot@public.bookTicker.v3.api.pb@<symbol>` returned `ack_status=blocked`
  - available spot channels like `increase.depth.batch`, `aggre.depth`,
    `aggre.bookTicker`, `bookTicker.batch`, `kline`, `miniTicker`, `miniTickers`
    were `acked + seen`
- `BTC_USDT` short futures matrix:
  - natural `push.contract` was `acked + seen`
- dedicated `futures_contract_channel_report` (`35s`) later confirmed:
  - `contract_payloads=2`
  - `first_contract_payload_after_ms=1562`

Это важный practical distinction для внешнего screener consumer:

- если канал реально доступен, matrix это подтвердит
- если MEXC сам его блокирует, это будет видно явно
- если change-only поток просто молчит в коротком окне, это уже не будет
  путаться с blocked subscribe

## Генерация полного public coverage

1. Получить полный universe:
   `let (spot_symbols, futures_symbols) = rest.all_public_symbols().await?;`
2. Построить полный план:
   `build_spot_public_subscriptions(&spot_symbols, &MexcSpotCoverageConfig::exhaustive())`
   `build_futures_public_subscriptions(&futures_symbols, &MexcFuturesCoverageConfig::exhaustive())`
3. Поднять sharded spot streams:
   `spot_ws.connect_sharded(spot_subscriptions).await?`
4. Поднять futures streams:
   `futures_ws.connect(futures_subscriptions).await?`

Или использовать готовый runtime builder:

`MexcConnector::default().public_runtime_builder().connect(MexcPublicRuntimeConfig::balanced()).await?`

Если нужен snapshot перед live-stream:

`MexcConnector::default().public_runtime_builder().bootstrap_snapshot().await?`

Или snapshot + stream одним вызовом:

`MexcConnector::default().public_runtime_builder().connect_with_snapshot(MexcPublicRuntimeConfig::balanced()).await?`

Если нужен более полный futures reference bootstrap для всех инструментов
(`index_price`, `fair_price`, `funding_rate`) перед стартом runtime:

`MexcConnector::default().public_runtime_builder().bootstrap_deep_snapshot(DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY).await?`

Если нужен сразу готовый путь `deep snapshot + live runtime`:

`MexcConnector::default().public_runtime_builder().connect_with_deep_snapshot(MexcPublicRuntimeConfig::balanced(), DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY).await?`

Если нужен stateful runtime, который сам применяет live события к `MexcPublicState`:

`MexcConnector::default().public_runtime_builder().connect_stateful_with_deep_snapshot(MexcPublicRuntimeConfig::balanced(), DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY).await?`

Этот state теперь держит не только per-symbol market data, но и публичные
global metadata snapshots:

- `spot_server_time`
- `spot_default_symbols`
- `spot_offline_symbols`
- `spot_exchange_info`
- `futures_server_time`
- `futures_transferable_currencies`
- `futures_insurance_balances`

Если нужен managed stateful runtime, который помнит свой режим запуска и умеет
по запросу пересобрать себя в том же режиме:

`MexcConnector::default().public_runtime_builder().connect_managed_stateful_with_deep_snapshot(MexcPublicRuntimeConfig::balanced(), DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY).await?`

Если нужен уже готовый one-call embed entrypoint без копирования startup glue из
examples, есть:

`MexcConnector::default().connect_managed_balanced_ready(Duration::from_secs(30)).await?`

И аналогичный exhaustive helper:

`MexcConnector::default().connect_managed_exhaustive_ready(Duration::from_secs(30)).await?`

Оба helper-а возвращают `MexcManagedReadyRuntime`:

- `runtime` - уже поднятый managed runtime
- `startup` - готовый `MexcLiveKindWaitReport`
- `contract_gap_warmup` - отчёт по стартовому futures contract gap-backfill до стабилизации

Минимальный embed shape:

```rust
let ready = MexcConnector::default()
    .connect_managed_balanced_ready(Duration::from_secs(30))
    .await?;

if !ready.is_ready() {
    anyhow::bail!("mexc startup not ready");
}

println!(
    "contract gaps: {} -> {} in {} pass(es), stop_reason={:?}",
    ready.contract_gap_warmup.initial_gap_count(),
    ready.contract_gap_warmup.final_gap_count(),
    ready.contract_gap_warmup.passes,
    ready.contract_gap_warmup.stop_reason
);

let mut runtime = ready.runtime;
```

Live proof on 2026-06-28 (`cargo run -q -p mexc --example public_managed_ready_smoke`):

- `startup_ready=true`
- `coverage=3/3`
- `contract_gap_warmup_passes=1`
- `contract_gap_initial=7`
- `contract_gap_final=0`
- `contract_gap_stop_reason=Converged`

Это закрывает важный embed-gap: one-call helper теперь возвращает не просто
runtime после live startup readiness, а уже максимально догретый futures
contract universe, even если редкие change-only `push.contract` /
`push.event.contract` в этот момент молчат.

Если нужен один управляемый шаг, который отдаёт очередной event вместе с
housekeeping-метаданными (pending reset, health alerts, optional heal outcome):

`runtime.next_step(&MexcManagedRuntimePolicy::balanced_defaults()).await`

Дальше consumer может просто вызывать:

`runtime.next_live_event().await`

Для futures REST kline family клиент теперь экспонирует и documented time-window
параметры:

- `futures_klines(symbol, interval, limit, start_time, end_time)`
- `futures_index_price_klines(symbol, interval, limit, start_time, end_time)`
- `futures_fair_price_klines(symbol, interval, limit, start_time, end_time)`

Live API принимает `start_time` / `end_time` как seconds-based `start` / `end`.
Для отдельной live-проверки есть:

`cargo run -p mexc --example public_futures_kline_window_smoke`

Для handoff-ready использования поверх official docs у futures REST client теперь
есть и docs-aligned alias-имена наряду с semantic wrapper-ами:

- `futures_ping()` -> alias к `futures_server_time()`
- `futures_support_currencies()` -> alias к `futures_transferable_currencies()`
- `futures_deals()` -> alias к `futures_recent_deals()`
- `futures_trend_data()` -> alias к `futures_ticker()`
- `futures_risk_reverse()` -> alias к `futures_insurance_fund_balance()`
- `futures_risk_reverse_history()` -> alias к `futures_insurance_fund_balance_history()`

Это убирает лишний mental mapping, когда consumer читает официальную MEXC docs и
хочет вызывать методы почти теми же именами, что и разделы/endpoint-ы в docs.

Live proof на `mexc_public_rest_matrix` 2026-06-28:

- `ok futures_ping 1782647141343`
- `ok futures_support_currencies 44`
- `ok futures_deals 2`
- `ok futures_trend_data BTC_USDT`
- `ok futures_risk_reverse 929`
- `ok futures_risk_reverse_history 2`

Для futures contract universe REST path клиент теперь сначала идёт в
docs-aligned endpoint:

- `/api/v1/contract/detail`

и только если он не сработал, пытается fallback:

- `/api/v1/contract/detail/country`

Если нужен explicit fallback для редкого `push.contract`, managed runtime теперь
умеет ручной refresh futures contract snapshot через REST:

`runtime.refresh_futures_contract_snapshot().await?`

Если нужен подробный delta-report по этому refresh:

`runtime.refresh_futures_contract_snapshot_with_report().await?`

Если нужен не full snapshot, а targeted refresh только по конкретным futures
symbols, есть:

`runtime.refresh_futures_contract_symbols_with_report(&["BTC_USDT".to_string()]).await?`

Этот targeted path:

- не чистит весь существующий contract universe
- upsert-ит только запрошенные symbols в текущий `MexcPublicState`
- возвращает тот же machine-readable delta-report
- дополнительно помечает:
  - `cause` (`interval`, `targeted` или `gap_backfill`)
  - `requested_symbols`
  - `unresolved_requested_symbols`
  - `used_full_snapshot_fallback`

Balanced managed policy теперь также умеет queue-ить contract refresh signals из:

- typed `futures.contract`
- typed `futures.eventContract`
- drifted `Raw(push.contract)`
- drifted `Raw(push.event.contract)`

и затем через `next_step(...)` делать targeted REST refresh по конкретным
symbols, не дожидаясь большого interval refresh.

Этим управляют policy knobs:

- `policy.contract_event_refresh = true | false`
- `policy.contract_event_refresh_cooldown = Duration::from_secs(...)`
- `policy.contract_gap_refresh = true | false`
- `policy.contract_gap_refresh_cooldown = Duration::from_secs(...)`
- `policy.contract_gap_refresh_batch_size = ...`

`contract_gap_refresh` использует текущий
`without_contract_with_other_state_symbols` из `MexcPublicState` и пытается
мягко догидратировать их targeted REST path-ом без destructive full-snapshot
fallback-а.

Это важно: часть live-only futures symbols реально восстанавливается этим путём,
а для оставшихся report честно показывает `unresolved_requested_symbols`.

Live proof на 2026-06-28 (`70s` soak, `checkpoint=30s`):

- `contract_refresh_count=2`
- обе refresh операции имели `cause=GapBackfill`
- первая gap batch дала:
  - `added=12`
  - `unresolved_requested_symbols=["AEON_USDT","ASTEROID1_USDT","ASTEROIDSOL_USDT","FAI_USDT"]`
- вторая gap batch дала:
  - `added=9`
  - `unresolved_requested_symbols=["AEON_USDT","ASTEROID1_USDT","ASTEROIDSOL_USDT","FAI_USDT","MAGAALIENS_USDT","NAT_USDT","POD_USDT"]`
- финальный state handoff в soak report:
  - `with_contract=943`
  - `without_contract=33`

Если хочется вообще не держать внешний cron вокруг slow metadata, тот же managed
path умеет interval-driven auto-refresh через policy:

- `policy.contract_refresh_interval = Some(Duration::from_secs(...))`
- `runtime.next_step(&policy).await`

В обоих случаях report теперь показывает не только размер snapshot, но и
`added / updated / removed / unchanged` по contract universe.

Дополнительно contract refresh report теперь несёт symbol-level delta:

- `added_symbols`
- `updated_symbols`
- `removed_symbols`

И ещё более детальный field-level diff для changed contracts:

- `changes`
- каждый entry содержит:
  - `symbol`
  - `kind` (`Added` / `Updated` / `Removed`)
  - `changed_fields`
  - `field_changes`

А каждый `field_changes` entry содержит:

- `field`
- `before`
- `after`

Если нужен external-consumer-friendly machine-readable snapshot именно по текущему
`MexcPublicState`, есть:

`cargo run -p mexc --example public_state_handoff_report`

Полезные env-переменные:

- `MEXC_STATE_HANDOFF_SECONDS=20`
- `MEXC_STATE_HANDOFF_REPORT_PATH=/tmp/mexc-state-handoff-report.json`

Этот artifact включает:

- global public metadata counts
- coverage summary по `spot_symbols` и `futures_symbols`
- сколько символов реально имеют `24hr / price / book / depth / funding / index / fair / kline`
- явный contract-gap truth для futures:
  - `without_contract`
  - `without_contract_with_other_state`
- compact readiness snapshot для `BTCUSDT` и `BTC_USDT`

На short live run 2026-06-28 (`8s`) artifact показал, например:

- `applied_events=2831`
- `live_applied_events=600`
- `spot.total_symbols=9911`
- `futures.total_symbols=929`
- `spot.with_agg_depth=165`
- `futures.with_contract=922`
- `futures.without_contract=7`
- `futures.without_contract_with_other_state=7`
- `futures.with_ticker=929`
- `futures.with_index_price=929`
- `futures.with_fair_price=929`

Это полезно как handoff truth: внешний consumer сразу видит не только сами counts,
но и насколько конкретный runtime уже гидратировал нужные ему куски state, плюс
какая часть futures universe уже seeded другой state-информацией даже без текущего
`contract` snapshot entry.

Если внешнему consumer нужен не point-in-time artifact, а change-only NDJSON feed
по мере того, как state реально гидратируется в managed runtime, есть:

`cargo run -p mexc --example public_state_change_feed`

Полезные env-переменные:

- `MEXC_STATE_FEED_SECONDS=20`
- `MEXC_STATE_FEED_INTERVAL_SECONDS=2`
- `MEXC_STATE_FEED_PATH=/tmp/mexc-state-change-feed.ndjson`

Этот feed:

- ждёт balanced startup managed runtime
- пишет `state_snapshot` только когда `runtime.state_handoff_report()` реально изменился
- даёт внешнему consumer готовый machine-readable hydration timeline
- показывает reconnect/rebuild counters рядом с coverage state

На short live run 2026-06-28 (`8s`) feed дал:

- `emitted_snapshots=5`
- `total_live_events=54038`
- `total_rebuilds=0`
- `total_spot_resets=0`
- `total_futures_resets=0`

На финальном snapshot того же run:

- `spot.total_symbols=9911`
- `futures.total_symbols=976`
- `futures.with_contract=922`
- `futures.without_contract=54`
- `futures.without_contract_with_other_state=54`
- `futures.with_depth=917`
- `futures.with_latest_deals=557`
- `futures.with_any_kline=558`

Это важная operational truth: live futures channels могут seed-ить symbols в state
раньше или шире, чем текущий contract snapshot universe, и feed делает это явным
для внешнего скринера без отдельного внутреннего diff-кода.

Если внешнему consumer нужен уже готовый NDJSON-style change feed поверх этого
fallback-механизма, есть:

`cargo run -p mexc --example futures_contract_change_feed`

Полезные env-переменные:

- `MEXC_CONTRACT_CHANGE_FEED_SECONDS=30`
- `MEXC_CONTRACT_CHANGE_FEED_INTERVAL_SECONDS=5`
- `MEXC_CONTRACT_CHANGE_FEED_PATH=/tmp/mexc-contract-change-feed.ndjson`

На short live run 2026-06-28:

- `total_refreshes=2`
- `change_refreshes=0`

То есть сам feed-путь уже operationally proved even on a stable contract universe.

Для deterministic live-проверки targeted path есть:

`cargo run -p mexc --example public_contract_targeted_refresh_smoke`

На live run 2026-06-28 пример показал:

- `startup_ready=true`
- `coverage=3/3`
- `symbol=BTC_USDT`
- `cause=targeted`
- `requested_symbols=["BTC_USDT"]`
- `refreshed_contracts=1`
- `added=1 updated=0 removed=0 unchanged=0`
- `after_has_contract=true`

Это отдельный важный operational guarantee: even если редкий change-only
websocket signal только намекнул на изменение контракта, у runtime уже есть
проверенный targeted REST path, чтобы быстро догидратировать точный symbol-level
contract state без полного re-snapshot всего futures universe.

Если нужен long-running durable collector именно для редких contract signals, есть:

`cargo run -p mexc --example futures_contract_signal_feed`

Если нужен уже готовый launcher, который поднимет одновременно:

- rare `contract/event.contract` signal collector
- общий managed runtime soak observer

есть:

`/opt/rust-market-data/scripts/start_mexc_watchers.sh`

Полезные env-переменные:

- `MEXC_CONTRACT_SIGNAL_FEED_SECONDS=60`
- `MEXC_CONTRACT_SIGNAL_FEED_SECONDS=0` для режима `run forever`
- `MEXC_CONTRACT_SIGNAL_CHECKPOINT_SECONDS=30`
- `MEXC_CONTRACT_SIGNAL_REFRESH=1`
- `MEXC_CONTRACT_SIGNAL_FEED_PATH=/tmp/mexc-contract-signal-feed.ndjson`
- `MEXC_CONTRACT_SIGNAL_SUMMARY_PATH=/tmp/mexc-contract-signal-summary.json`

Этот feed пишет NDJSON lines для:

- `session_start`
- `ack`
- `typed_contract_payload`
- `typed_event_contract_payload`
- `raw_contract_frame`
- `signal_refresh`
- `stream_error`
- `checkpoint`
- `summary`

Идея простая: это durable collector, который можно оставить крутиться долго и
потом уже разбирать реальные редкие `push.contract` / `push.event.contract`
frames вместе с symbol-targeted REST refresh reactions.

Практически он теперь полезнее short smoke-варианта:

- переживает transient websocket errors, не теряя весь прогон целиком
- пишет periodic checkpoint snapshots даже если payload так и не пришёл
- поддерживает отдельный summary JSON path для внешнего watchdog/sidecar
- на `MEXC_CONTRACT_SIGNAL_FEED_SECONDS=0` подходит для долгого фонового сбора evidence

На short live run 2026-06-28 (`8s`, `checkpoint=3s`) feed показал:

- `baseline_contracts=922`
- `acked_contract=true`
- `acked_event_contract=true`
- `session_starts=1`
- `typed_contract_payloads=0`
- `typed_event_contract_payloads=0`
- `raw_contract_frames=0`
- `raw_event_contract_frames=0`
- `stream_errors=0`
- `signal_refreshes=0`
- `emitted_lines=7`
- `stop_reason="deadline"`

А NDJSON `kind`-ы были:

- `session_start`
- `ack`
- `ack`
- `checkpoint`
- `checkpoint`
- `checkpoint`
- `summary`

Summary JSON в `/tmp/mexc-contract-signal-summary-v2.json` на этом прогоне тоже
подтвердил те же counters и пригоден как machine-readable heartbeat для долгого
наблюдения за редким `push.contract`.

Для launcher-а полезны отдельные env-переменные:

- `MEXC_OBSERVER_SECONDS=1800`
- `MEXC_CONTRACT_SIGNAL_OBSERVER_SECONDS=0` если signal collector нужно оставить бесконечно
- `MEXC_SOAK_OBSERVER_SECONDS=1800`
- `MEXC_OBSERVER_CHECKPOINT_SECONDS=60`
- `MEXC_OBSERVER_OUTPUT_ROOT=/opt/rust-market-data/runtime/mexc-observers/<run>`

Launcher:

- сначала собирает оба example binary
- потом стартует их через `setsid`
- кладёт в один каталог:
  - `futures-contract-signal.ndjson`
  - `futures-contract-signal-summary.json`
  - `public-soak-report.json`
  - `*.log`
  - `*.pid`
  - `launcher-metadata.txt`

На коротком launcher smoke run 2026-06-28 (`12s`, `checkpoint=4s`, output root
`/tmp/mexc-observer-test2`) было подтверждено:

- signal summary: `kind="summary"`, `watch_seconds=12`, `stop_reason="deadline"`, `emitted_lines=7`
- soak report: `watch_seconds=12`, `checkpoint_every_seconds=4`, `total_live_events=94407`, `health.healthy=true`

После upgrade `public_soak_report` launcher теперь получает live-updating soak JSON,
а не только файл в самом конце окна. Это было отдельно подтверждено на smoke run
2026-06-28 (`30s`, `checkpoint=10s`, output root `/tmp/mexc-observer-test4`):

- mid-run файл уже существовал на `10s`
- он имел `kind="checkpoint"`
- `complete=false`
- `checkpoint_count=1`

Это удобно, если нужно оставить живой сбор доказательств без ручного запуска
двух разных commands.

Для быстрого чтения уже идущего background run есть:

`/opt/rust-market-data/scripts/mexc_watchers_status.sh`

Он показывает:

- текущий run directory
- `started_at_utc`, `checkpoint_seconds`
- PID и live `ps`-статус обоих observers
- signal counters (`typed_contract_payloads`, `typed_event_contract_payloads`, `last_signal_*`)
- soak `state_handoff` coverage
- `without_contract_symbols`
- `with_event_contract_symbols`

Для live-проверки нового one-call embed helper есть:

`cargo run -p mexc --example public_managed_ready_smoke`

На live run 2026-06-28:

- `startup_ready=true`
- `coverage=3/3`
- `observed_live_events=68`
- `spot_connections=298`
- `futures_connections=37`
- `spot_subscriptions=8926`
- `futures_subscriptions=7379`

Operator-oriented примеры теперь тоже видят refresh `cause` и `requested_symbols`:

- `public_runtime_watch` читает managed `next_step(policy)` напрямую
- `public_soak_report` сохраняет отдельные counters:
  - `contract_refresh_interval_count`
  - `contract_refresh_targeted_count`
  - `contract_refresh_requested_symbol_total`

На short live runs 2026-06-28 с `MEXC_CONTRACT_REFRESH_INTERVAL_SECONDS=3`:

- `public_runtime_watch` печатал:
  - `contract_refresh cause=Interval requested_symbols=[] refreshed_contracts=922 ...`
- `public_soak_report` сохранил:
  - `contract_refresh_count=2`
  - `contract_refresh_interval_count=2`
  - `contract_refresh_targeted_count=0`
  - `contract_refresh_requested_symbol_total=0`

Для spot/futures public metadata есть отдельный managed refresh path:

- `runtime.refresh_public_metadata_snapshot().await?`
- `runtime.refresh_public_metadata_snapshot_with_report().await?`

Он обновляет текущий `MexcPublicState` для:

- `spot_server_time`
- `spot_default_symbols`
- `spot_offline_symbols`
- `spot_exchange_info`
- `futures_server_time`
- `futures_transferable_currencies`
- `futures_insurance_balances`

Если нужен interval-driven refresh этого metadata через managed loop:

- `policy.public_metadata_refresh_interval = Some(Duration::from_secs(...))`
- `runtime.next_step(&policy).await`

Для отдельной live-проверки есть:

`cargo run -p mexc --example public_metadata_refresh_smoke`

Для operator-style watch без собственного policy loop есть ещё env в
`public_runtime_watch`:

- `MEXC_PUBLIC_METADATA_REFRESH_INTERVAL_SECONDS=...`
- `MEXC_CONTRACT_REFRESH_INTERVAL_SECONDS=...`

А `public_soak_report` теперь умеет сохранять cumulative refresh telemetry:

- `public_metadata_refresh_count`
- `public_metadata_default_symbols_changed_count`
- `public_metadata_offline_symbols_changed_count`
- `public_metadata_exchange_info_changed_count`
- `public_metadata_transferable_currencies_changed_count`
- `public_metadata_insurance_balances_changed_count`
- `contract_refresh_count`
- `contract_refresh_added`
- `contract_refresh_updated`
- `contract_refresh_removed`
- `contract_refresh_unchanged`

и читать уже обновлённое состояние через:

`runtime.state()`

Если consumer использует именно `next_live_event()` и не читает lifecycle через
`next_event()`, после каждого live-event можно дополнительно проверять:

`runtime.take_pending_reset_report()`

Если report не `None`, значит один из websocket session был переподнят, и runtime
уже очистил reconnect-чувствительные live caches. Это удобный сигнал, что внешнему
скринеру пора заново прогреть свои производные структуры, если он держит их поверх
state модуля.

Если нужен не только сигнал о reset, но и готовый helper, который дождётся
минимального rewarm после reconnect, можно вызывать:

`runtime.await_recovery_after_pending_reset(Duration::from_secs(30)).await`

Сейчас recovery helper ждёт:

- для spot reset: `spot.aggDepth`
- для futures reset: `futures.indexPrice` и `futures.fairPrice`

И возвращает combined report: какой reset был замечен и насколько recovery уже
дошёл до readiness по затронутой стороне.

## Spot historical downloads

Официальная spot docs ссылается на `Download Historical Market Data`. В crate
теперь есть typed client для этого public tree:

- `rest.spot_history_symbol_ids(SpotHistoricalDataKind::Kline)`
- `rest.spot_history_symbol_periods(kind, symbol_id)`
- `rest.spot_history_symbol_intervals(kind, symbol_id, period)`
- `rest.spot_history_kline_files(symbol_id, period, interval)`
- `rest.spot_history_trade_files(symbol_id, period)`
- `rest.spot_history_symbols_for_symbol_id(symbol_id)`

Быстрый live smoke:

`cargo run -p mexc --example spot_historical_market_data_smoke`

Optional heavy mode:

- `MEXC_HISTORY_BUILD_INDEX=1 cargo run -p mexc --example spot_historical_market_data_smoke`

Замечание по semantics:

- быстрый smoke проверяет tree navigation и real downloadable file metadata
- текущий live historical root на 2026-06-28 отдавал `SPOT2/kline/` и пустой `SPOT2/trades/`
- полный `spot_history_symbol_id_map(...)` это уже тяжёлый offline-style проход по
  всему history tree, а не лёгкий startup probe

Если нужен уже готовый long-running operator/soak пример для полного public runtime:

`cargo run -p mexc --example public_runtime_watch`

Полезные env-переменные для него:

- `MEXC_WATCH_SECONDS=300`
- `MEXC_WATCH_SUMMARY_EVERY_SECONDS=15`
- `MEXC_RECOVERY_WAIT_SECONDS=30`
- `MEXC_HEALTH_MAX_AGE_SECONDS=5`
- `MEXC_HEALTH_MIN_STALE_SECONDS=5`
- `MEXC_USE_DEFAULT_HEALTH_POLICY=1`
- `MEXC_SELF_HEAL_STALE_SECONDS=0`
- `MEXC_SELF_HEAL_STARTUP_WAIT_SECONDS=30`
- `MEXC_SELF_HEAL_COOLDOWN_SECONDS=60`

В summary этот пример теперь дополнительно печатает freshness/health по critical
balanced kinds, чтобы сразу было видно, не затух ли один из ключевых always-on
stream-ов даже без явного reconnect.

По умолчанию health-policy теперь kind-specific:

- `spot.aggDepth` -> 2s
- `spot.bookTickerBatch` -> 5s
- `futures.depth` -> 2s
- `futures.ticker` -> 5s
- `futures.indexPrice` / `futures.fairPrice` -> 10s

Если нужна более грубая единая политика, можно отключить default policy через
`MEXC_USE_DEFAULT_HEALTH_POLICY=0` и задать единый `MEXC_HEALTH_MAX_AGE_SECONDS`.

`public_runtime_watch` теперь дополнительно печатает alert-style transitions:

- `newly_stale`
- `persistent_stale`
- `recovered`

То есть пример уже можно использовать не только как throughput/summary watcher, но и
как базовый сигнализатор деградации critical public stream-ов.

Если нужен именно machine-readable soak artifact для completion-аудита или long-run
evidence, есть:

`cargo run -p mexc --example public_soak_report`

Полезные env-переменные:

- `MEXC_SOAK_SECONDS=600`
- `MEXC_SOAK_AUTO_HEAL=1`
- `MEXC_SOAK_CHECKPOINT_EVERY_SECONDS=30`
- `MEXC_SOAK_REPORT_PATH=/tmp/mexc-soak-report.json`

Report теперь включает:

- `kind` (`checkpoint` или `summary`)
- `elapsed_seconds`
- `complete`
- `state_handoff`
- `manifest`
- `deep_bootstrap`
- periodic `checkpoints`
- compact `timeline` для meaningful reset/heal/persistent-stale transitions

Если задан `MEXC_SOAK_REPORT_PATH`, файл теперь обновляется не только в самом
конце, но и на каждом checkpoint. Это делает `public_soak_report` пригодным для
фонового launcher/sidecar сценария, где внешний процесс хочет читать текущее
machine-readable состояние long run прямо во время прогона.

`state_handoff` особенно полезен для внешнего screener-а, потому что теперь тот
же soak artifact прямо несёт coverage текущего `MexcPublicState`, включая:

- `futures.with_contract`
- `futures.without_contract`
- `futures.without_contract_symbols`
- `futures.with_event_contract`
- `futures.with_event_contract_symbols`
- `futures.with_ticker`
- `futures.with_depth`

На live run 2026-06-28 (`8s`) это уже подтвердилось:

- `state_handoff.spot.total_symbols=9911`
- `state_handoff.futures.total_symbols=976`
- `state_handoff.futures.with_contract=922`
- `state_handoff.futures.without_contract=54`
- `state_handoff.futures.with_event_contract=0`
- `state_handoff.futures.with_ticker=976`
- `state_handoff.futures.with_depth=912`

После перезапуска long-running launcher-а на 2026-06-28 с `checkpoint=30s`
это было подтверждено уже на живом фоне вместе с rare `event.contract`:

- signal checkpoint: `typed_event_contract_payloads=1`
- `last_signal_kind="push.event.contract"`
- `last_signal_symbol="ETH_USDT"`
- soak checkpoint: `state_handoff.futures.with_event_contract=1`

То есть `push.event.contract` не просто ловится transport-ом, а реально доходит
до `MexcPublicState` и виден во внешнем machine-readable handoff artifact.

Отдельный `public_state_handoff_report` теперь тоже несёт symbol-level arrays, а
не только counts. На live run 2026-06-28 это показало:

- `with_contract=922`
- `without_contract=7`
- `without_contract_symbols=["MXSOL_USDT","MX_USDT","STETH_USDT","TON_USDT","USD1_USDT","USDE_USDT","WBTC_USDT"]`
- `without_contract_with_other_state=7`
- `without_contract_with_other_state_symbols=["MXSOL_USDT","MX_USDT","STETH_USDT","TON_USDT","USD1_USDT","USDE_USDT","WBTC_USDT"]`
- `with_event_contract=0`
- `with_event_contract_symbols=[]`

Это уже даёт внешнему consumer-у точный список symbol-level contract gaps, а не
только абстрактный counter.

Если нужен machine-readable forced-reconnect report с реальным socket-break
через локальные websocket proxies, есть:

`cargo run -p mexc --example public_forced_reconnect_report`

Полезные env-переменные:

- `MEXC_FORCE_RECONNECT_CUT_AFTER_SECONDS=5`
- `MEXC_FORCE_RECONNECT_WATCH_AFTER_CUT_SECONDS=35`
- `MEXC_FORCE_RECONNECT_REPORT_PATH=/tmp/mexc-forced-reconnect-report.json`

Этот пример:

- поднимает managed runtime через локальные `ws://127.0.0.1:*` proxies
- насильно рвёт spot и futures websocket-сессии
- проверяет, что тот же runtime ловит pending resets и возвращается в healthy state

Если нужен live smoke именно для planned session rotation по max-age policy,
есть:

`cargo run -p mexc --example public_session_rotation_smoke`

Полезные env-переменные:

- `MEXC_ROTATE_AFTER_SECONDS=5`
- `MEXC_ROTATION_RECOVERY_WAIT_SECONDS=20`

Это проверяет, что runtime переживает не только аварийный socket-break, но и
управляемую ротацию websocket-сессий с recovery на том же инстансе.

По умолчанию это особенно важно для spot transport: docs ограничивает lifetime
одного spot websocket-соединения `24h`, поэтому `MexcSpotWsClient::default()`
теперь заранее ротирует сессию немного раньше этого лимита и по умолчанию
размазывает ротацию между shard-ами, чтобы не устраивать reconnect herd.

Если нужен отдельный machine-readable report именно по редким futures channels
`contract / event.contract`, есть:

`cargo run -p mexc --example futures_contract_channel_report`

Полезные env-переменные:

- `MEXC_CONTRACT_WATCH_SECONDS=300`
- `MEXC_CONTRACT_REPORT_PATH=/tmp/mexc-contract-report.json`

После parser-tolerance fix оба rare change-only futures канала уже имеют live
proof, но с разной частотой:

- `event.contract` подтверждался payload-ами даже на длинных окнах наблюдения
- `contract` теперь тоже подтверждён на коротком normal window:
  - `futures_contract_channel_report` (`35s`) дал `contract_payloads=2`
  - `first_contract_payload_after_ms=1562`

При этом оба канала всё равно могут молчать в отдельных коротких окнах, поэтому
для consumer важнее не ожидание “payload придёт всегда быстро”, а наличие:

- явного `ack`
- parser-tolerant transport
- REST refresh fallback
- capability/report tooling

Если нужен более практичный durable observer именно вокруг этого weak spot, есть:

`cargo run -p mexc --example futures_contract_observer_report`

Полезные env-переменные:

- `MEXC_CONTRACT_OBSERVER_SECONDS=60`
- `MEXC_CONTRACT_REFRESH_INTERVAL_SECONDS=15`
- `MEXC_CONTRACT_OBSERVER_REPORT_PATH=/tmp/mexc-contract-observer.json`

Этот observer одновременно:

- слушает `contract / event.contract`
- считает реальные WS payload-ы
- периодически снимает REST contract snapshot delta
- пишет symbol-level delta arrays для каждого refresh entry:
  - `added_symbols`
  - `updated_symbols`
  - `removed_symbols`
- сохраняет machine-readable evidence, даже если `push.contract` так и не пришёл

На коротком live прогоне 2026-06-28 этот observer дал:

- `acked_contract=true`
- `acked_event_contract=true`
- `contract_payloads=0`
- `event_contract_payloads=0`
- `raw_contract_frames=0`
- `raw_event_contract_frames=0`
- `refresh_count=4`
- `refresh_total_unchanged=3640`
- symbol-level deltas were empty on a stable contract universe

На short run с field-level diff observer report (`/tmp/mexc-contract-observer-field-diff.json`)
2026-06-28:

- `refresh_count=3`
- все `added_symbols / updated_symbols / removed_symbols` были пустыми
- все `changes` были пустыми

Даже если в будущем `push.contract` / `push.event.contract` придут с drift-нувшей
схемой, transport теперь не падает фатально: такие frames уходят в `Raw` и могут
быть посчитаны observer-ом отдельно.

Практический live нюанс для spot snapshot bootstrap:

- all-symbol `ticker/bookTicker` на MEXC может содержать отдельные инструменты с
  `null` bid/ask полями
- текущий модуль это уже терпит и не падает на `deep snapshot` / `deep handoff`

Если нужен отдельный live smoke именно для документированного raw futures trade mode
(`sub.deal` c `compress=false`), есть:

`cargo run -p mexc --example futures_deals_raw_smoke`

Полезные env-переменные:

- `MEXC_FUTURES_DEALS_SYMBOL=BTC_USDT`
- `MEXC_FUTURES_DEALS_WATCH_SECONDS=15`

На live прогоне 2026-06-28 этот режим реально отличался от default aggregated stream:

- aggregated: `messages=43`, `trades=60`
- raw: `messages=59`, `trades=59`

Если нужен отдельный live smoke именно для документированных depth knobs на futures ws:

- `sub.depth` c `compress=false`
- `sub.depth.full` c `limit=5`

есть:

`cargo run -p mexc --example futures_depth_modes_smoke`

На live прогоне 2026-06-28:

- incremental depth (`compress=false`): `version=39040488458 asks=0 bids=1`
- full depth (`limit=5`): `version=39040488465 asks=5 bids=5`

Если задать `MEXC_SELF_HEAL_STALE_SECONDS>0`, тот же пример перейдёт в managed mode:
при persistently stale critical kind-ах он пересоберёт полный managed runtime в том же
режиме (`deep snapshot + balanced stream`) и заново дождётся balanced startup.

Чтобы избежать rebuild storm, managed self-heal теперь уважает cooldown:

- если cooldown ещё не истёк, пример печатает `self_heal_suppressed`
- если cooldown истёк и stale condition сохраняется, печатает `self_heal`

Если нужен lifecycle-level контроль, `runtime.next_event().await` также возвращает
synthetic `SessionStart` события на каждый новый spot/futures websocket session.
На resumed session stateful runtime автоматически сбрасывает reconnect-чувствительные
поля вроде live depth/deals/kline caches, чтобы после reconnect не оставалось ложного
ощущения непрерывности там, где поток уже начался заново.

Если нужно дождаться конкретного типа live payload без ручной фильтрации потока:

`runtime.wait_for_live_event_kind(MexcPublicEventKind::FuturesIndexPrice, Duration::from_secs(20)).await`

Если нужен readiness-report по набору ключевых kinds:

`runtime.wait_for_live_event_kinds(&expected_kinds, Duration::from_secs(30)).await`

Если нужен observation-report для редких change-only channels:

`runtime.observe_live_event_kinds(&watched_kinds, Duration::from_secs(30)).await`

Для `balanced()` готовый рекомендованный startup set уже есть:

`BALANCED_STARTUP_KINDS`

И готовый readiness helper поверх него:

`runtime.await_balanced_startup(Duration::from_secs(30)).await`

Если runtime был поднят через deep snapshot, consumer также может посмотреть
режим futures reference bootstrap через:

`runtime.deep_report`

В report также есть разбивка, сколько `index/fair/funding` значений пришло из
bulk ticker snapshot, а сколько пришлось добирать отдельными endpoint-запросами.

## Локальный spot стакан

Для локальной книги заявок по spot:

1. Подписаться на `MexcSpotSubscription::AggDepth`.
2. Буферизовать первые апдейты через `MexcSpotOrderBookBootstrap`.
3. Взять REST snapshot `spot_order_book(symbol, Some(5000))`.
4. Инициализировать локальную книгу через `initialize_from_snapshot`.
5. После bootstrap применять новые `aggDepth` апдейты через `MexcSpotOrderBook::apply_update`.

Готовый live smoke:

`cargo run -p mexc --example spot_order_book_smoke`

## Локальный futures стакан

Для локальной книги заявок по futures:

1. Подписаться на `MexcFuturesSubscription::Depth`.
2. Буферизовать первые depth апдейты через `MexcFuturesOrderBookBootstrap`.
3. Взять REST snapshot `futures_depth(symbol)`.
4. Взять REST incremental commits `futures_depth_commits(symbol, 1000)`.
5. Инициализировать локальную книгу через `initialize_from_snapshot(snapshot, commits)`.
6. После bootstrap применять новые `depth` апдейты через `MexcFuturesOrderBook::apply_update`.

Готовый live smoke:

`cargo run -p mexc --example futures_order_book_smoke`

## Примечание

`balanced()` и `exhaustive()` теперь имеют разный operational смысл:

- `balanced()` — практичный full-universe runtime профиль. Он сохраняет ключевые live потоки
  для всех spot/futures инструментов, но убирает явные spot-дубли вроде per-symbol
  `miniTicker`, raw `bookTicker` и `limitDepth`, когда для always-on full-universe режима
  достаточно `miniTickers`, `bookTickerBatch`, snapshot bootstrap и `aggDepth`.
- `exhaustive()` — максимально полный WS coverage-профиль для валидации, исследований
  и случаев, когда нужно именно подписаться на весь публичный набор каналов, несмотря
  на большую нагрузку.

Практическая заметка по live MEXC на 2026-06-27:

- `spot@public.increase.depth.batch.v3.api.pb@<symbol>` работает.
- `spot@public.increase.depth.v3.api.pb@<symbol>` принимается proto-репозиторием, но live websocket возвращал `Blocked!`, поэтому этот канал не включён в стандартный coverage plan.
- futures `sub.contract` и `sub.event.contract` по официальной документации push-only on change: подписка подтверждается `ack`, а отсутствие payload в коротком окне само по себе не означает поломку канала.
