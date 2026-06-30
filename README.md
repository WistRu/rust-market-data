# rust-market-data

Папка в `/opt` под переиспользуемые Rust-модули криптобирж для подписки на market data:
`/opt/rust-market-data`

## Структура

- `crates/common` - общие типы и trait для market data listener.
- `crates/binance` - handoff-ready public market-data модуль Binance spot/USD-M futures: REST, WS stream builder, full-universe coverage report и live smoke examples.
- `crates/coinbase` - каркас коннектора Coinbase market data.
- `crates/crypto_com` - каркас коннектора Crypto.com Exchange market data.
- `crates/deribit` - каркас коннектора Deribit market data.
- `crates/bybit` - handoff-ready public market-data модуль Bybit V5: spot/linear REST, WS stream builder, coverage report и live smoke examples.
- `crates/okx` - handoff-ready public market-data модуль OKX V5: spot/swap REST, WS subscription builder, acceptance report и live smoke examples.
- `crates/bitget` - каркас коннектора Bitget.
- `crates/hyperliquid` - каркас коннектора Hyperliquid market data.
- `crates/kraken` - каркас коннектора Kraken market data.
- `crates/kucoin` - каркас коннектора KuCoin.
- `crates/gateio` - каркас коннектора Gate.io.
- `crates/mexc` - модуль MEXC, уже заметно глубже простого каркаса.
- `crates/bitunix` - каркас коннектора Bitunix.
- `crates/aster` - handoff-ready public market-data модуль Aster DEX: spot/futures REST, WS stream builder и live smoke examples.
- `crates/acceptance` - workspace-level readiness inventory, acceptance reports и mini drift audit.
- `crates/handoff_consumer` - downstream-style smoke proof, который подключает handoff-ready crates через публичный `MarketDataConnector`.
- `scripts/new_exchange.sh` - скрипт для быстрого создания нового exchange crate.
- `scripts/list_exchanges.sh` - быстрый список текущих exchange-модулей и их public WS endpoint'ов.

## Идея

Каждый exchange crate должен реализовать trait `MarketDataConnector` из `common`.
Сейчас структура рассчитана на переиспользование в разных проектах:

- отдельный crate на каждую биржу
- единая абстракция `MarketDataConnector`
- базовый публичный WebSocket endpoint на уровне crate
- шаблон построения подписок
- общий формат market event для будущих слушателей, скринеров и ботов

## Добавленные биржи

Новые crates, которые я добавил в этот workspace:

- `coinbase` - `wss://advanced-trade-ws.coinbase.com`
- `crypto_com` - `wss://stream.crypto.com/exchange/v1/market`
- `deribit` - `wss://www.deribit.com/ws/api/v2`
- `hyperliquid` - `wss://api.hyperliquid.xyz/ws`
- `kraken` - `wss://ws.kraken.com/v2`

## Быстро добавить новую биржу

```bash
cd /opt/rust-market-data
./scripts/new_exchange.sh exchange_slug wss://your-public-ws-endpoint
```

Пример:

```bash
./scripts/new_exchange.sh kraken wss://your-public-ws-endpoint
```

Скрипт:

- создаёт `crates/<exchange_slug>/Cargo.toml`
- создаёт `crates/<exchange_slug>/src/lib.rs`
- добавляет crate в workspace root `Cargo.toml`, если его там ещё нет

## Посмотреть, что уже есть

```bash
cd /opt/rust-market-data
./scripts/list_exchanges.sh
```

Скрипт выводит:

- slug биржи
- public WebSocket endpoint, который сейчас зашит в crate

## Дальше

1. Запустить readiness inventory: `cargo run -p acceptance -- inventory`.
2. Проверить готовый модуль: `cargo run -p acceptance -- report bybit`.
3. Проверить текущий ready set: `cargo run -p acceptance -- drift-audit`.
4. Проверить downstream-style потребление: `cargo run -p handoff-consumer`.
5. Для следующей биржи идти по `docs/connector-factory.md`, а не считать crate с endpoint строкой готовым коннектором.
