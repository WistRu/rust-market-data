use anyhow::Result;
use common::{MarketDataChannel, MarketDataConnector, Subscription};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct ConnectorSmoke {
    exchange: &'static str,
    endpoint: &'static str,
    subscriptions: Vec<String>,
}

struct ConnectorCase {
    connector: Box<dyn MarketDataConnector>,
    subscriptions: Vec<Subscription>,
}

fn main() -> Result<()> {
    let json = std::env::args().any(|arg| arg == "--json");
    let btcusdt_subscription_set = vec![
        Subscription {
            symbol: "BTCUSDT".to_string(),
            channel: MarketDataChannel::Ticker,
        },
        Subscription {
            symbol: "BTCUSDT".to_string(),
            channel: MarketDataChannel::Trades,
        },
        Subscription {
            symbol: "BTCUSDT".to_string(),
            channel: MarketDataChannel::OrderBook,
        },
    ];
    let okx_subscription_set = vec![
        Subscription {
            symbol: "BTC-USDT".to_string(),
            channel: MarketDataChannel::Ticker,
        },
        Subscription {
            symbol: "BTC-USDT".to_string(),
            channel: MarketDataChannel::Trades,
        },
        Subscription {
            symbol: "BTC-USDT".to_string(),
            channel: MarketDataChannel::OrderBook,
        },
    ];

    let cases = vec![
        ConnectorCase {
            connector: Box::new(mexc::MexcConnector::default()),
            subscriptions: btcusdt_subscription_set.clone(),
        },
        ConnectorCase {
            connector: Box::new(aster::AsterConnector::default()),
            subscriptions: btcusdt_subscription_set.clone(),
        },
        ConnectorCase {
            connector: Box::new(binance::BinanceConnector::default()),
            subscriptions: btcusdt_subscription_set.clone(),
        },
        ConnectorCase {
            connector: Box::new(bybit::BybitConnector),
            subscriptions: btcusdt_subscription_set,
        },
        ConnectorCase {
            connector: Box::new(okx::OkxConnector),
            subscriptions: okx_subscription_set,
        },
        ConnectorCase {
            connector: Box::new(bitget::BitgetConnector),
            subscriptions: vec![
                Subscription {
                    symbol: "BTCUSDT".to_string(),
                    channel: MarketDataChannel::Ticker,
                },
                Subscription {
                    symbol: "BTCUSDT".to_string(),
                    channel: MarketDataChannel::Trades,
                },
                Subscription {
                    symbol: "BTCUSDT".to_string(),
                    channel: MarketDataChannel::OrderBook,
                },
            ],
        },
    ];

    let reports = cases
        .iter()
        .map(|case| ConnectorSmoke {
            exchange: case.connector.exchange(),
            endpoint: case.connector.ws_endpoint(),
            subscriptions: case.connector.build_subscriptions(&case.subscriptions),
        })
        .collect::<Vec<_>>();

    if json {
        println!("{}", serde_json::to_string_pretty(&reports)?);
    } else {
        println!("handoff_consumer_smoke ready_connectors={}", reports.len());
        for report in &reports {
            println!(
                "ok {} endpoint={} subscriptions={}",
                report.exchange,
                report.endpoint,
                report.subscriptions.len()
            );
        }
    }

    Ok(())
}
