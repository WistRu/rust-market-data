use anyhow::Result;
use common::{MarketDataChannel, MarketDataConnector, Subscription};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct ConnectorSmoke {
    exchange: &'static str,
    endpoint: &'static str,
    subscriptions: Vec<String>,
}

fn main() -> Result<()> {
    let json = std::env::args().any(|arg| arg == "--json");
    let subscription_set = vec![
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

    let connectors: Vec<Box<dyn MarketDataConnector>> = vec![
        Box::new(mexc::MexcConnector::default()),
        Box::new(aster::AsterConnector::default()),
        Box::new(binance::BinanceConnector::default()),
        Box::new(bybit::BybitConnector),
    ];

    let reports = connectors
        .iter()
        .map(|connector| ConnectorSmoke {
            exchange: connector.exchange(),
            endpoint: connector.ws_endpoint(),
            subscriptions: connector.build_subscriptions(&subscription_set),
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
