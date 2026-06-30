use common::{MarketDataConnector, Subscription};

pub struct KrakenConnector;

impl MarketDataConnector for KrakenConnector {
    fn exchange(&self) -> &'static str {
        "kraken"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://ws.kraken.com/v2"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
