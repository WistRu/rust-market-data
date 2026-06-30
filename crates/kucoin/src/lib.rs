use common::{MarketDataConnector, Subscription};

pub struct KucoinConnector;

impl MarketDataConnector for KucoinConnector {
    fn exchange(&self) -> &'static str {
        "kucoin"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://ws-api-spot.kucoin.com/"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
