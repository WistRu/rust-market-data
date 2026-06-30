use common::{MarketDataConnector, Subscription};

pub struct BitunixConnector;

impl MarketDataConnector for BitunixConnector {
    fn exchange(&self) -> &'static str {
        "bitunix"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://fapi.bitunix.com/public/"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
