use common::{MarketDataConnector, Subscription};

pub struct BitgetConnector;

impl MarketDataConnector for BitgetConnector {
    fn exchange(&self) -> &'static str {
        "bitget"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://ws.bitget.com/v2/ws/public"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
