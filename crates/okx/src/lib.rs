use common::{MarketDataConnector, Subscription};

pub struct OkxConnector;

impl MarketDataConnector for OkxConnector {
    fn exchange(&self) -> &'static str {
        "okx"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://ws.okx.com:8443/ws/v5/public"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
