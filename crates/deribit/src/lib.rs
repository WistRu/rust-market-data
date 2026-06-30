use common::{MarketDataConnector, Subscription};

pub struct DeribitConnector;

impl MarketDataConnector for DeribitConnector {
    fn exchange(&self) -> &'static str {
        "deribit"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://www.deribit.com/ws/api/v2"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
