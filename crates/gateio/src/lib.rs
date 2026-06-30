use common::{MarketDataConnector, Subscription};

pub struct GateIoConnector;

impl MarketDataConnector for GateIoConnector {
    fn exchange(&self) -> &'static str {
        "gateio"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://api.gateio.ws/ws/v4/"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
