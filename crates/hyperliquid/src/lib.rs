use common::{MarketDataConnector, Subscription};

pub struct HyperliquidConnector;

impl MarketDataConnector for HyperliquidConnector {
    fn exchange(&self) -> &'static str {
        "hyperliquid"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://api.hyperliquid.xyz/ws"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
