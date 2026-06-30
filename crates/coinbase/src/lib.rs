use common::{MarketDataConnector, Subscription};

pub struct CoinbaseConnector;

impl MarketDataConnector for CoinbaseConnector {
    fn exchange(&self) -> &'static str {
        "coinbase"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://advanced-trade-ws.coinbase.com"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
