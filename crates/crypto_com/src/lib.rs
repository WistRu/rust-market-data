use common::{MarketDataConnector, Subscription};

pub struct CryptoComConnector;

impl MarketDataConnector for CryptoComConnector {
    fn exchange(&self) -> &'static str {
        "crypto_com"
    }

    fn ws_endpoint(&self) -> &'static str {
        "wss://stream.crypto.com/exchange/v1/market"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
