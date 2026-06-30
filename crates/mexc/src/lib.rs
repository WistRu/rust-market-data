mod coverage;
mod futures_order_book;
mod futures_ws;
mod model;
mod public;
mod public_state;
mod rest;
mod spot_order_book;
mod spot_ws;

pub use coverage::*;
pub use futures_order_book::*;
pub use futures_ws::*;
pub use model::*;
pub use public::*;
pub use public_state::*;
pub use rest::*;
pub use spot_order_book::*;
pub use spot_ws::*;

use common::{MarketDataConnector, Subscription};

pub struct MexcConnector {
    pub rest: MexcPublicRestClient,
    pub spot_ws: MexcSpotWsClient,
    pub futures_ws: MexcFuturesWsClient,
}

impl Default for MexcConnector {
    fn default() -> Self {
        Self {
            rest: MexcPublicRestClient::default(),
            spot_ws: MexcSpotWsClient::default(),
            futures_ws: MexcFuturesWsClient::default(),
        }
    }
}

impl MexcConnector {
    pub fn public_runtime_builder(&self) -> MexcPublicRuntimeBuilder {
        MexcPublicRuntimeBuilder::new(
            self.rest.clone(),
            self.spot_ws.clone(),
            self.futures_ws.clone(),
        )
    }

    pub async fn connect_managed_balanced_ready(
        &self,
        startup_wait_for: std::time::Duration,
    ) -> anyhow::Result<MexcManagedReadyRuntime> {
        self.public_runtime_builder()
            .connect_managed_stateful_ready_with_deep_snapshot(
                MexcPublicRuntimeConfig::balanced(),
                DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
                startup_wait_for,
            )
            .await
    }

    pub async fn connect_managed_exhaustive_ready(
        &self,
        startup_wait_for: std::time::Duration,
    ) -> anyhow::Result<MexcManagedReadyRuntime> {
        self.public_runtime_builder()
            .connect_managed_stateful_ready_with_deep_snapshot(
                MexcPublicRuntimeConfig::exhaustive(),
                DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
                startup_wait_for,
            )
            .await
    }
}

impl MarketDataConnector for MexcConnector {
    fn exchange(&self) -> &'static str {
        "mexc"
    }

    fn ws_endpoint(&self) -> &'static str {
        self.spot_ws.endpoint()
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}

pub mod spot_proto {
    #![allow(clippy::all)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/mexc_spot_protos.rs"));
}
