use anyhow::Result;
use okx::{
    DEFAULT_SPOT_INST_ID, DEFAULT_SWAP_INST_ID, OkxInstType, OkxInstrumentId, OkxPublicRestClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = OkxPublicRestClient::default();
    let spot_inst_id =
        std::env::var("OKX_SPOT_INST_ID").unwrap_or_else(|_| DEFAULT_SPOT_INST_ID.to_string());
    let swap_inst_id =
        std::env::var("OKX_SWAP_INST_ID").unwrap_or_else(|_| DEFAULT_SWAP_INST_ID.to_string());
    let spot = OkxInstrumentId::spot(spot_inst_id);
    let swap = OkxInstrumentId::swap(swap_inst_id);

    println!(
        "ok spot_instruments {}",
        rest.instruments(OkxInstType::Spot).await?.len()
    );
    println!(
        "ok swap_instruments {}",
        rest.instruments(OkxInstType::Swap).await?.len()
    );
    println!("ok identity {} {}", spot.inst_id, spot.inst_type);
    println!("ok identity {} {}", swap.inst_id, swap.inst_type);

    println!(
        "ok spot_orderbook bids={}",
        rest.order_book(&spot, Some(5)).await?.bids.len()
    );
    println!(
        "ok swap_orderbook bids={}",
        rest.order_book(&swap, Some(5)).await?.bids.len()
    );
    println!("ok spot_ticker last={}", rest.ticker(&spot).await?.last);
    println!("ok swap_ticker last={}", rest.ticker(&swap).await?.last);
    println!(
        "ok spot_trades {}",
        rest.trades(&spot, Some(5)).await?.len()
    );
    println!(
        "ok swap_trades {}",
        rest.trades(&swap, Some(5)).await?.len()
    );

    Ok(())
}
