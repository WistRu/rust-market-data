use anyhow::Result;
use bybit::{BybitPublicRestClient, public_acceptance_report};

#[tokio::main]
async fn main() -> Result<()> {
    let report = public_acceptance_report(&BybitPublicRestClient::default()).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
