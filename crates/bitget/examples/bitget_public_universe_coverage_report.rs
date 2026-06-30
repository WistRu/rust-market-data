use anyhow::Result;
use bitget::{BitgetPublicRestClient, public_acceptance_report};

#[tokio::main]
async fn main() -> Result<()> {
    let report = public_acceptance_report(&BitgetPublicRestClient::default()).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
