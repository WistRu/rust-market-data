use anyhow::Result;
use okx::{OkxPublicRestClient, public_acceptance_report};

#[tokio::main]
async fn main() -> Result<()> {
    let report = public_acceptance_report(&OkxPublicRestClient::default()).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
