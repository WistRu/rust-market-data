use anyhow::{Context, Result, ensure};
use futures::StreamExt;
use mexc::{MexcConnector, MexcFuturesSubscription, MexcFuturesWsMessage};
use tokio::time::{Duration, timeout};

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let mut stream = connector
        .futures_ws
        .connect(vec![
            MexcFuturesSubscription::Contract,
            MexcFuturesSubscription::EventContract,
        ])
        .await
        .context("connect contract/event-contract websocket")?;

    let mut acked_contract = false;
    let mut acked_event_contract = false;
    let mut contract_payloads = 0usize;
    let mut event_contract_payloads = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while tokio::time::Instant::now() < deadline {
        let maybe_message = match timeout(Duration::from_secs(10), stream.next()).await {
            Ok(maybe_message) => maybe_message,
            Err(_) => continue,
        };
        let message = maybe_message
            .transpose()?
            .context("contract channel stream ended unexpectedly")?;

        match message {
            MexcFuturesWsMessage::Ack(ack) => {
                if ack.channel.as_deref() == Some("rs.sub.contract") {
                    acked_contract = true;
                    println!("ack contract");
                }
                if ack.channel.as_deref() == Some("rs.sub.event.contract") {
                    acked_event_contract = true;
                    println!("ack event_contract");
                }
            }
            MexcFuturesWsMessage::Contract(event) => {
                contract_payloads += 1;
                println!(
                    "contract_payload#{} symbol={} state={:?} max_leverage={:?}",
                    contract_payloads, event.data.symbol, event.data.state, event.data.max_leverage
                );
            }
            MexcFuturesWsMessage::EventContract(event) => {
                event_contract_payloads += 1;
                println!(
                    "event_contract_payload#{} symbol={} contract_id={}",
                    event_contract_payloads,
                    event.data.symbol,
                    event.data.contract_id.as_str_lossy()
                );
            }
            _ => {}
        }

        if acked_contract
            && acked_event_contract
            && contract_payloads > 0
            && event_contract_payloads > 0
        {
            break;
        }
    }

    ensure!(acked_contract, "did not receive contract ack");
    ensure!(acked_event_contract, "did not receive event contract ack");
    println!(
        "summary acked_contract={} acked_event_contract={} contract_payloads={} event_contract_payloads={}",
        acked_contract, acked_event_contract, contract_payloads, event_contract_payloads
    );
    Ok(())
}
