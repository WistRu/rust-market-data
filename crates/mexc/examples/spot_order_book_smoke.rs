use anyhow::{Context, Result, anyhow, ensure};
use futures::StreamExt;
use mexc::{
    MexcConnector, MexcSpotOrderBookBootstrap, MexcSpotSubscription, MexcSpotUpdateSpeed,
    MexcSpotWsMessage, SpotBookApplyOutcome, SpotBookBootstrapOutcome,
};
use tokio::time::{Duration, Instant, timeout};

#[tokio::main]
async fn main() -> Result<()> {
    let symbol = "BTCUSDT";
    let connector = MexcConnector::default();
    let mut stream = connector
        .spot_ws
        .connect(vec![MexcSpotSubscription::AggDepth {
            symbol: symbol.to_string(),
            speed: MexcSpotUpdateSpeed::Ms100,
        }])
        .await
        .context("connect spot aggDepth websocket")?;

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut bootstrap = MexcSpotOrderBookBootstrap::new(symbol);
    let mut initialized_book = None;
    let mut applied_live_updates = 0usize;

    while Instant::now() < deadline {
        let message = timeout(Duration::from_secs(10), stream.next())
            .await
            .context("wait for aggDepth event")?
            .transpose()?
            .context("spot websocket ended unexpectedly")?;

        let MexcSpotWsMessage::AggDepth(event) = message else {
            continue;
        };
        if !bootstrap.push_envelope(&event)? {
            continue;
        }

        if initialized_book.is_none() {
            let snapshot = connector
                .rest
                .spot_order_book(symbol, Some(5000))
                .await
                .context("fetch spot depth snapshot")?;

            match bootstrap.initialize_from_snapshot(&snapshot)? {
                SpotBookBootstrapOutcome::SnapshotTooOld {
                    last_update_id,
                    first_from_version,
                } => {
                    println!(
                        "snapshot_retry last_update_id={} first_from_version={} buffered_updates={}",
                        last_update_id,
                        first_from_version,
                        bootstrap.buffered_len()
                    );
                    continue;
                }
                SpotBookBootstrapOutcome::NeedsResync {
                    current_version,
                    from_version,
                    to_version,
                } => {
                    println!(
                        "bootstrap_resync current_version={} from_version={} to_version={}",
                        current_version, from_version, to_version
                    );
                    bootstrap = MexcSpotOrderBookBootstrap::new(symbol);
                    bootstrap.push_envelope(&event)?;
                    continue;
                }
                SpotBookBootstrapOutcome::Ready(book) => {
                    let best_bid = book
                        .best_bid()
                        .context("best bid missing after bootstrap")?;
                    let best_ask = book
                        .best_ask()
                        .context("best ask missing after bootstrap")?;
                    println!(
                        "initialized version={} buffered_updates={} bid={}/{} ask={}/{}",
                        book.last_update_id,
                        bootstrap.buffered_len(),
                        best_bid.0,
                        best_bid.1,
                        best_ask.0,
                        best_ask.1
                    );
                    initialized_book = Some(book);
                    continue;
                }
            }
        }

        let book = initialized_book
            .as_mut()
            .ok_or_else(|| anyhow!("book should be initialized before live apply"))?;
        match book.apply_update(&event.data)? {
            SpotBookApplyOutcome::Applied {
                from_version,
                to_version,
            } => {
                applied_live_updates += 1;
                let best_bid = book.best_bid().context("best bid missing after update")?;
                let best_ask = book.best_ask().context("best ask missing after update")?;
                println!(
                    "live_update#{} {}->{} bid={}/{} ask={}/{}",
                    applied_live_updates,
                    from_version,
                    to_version,
                    best_bid.0,
                    best_bid.1,
                    best_ask.0,
                    best_ask.1
                );

                if applied_live_updates >= 5 {
                    break;
                }
            }
            SpotBookApplyOutcome::IgnoredStale {
                current_version,
                to_version,
            } => {
                println!(
                    "ignored_stale current_version={} to_version={}",
                    current_version, to_version
                );
            }
            SpotBookApplyOutcome::NeedsResync {
                current_version,
                from_version,
                to_version,
            } => {
                println!(
                    "live_resync current_version={} from_version={} to_version={}",
                    current_version, from_version, to_version
                );
                bootstrap = MexcSpotOrderBookBootstrap::new(symbol);
                bootstrap.push_envelope(&event)?;
                initialized_book = None;
            }
        }
    }

    ensure!(
        initialized_book.is_some(),
        "did not bootstrap local spot order book within deadline"
    );
    ensure!(
        applied_live_updates > 0,
        "did not apply live spot depth updates after bootstrap"
    );

    Ok(())
}
