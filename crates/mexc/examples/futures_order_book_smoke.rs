use anyhow::{Context, Result, anyhow, ensure};
use futures::StreamExt;
use mexc::{
    FuturesBookApplyOutcome, FuturesBookBootstrapOutcome, FuturesBookRecoveryOutcome,
    MexcConnector, MexcFuturesOrderBookBootstrap, MexcFuturesSubscription, MexcFuturesWsMessage,
};
use tokio::time::{Duration, Instant, timeout};

#[tokio::main]
async fn main() -> Result<()> {
    let symbol = "BTC_USDT";
    let connector = MexcConnector::default();
    let mut stream = connector
        .futures_ws
        .connect(vec![MexcFuturesSubscription::Depth {
            symbol: symbol.to_string(),
            compress: Some(false),
        }])
        .await
        .context("connect futures depth websocket")?;

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut bootstrap = MexcFuturesOrderBookBootstrap::new(symbol);
    let mut initialized_book = None;
    let mut ever_initialized = false;
    let mut live_progress_steps = 0usize;

    while Instant::now() < deadline {
        let message = timeout(Duration::from_secs(10), stream.next())
            .await
            .context("wait for futures depth event")?
            .transpose()?
            .context("futures websocket ended unexpectedly")?;

        let MexcFuturesWsMessage::Depth(event) = message else {
            continue;
        };
        if !bootstrap.push_envelope(&event) {
            continue;
        }

        if initialized_book.is_none() {
            let snapshot = connector
                .rest
                .futures_depth(symbol)
                .await
                .context("fetch futures depth snapshot")?;
            let commits = connector
                .rest
                .futures_depth_commits(symbol, 1000)
                .await
                .context("fetch futures depth commits")?;

            match bootstrap.initialize_from_snapshot(&snapshot, &commits)? {
                FuturesBookBootstrapOutcome::NeedsRecovery {
                    current_version,
                    next_available_version,
                } => {
                    println!(
                        "bootstrap_retry snapshot_version={} next_available_version={} cached_updates={} commits={}",
                        current_version,
                        next_available_version,
                        bootstrap.cached_len(),
                        commits.len()
                    );
                    continue;
                }
                FuturesBookBootstrapOutcome::Ready(book) => {
                    let best_bid = book
                        .best_bid()
                        .context("best bid missing after bootstrap")?;
                    let best_ask = book
                        .best_ask()
                        .context("best ask missing after bootstrap")?;
                    println!(
                        "initialized version={} cached_updates={} bid={}/{}/{} ask={}/{}/{}",
                        book.version,
                        bootstrap.cached_len(),
                        best_bid.price,
                        best_bid.order_count,
                        best_bid.quantity,
                        best_ask.price,
                        best_ask.order_count,
                        best_ask.quantity
                    );
                    ever_initialized = true;
                    initialized_book = Some(book);
                    continue;
                }
            }
        }

        let book = initialized_book
            .as_mut()
            .ok_or_else(|| anyhow!("book should be initialized before live apply"))?;
        match book.apply_update(&event.data)? {
            FuturesBookApplyOutcome::Applied { version } => {
                live_progress_steps += 1;
                let best_bid = book.best_bid().context("best bid missing after update")?;
                let best_ask = book.best_ask().context("best ask missing after update")?;
                println!(
                    "live_update#{} version={} bid={}/{}/{} ask={}/{}/{}",
                    live_progress_steps,
                    version,
                    best_bid.price,
                    best_bid.order_count,
                    best_bid.quantity,
                    best_ask.price,
                    best_ask.order_count,
                    best_ask.quantity
                );
                if live_progress_steps >= 5 {
                    break;
                }
            }
            FuturesBookApplyOutcome::IgnoredStale {
                current_version,
                update_version,
            } => {
                println!(
                    "ignored_stale current_version={} update_version={}",
                    current_version, update_version
                );
            }
            FuturesBookApplyOutcome::NeedsRecovery {
                current_version,
                update_version,
            } => {
                let mut recovery_updates = connector
                    .rest
                    .futures_depth_commits(symbol, 1000)
                    .await
                    .context("fetch futures depth commits for live recovery")?;
                recovery_updates.push(event.data.clone());

                match book.recover_from_updates(&recovery_updates)? {
                    FuturesBookRecoveryOutcome::Recovered {
                        from_version,
                        to_version,
                    } => {
                        live_progress_steps += 1;
                        let best_bid =
                            book.best_bid().context("best bid missing after recovery")?;
                        let best_ask =
                            book.best_ask().context("best ask missing after recovery")?;
                        println!(
                            "recovered {}->{} after gap {}->{} bid={}/{}/{} ask={}/{}/{}",
                            from_version,
                            to_version,
                            current_version,
                            update_version,
                            best_bid.price,
                            best_bid.order_count,
                            best_bid.quantity,
                            best_ask.price,
                            best_ask.order_count,
                            best_ask.quantity
                        );
                        if live_progress_steps >= 5 {
                            break;
                        }
                    }
                    FuturesBookRecoveryOutcome::NoNewUpdates { current_version } => {
                        println!("recovery_noop current_version={current_version}");
                    }
                    FuturesBookRecoveryOutcome::NeedsRecovery {
                        current_version,
                        next_available_version,
                    } => {
                        println!(
                            "rebootstrap_after_failed_recovery current_version={} next_available_version={}",
                            current_version, next_available_version
                        );
                        bootstrap = MexcFuturesOrderBookBootstrap::new(symbol);
                        bootstrap.push_update(&event.data);
                        initialized_book = None;
                    }
                }
            }
        }
    }

    ensure!(
        ever_initialized,
        "did not bootstrap local futures order book within deadline"
    );
    ensure!(
        live_progress_steps > 0,
        "did not advance local futures order book after bootstrap"
    );

    Ok(())
}
