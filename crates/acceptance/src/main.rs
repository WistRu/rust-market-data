use acceptance::{drift_audit, inventory, print_human_report, report};
use anyhow::{Result, bail};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let json = take_flag(&mut args, "--json");
    let command = args.first().map(String::as_str).unwrap_or("inventory");

    match command {
        "inventory" => {
            let items = inventory();
            if json {
                println!("{}", serde_json::to_string_pretty(&items)?);
            } else {
                for item in items {
                    println!(
                        "{} crate={} status={} endpoint={} reason={}",
                        item.exchange,
                        item.crate_name,
                        item.status,
                        item.ws_endpoint.as_deref().unwrap_or("n/a"),
                        item.reason
                    );
                }
            }
        }
        "report" => {
            let exchange = args.get(1).map(String::as_str).unwrap_or("all");
            let reports = if exchange == "all" {
                let mut reports = Vec::new();
                for item in inventory() {
                    reports.push(report(&item.exchange).await?);
                }
                reports
            } else {
                vec![report(exchange).await?]
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&reports)?);
            } else {
                for report in &reports {
                    print_human_report(report);
                }
            }
        }
        "drift-audit" => {
            let audit = drift_audit().await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&audit)?);
            } else {
                println!("drift_audit reports={}", audit.len());
                for report in &audit {
                    println!(
                        "{} status={} failures={}",
                        report.exchange,
                        report.status,
                        report.has_failures()
                    );
                }
            }
        }
        _ => bail!("unknown acceptance command: {command}"),
    }

    Ok(())
}

fn take_flag(args: &mut Vec<String>, flag: &str) -> bool {
    if let Some(index) = args.iter().position(|arg| arg == flag) {
        args.remove(index);
        true
    } else {
        false
    }
}
