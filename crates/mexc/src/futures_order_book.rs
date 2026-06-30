use crate::{FuturesDepthSnapshot, FuturesWsChannelEnvelope, MexcFuturesEnvelope};
use anyhow::{Context, Result, anyhow};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuturesBookLevel {
    pub price: Decimal,
    pub order_count: u64,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MexcFuturesOrderBook {
    pub version: u64,
    pub cts: Option<u64>,
    pub bids: BTreeMap<Decimal, FuturesBookLevel>,
    pub asks: BTreeMap<Decimal, FuturesBookLevel>,
}

impl MexcFuturesOrderBook {
    pub fn from_snapshot(snapshot: &FuturesDepthSnapshot) -> Result<Self> {
        let mut book = Self {
            version: snapshot.version,
            cts: snapshot.cts,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };

        for level in &snapshot.bids {
            apply_level_map(&mut book.bids, level)?;
        }
        for level in &snapshot.asks {
            apply_level_map(&mut book.asks, level)?;
        }

        Ok(book)
    }

    pub fn apply_update(
        &mut self,
        update: &FuturesDepthSnapshot,
    ) -> Result<FuturesBookApplyOutcome> {
        if update.version <= self.version {
            return Ok(FuturesBookApplyOutcome::IgnoredStale {
                current_version: self.version,
                update_version: update.version,
            });
        }

        if update.version != self.version + 1 {
            return Ok(FuturesBookApplyOutcome::NeedsRecovery {
                current_version: self.version,
                update_version: update.version,
            });
        }

        for level in &update.bids {
            apply_level_map(&mut self.bids, level)?;
        }
        for level in &update.asks {
            apply_level_map(&mut self.asks, level)?;
        }

        self.version = update.version;
        self.cts = update.cts.or(self.cts);

        Ok(FuturesBookApplyOutcome::Applied {
            version: update.version,
        })
    }

    pub fn recover_from_updates(
        &mut self,
        updates: &[FuturesDepthSnapshot],
    ) -> Result<FuturesBookRecoveryOutcome> {
        let mut merged = BTreeMap::new();
        for update in updates {
            if update.version > self.version {
                merged.insert(update.version, update.clone());
            }
        }

        let Some((&first_version, _)) = merged.first_key_value() else {
            return Ok(FuturesBookRecoveryOutcome::NoNewUpdates {
                current_version: self.version,
            });
        };

        if first_version > self.version + 1 {
            return Ok(FuturesBookRecoveryOutcome::NeedsRecovery {
                current_version: self.version,
                next_available_version: first_version,
            });
        }

        let from_version = self.version + 1;
        for update in merged.values() {
            match self.apply_update(update)? {
                FuturesBookApplyOutcome::Applied { .. }
                | FuturesBookApplyOutcome::IgnoredStale { .. } => {}
                FuturesBookApplyOutcome::NeedsRecovery {
                    current_version,
                    update_version,
                } => {
                    return Ok(FuturesBookRecoveryOutcome::NeedsRecovery {
                        current_version,
                        next_available_version: update_version,
                    });
                }
            }
        }

        Ok(FuturesBookRecoveryOutcome::Recovered {
            from_version,
            to_version: self.version,
        })
    }

    pub fn best_bid(&self) -> Option<&FuturesBookLevel> {
        self.bids.values().next_back()
    }

    pub fn best_ask(&self) -> Option<&FuturesBookLevel> {
        self.asks.values().next()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FuturesBookApplyOutcome {
    Applied {
        version: u64,
    },
    IgnoredStale {
        current_version: u64,
        update_version: u64,
    },
    NeedsRecovery {
        current_version: u64,
        update_version: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FuturesBookRecoveryOutcome {
    NoNewUpdates {
        current_version: u64,
    },
    NeedsRecovery {
        current_version: u64,
        next_available_version: u64,
    },
    Recovered {
        from_version: u64,
        to_version: u64,
    },
}

#[derive(Debug, Clone, Default)]
pub struct MexcFuturesOrderBookBootstrap {
    symbol: String,
    cached_updates: BTreeMap<u64, FuturesDepthSnapshot>,
}

impl MexcFuturesOrderBookBootstrap {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            cached_updates: BTreeMap::new(),
        }
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn cached_len(&self) -> usize {
        self.cached_updates.len()
    }

    pub fn push_envelope(&mut self, envelope: &MexcFuturesEnvelope<FuturesDepthSnapshot>) -> bool {
        let Some(symbol) = envelope.symbol.as_deref() else {
            return false;
        };
        if symbol != self.symbol {
            return false;
        }

        self.push_update(&envelope.data);
        true
    }

    pub fn push_channel_envelope(
        &mut self,
        envelope: &FuturesWsChannelEnvelope<FuturesDepthSnapshot>,
    ) -> bool {
        let Some(symbol) = envelope.symbol.as_deref() else {
            return false;
        };
        if symbol != self.symbol {
            return false;
        }

        self.push_update(&envelope.data);
        true
    }

    pub fn push_update(&mut self, update: &FuturesDepthSnapshot) {
        self.cached_updates.insert(update.version, update.clone());
    }

    pub fn initialize_from_snapshot(
        &self,
        snapshot: &FuturesDepthSnapshot,
        commits: &[FuturesDepthSnapshot],
    ) -> Result<FuturesBookBootstrapOutcome> {
        let mut merged_updates = BTreeMap::new();
        for commit in commits {
            if commit.version > snapshot.version {
                merged_updates.insert(commit.version, commit.clone());
            }
        }
        for (version, update) in &self.cached_updates {
            if *version > snapshot.version {
                merged_updates.insert(*version, update.clone());
            }
        }

        let mut book = MexcFuturesOrderBook::from_snapshot(snapshot)?;
        let updates = merged_updates.into_values().collect::<Vec<_>>();
        match book.recover_from_updates(&updates)? {
            FuturesBookRecoveryOutcome::NoNewUpdates { .. }
            | FuturesBookRecoveryOutcome::Recovered { .. } => {
                Ok(FuturesBookBootstrapOutcome::Ready(book))
            }
            FuturesBookRecoveryOutcome::NeedsRecovery {
                current_version,
                next_available_version,
            } => Ok(FuturesBookBootstrapOutcome::NeedsRecovery {
                current_version,
                next_available_version,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FuturesBookBootstrapOutcome {
    NeedsRecovery {
        current_version: u64,
        next_available_version: u64,
    },
    Ready(MexcFuturesOrderBook),
}

fn parse_decimal_value(value: &Value) -> Result<Decimal> {
    match value {
        Value::String(raw) => {
            Decimal::from_str(raw).with_context(|| format!("parse decimal {raw}"))
        }
        Value::Number(raw) => {
            Decimal::from_str(&raw.to_string()).with_context(|| format!("parse decimal {}", raw))
        }
        other => Err(anyhow!("unsupported decimal value {other}")),
    }
}

fn parse_u64_value(value: &Value) -> Result<u64> {
    match value {
        Value::String(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("parse integer {raw}")),
        Value::Number(raw) => raw
            .as_u64()
            .ok_or_else(|| anyhow!("number is not u64: {raw}")),
        other => Err(anyhow!("unsupported integer value {other}")),
    }
}

fn apply_level_map(
    levels: &mut BTreeMap<Decimal, FuturesBookLevel>,
    raw: &(Value, Value, Value),
) -> Result<()> {
    let price = parse_decimal_value(&raw.0)?;
    let order_count = parse_u64_value(&raw.1)?;
    let quantity = parse_decimal_value(&raw.2)?;

    if quantity.is_sign_negative() {
        return Err(anyhow!("negative quantity at price {price}"));
    }

    if quantity.is_zero() {
        levels.remove(&price);
    } else {
        levels.insert(
            price,
            FuturesBookLevel {
                price,
                order_count,
                quantity,
            },
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn level(price: &str, orders: u64, quantity: &str) -> (Value, Value, Value) {
        (json!(price), json!(orders), json!(quantity))
    }

    #[test]
    fn snapshot_builds_book() {
        let snapshot = FuturesDepthSnapshot {
            cts: Some(1),
            asks: vec![level("101", 2, "3")],
            bids: vec![level("100", 1, "2")],
            version: 10,
        };

        let book = MexcFuturesOrderBook::from_snapshot(&snapshot).expect("build book");
        assert_eq!(book.best_bid().expect("bid").price.to_string(), "100");
        assert_eq!(book.best_ask().expect("ask").price.to_string(), "101");
    }

    #[test]
    fn update_applies_and_removes_zero_levels() {
        let snapshot = FuturesDepthSnapshot {
            cts: Some(1),
            asks: vec![level("101", 2, "3")],
            bids: vec![level("100", 1, "2")],
            version: 10,
        };
        let mut book = MexcFuturesOrderBook::from_snapshot(&snapshot).expect("build book");
        let update = FuturesDepthSnapshot {
            cts: Some(2),
            asks: vec![level("101", 0, "0")],
            bids: vec![level("100.5", 4, "7")],
            version: 11,
        };

        let outcome = book.apply_update(&update).expect("apply update");
        assert!(matches!(
            outcome,
            FuturesBookApplyOutcome::Applied { version: 11 }
        ));
        assert_eq!(book.best_bid().expect("bid").price.to_string(), "100.5");
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn gap_requires_recovery() {
        let snapshot = FuturesDepthSnapshot {
            cts: Some(1),
            asks: vec![],
            bids: vec![],
            version: 10,
        };
        let mut book = MexcFuturesOrderBook::from_snapshot(&snapshot).expect("build book");
        let update = FuturesDepthSnapshot {
            cts: Some(2),
            asks: vec![],
            bids: vec![],
            version: 13,
        };

        let outcome = book.apply_update(&update).expect("apply update");
        assert!(matches!(
            outcome,
            FuturesBookApplyOutcome::NeedsRecovery {
                current_version: 10,
                update_version: 13
            }
        ));
    }

    #[test]
    fn bootstrap_merges_commits_and_cached_updates() {
        let snapshot = FuturesDepthSnapshot {
            cts: Some(1),
            asks: vec![level("101", 2, "3")],
            bids: vec![level("100", 1, "2")],
            version: 10,
        };
        let commit_11 = FuturesDepthSnapshot {
            cts: Some(2),
            asks: vec![],
            bids: vec![level("100", 3, "5")],
            version: 11,
        };
        let cached_12 = FuturesDepthSnapshot {
            cts: Some(3),
            asks: vec![level("101.5", 4, "2")],
            bids: vec![],
            version: 12,
        };

        let mut bootstrap = MexcFuturesOrderBookBootstrap::new("BTC_USDT");
        bootstrap.push_update(&cached_12);

        let outcome = bootstrap
            .initialize_from_snapshot(&snapshot, &[commit_11])
            .expect("bootstrap");
        let FuturesBookBootstrapOutcome::Ready(book) = outcome else {
            panic!("expected ready");
        };
        assert_eq!(book.version, 12);
        assert_eq!(book.best_bid().expect("bid").quantity.to_string(), "5");
        assert_eq!(book.best_ask().expect("ask").price.to_string(), "101");
    }

    #[test]
    fn bootstrap_detects_missing_first_version() {
        let snapshot = FuturesDepthSnapshot {
            cts: Some(1),
            asks: vec![],
            bids: vec![],
            version: 10,
        };
        let commit_12 = FuturesDepthSnapshot {
            cts: Some(2),
            asks: vec![],
            bids: vec![],
            version: 12,
        };
        let bootstrap = MexcFuturesOrderBookBootstrap::new("BTC_USDT");

        let outcome = bootstrap
            .initialize_from_snapshot(&snapshot, &[commit_12])
            .expect("bootstrap");
        assert!(matches!(
            outcome,
            FuturesBookBootstrapOutcome::NeedsRecovery {
                current_version: 10,
                next_available_version: 12
            }
        ));
    }

    #[test]
    fn recovery_applies_sequential_commits() {
        let snapshot = FuturesDepthSnapshot {
            cts: Some(1),
            asks: vec![level("101", 2, "3")],
            bids: vec![level("100", 1, "2")],
            version: 10,
        };
        let mut book = MexcFuturesOrderBook::from_snapshot(&snapshot).expect("build book");
        let updates = vec![
            FuturesDepthSnapshot {
                cts: Some(2),
                asks: vec![],
                bids: vec![level("100", 3, "5")],
                version: 11,
            },
            FuturesDepthSnapshot {
                cts: Some(3),
                asks: vec![level("101.5", 4, "2")],
                bids: vec![],
                version: 12,
            },
        ];

        let outcome = book
            .recover_from_updates(&updates)
            .expect("recover from updates");
        assert!(matches!(
            outcome,
            FuturesBookRecoveryOutcome::Recovered {
                from_version: 11,
                to_version: 12
            }
        ));
        assert_eq!(book.version, 12);
    }
}
