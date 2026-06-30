use crate::spot_proto::PublicAggreDepthsV3Api;
use crate::{MexcSpotEnvelope, SpotOrderBook};
use anyhow::{Context, Result, anyhow};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MexcSpotOrderBook {
    pub last_update_id: u64,
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
}

impl MexcSpotOrderBook {
    pub fn from_snapshot(snapshot: &SpotOrderBook) -> Result<Self> {
        let mut book = Self {
            last_update_id: snapshot.last_update_id,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };

        for (price, quantity) in &snapshot.bids {
            apply_level_map(&mut book.bids, price, quantity)?;
        }
        for (price, quantity) in &snapshot.asks {
            apply_level_map(&mut book.asks, price, quantity)?;
        }

        Ok(book)
    }

    pub fn apply_update(
        &mut self,
        update: &PublicAggreDepthsV3Api,
    ) -> Result<SpotBookApplyOutcome> {
        self.apply_update_internal(update, false)
    }

    pub fn apply_snapshot_catchup(
        &mut self,
        update: &PublicAggreDepthsV3Api,
    ) -> Result<SpotBookApplyOutcome> {
        self.apply_update_internal(update, true)
    }

    fn apply_update_internal(
        &mut self,
        update: &PublicAggreDepthsV3Api,
        allow_overlap: bool,
    ) -> Result<SpotBookApplyOutcome> {
        let from_version = parse_u64(&update.from_version)
            .with_context(|| format!("parse fromVersion {}", update.from_version))?;
        let to_version = parse_u64(&update.to_version)
            .with_context(|| format!("parse toVersion {}", update.to_version))?;

        if to_version <= self.last_update_id {
            return Ok(SpotBookApplyOutcome::IgnoredStale {
                current_version: self.last_update_id,
                to_version,
            });
        }

        let expected_from_version = self.last_update_id + 1;
        let has_gap = if allow_overlap {
            from_version > expected_from_version
        } else {
            from_version != expected_from_version
        };

        if has_gap {
            return Ok(SpotBookApplyOutcome::NeedsResync {
                current_version: self.last_update_id,
                from_version,
                to_version,
            });
        }

        for level in &update.bids {
            apply_level_map(&mut self.bids, &level.price, &level.quantity)?;
        }
        for level in &update.asks {
            apply_level_map(&mut self.asks, &level.price, &level.quantity)?;
        }
        self.last_update_id = to_version;

        Ok(SpotBookApplyOutcome::Applied {
            from_version,
            to_version,
        })
    }

    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, qty)| (*price, *qty))
    }

    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(price, qty)| (*price, *qty))
    }
}

#[derive(Debug, Clone, Default)]
pub struct MexcSpotOrderBookBootstrap {
    symbol: String,
    first_from_version: Option<u64>,
    buffered_updates: Vec<PublicAggreDepthsV3Api>,
}

impl MexcSpotOrderBookBootstrap {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            first_from_version: None,
            buffered_updates: Vec::new(),
        }
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn first_from_version(&self) -> Option<u64> {
        self.first_from_version
    }

    pub fn buffered_len(&self) -> usize {
        self.buffered_updates.len()
    }

    pub fn push_envelope(
        &mut self,
        envelope: &MexcSpotEnvelope<PublicAggreDepthsV3Api>,
    ) -> Result<bool> {
        let Some(symbol) = envelope.symbol.as_deref() else {
            return Ok(false);
        };
        if symbol != self.symbol {
            return Ok(false);
        }

        self.push_update(&envelope.data)?;
        Ok(true)
    }

    pub fn push_update(&mut self, update: &PublicAggreDepthsV3Api) -> Result<()> {
        let from_version = parse_u64(&update.from_version)
            .with_context(|| format!("parse fromVersion {}", update.from_version))?;
        if self.first_from_version.is_none() {
            self.first_from_version = Some(from_version);
        }
        self.buffered_updates.push(update.clone());
        Ok(())
    }

    pub fn initialize_from_snapshot(
        &self,
        snapshot: &SpotOrderBook,
    ) -> Result<SpotBookBootstrapOutcome> {
        if let Some(first_from_version) = self.first_from_version
            && snapshot.last_update_id < first_from_version
        {
            return Ok(SpotBookBootstrapOutcome::SnapshotTooOld {
                last_update_id: snapshot.last_update_id,
                first_from_version,
            });
        }

        let mut book = MexcSpotOrderBook::from_snapshot(snapshot)?;

        for update in &self.buffered_updates {
            let to_version = parse_u64(&update.to_version)
                .with_context(|| format!("parse toVersion {}", update.to_version))?;
            if to_version <= snapshot.last_update_id {
                continue;
            }

            let outcome = book.apply_snapshot_catchup(update)?;
            if let SpotBookApplyOutcome::NeedsResync {
                current_version,
                from_version,
                to_version,
            } = outcome
            {
                return Ok(SpotBookBootstrapOutcome::NeedsResync {
                    current_version,
                    from_version,
                    to_version,
                });
            }
        }

        Ok(SpotBookBootstrapOutcome::Ready(book))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpotBookApplyOutcome {
    Applied {
        from_version: u64,
        to_version: u64,
    },
    IgnoredStale {
        current_version: u64,
        to_version: u64,
    },
    NeedsResync {
        current_version: u64,
        from_version: u64,
        to_version: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpotBookBootstrapOutcome {
    SnapshotTooOld {
        last_update_id: u64,
        first_from_version: u64,
    },
    NeedsResync {
        current_version: u64,
        from_version: u64,
        to_version: u64,
    },
    Ready(MexcSpotOrderBook),
}

fn parse_decimal(raw: &str) -> Result<Decimal> {
    Decimal::from_str(raw).with_context(|| format!("parse decimal {raw}"))
}

fn parse_u64(raw: &str) -> Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("parse integer {raw}"))
}

fn apply_level_map(
    levels: &mut BTreeMap<Decimal, Decimal>,
    price: &str,
    quantity: &str,
) -> Result<()> {
    let price = parse_decimal(price)?;
    let quantity = parse_decimal(quantity)?;

    if quantity.is_sign_negative() {
        return Err(anyhow!("negative quantity at price {price}"));
    }

    if quantity.is_zero() {
        levels.remove(&price);
    } else {
        levels.insert(price, quantity);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spot_proto::{PublicAggreDepthV3ApiItem, PublicAggreDepthsV3Api};

    #[test]
    fn snapshot_builds_book() {
        let snapshot = SpotOrderBook {
            last_update_id: 10,
            bids: vec![("100".to_string(), "2".to_string())],
            asks: vec![("101".to_string(), "3".to_string())],
            timestamp: None,
        };
        let book = MexcSpotOrderBook::from_snapshot(&snapshot).expect("build book");
        assert_eq!(book.best_bid().expect("bid").0.to_string(), "100");
        assert_eq!(book.best_ask().expect("ask").0.to_string(), "101");
    }

    #[test]
    fn incremental_update_applies_and_removes_zero_levels() {
        let snapshot = SpotOrderBook {
            last_update_id: 10,
            bids: vec![("100".to_string(), "2".to_string())],
            asks: vec![("101".to_string(), "3".to_string())],
            timestamp: None,
        };
        let mut book = MexcSpotOrderBook::from_snapshot(&snapshot).expect("build book");
        let update = PublicAggreDepthsV3Api {
            asks: vec![PublicAggreDepthV3ApiItem {
                price: "101".to_string(),
                quantity: "0".to_string(),
            }],
            bids: vec![PublicAggreDepthV3ApiItem {
                price: "100.5".to_string(),
                quantity: "1".to_string(),
            }],
            event_type: "spot@public.aggre.depth.v3.api.pb@100ms".to_string(),
            from_version: "11".to_string(),
            to_version: "11".to_string(),
            last_order_create_time: 0,
        };

        let outcome = book.apply_update(&update).expect("apply update");
        assert!(matches!(outcome, SpotBookApplyOutcome::Applied { .. }));
        assert_eq!(book.best_bid().expect("bid").0.to_string(), "100.5");
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn gap_requires_resync() {
        let snapshot = SpotOrderBook {
            last_update_id: 10,
            bids: vec![],
            asks: vec![],
            timestamp: None,
        };
        let mut book = MexcSpotOrderBook::from_snapshot(&snapshot).expect("build book");
        let update = PublicAggreDepthsV3Api {
            asks: vec![],
            bids: vec![],
            event_type: String::new(),
            from_version: "15".to_string(),
            to_version: "15".to_string(),
            last_order_create_time: 0,
        };

        let outcome = book.apply_update(&update).expect("apply update");
        assert!(matches!(outcome, SpotBookApplyOutcome::NeedsResync { .. }));
    }

    #[test]
    fn strict_live_update_rejects_overlapping_sequence() {
        let snapshot = SpotOrderBook {
            last_update_id: 10,
            bids: vec![],
            asks: vec![],
            timestamp: None,
        };
        let mut book = MexcSpotOrderBook::from_snapshot(&snapshot).expect("build book");
        let update = PublicAggreDepthsV3Api {
            asks: vec![],
            bids: vec![],
            event_type: String::new(),
            from_version: "10".to_string(),
            to_version: "11".to_string(),
            last_order_create_time: 0,
        };

        let outcome = book.apply_update(&update).expect("apply update");
        assert!(matches!(outcome, SpotBookApplyOutcome::NeedsResync { .. }));
    }

    #[test]
    fn snapshot_catchup_allows_first_overlapping_update() {
        let snapshot = SpotOrderBook {
            last_update_id: 10,
            bids: vec![],
            asks: vec![],
            timestamp: None,
        };
        let mut book = MexcSpotOrderBook::from_snapshot(&snapshot).expect("build book");
        let update = PublicAggreDepthsV3Api {
            asks: vec![],
            bids: vec![],
            event_type: String::new(),
            from_version: "10".to_string(),
            to_version: "11".to_string(),
            last_order_create_time: 0,
        };

        let outcome = book
            .apply_snapshot_catchup(&update)
            .expect("apply catchup update");
        assert!(matches!(outcome, SpotBookApplyOutcome::Applied { .. }));
        assert_eq!(book.last_update_id, 11);
    }

    #[test]
    fn bootstrap_detects_stale_snapshot() {
        let snapshot = SpotOrderBook {
            last_update_id: 9,
            bids: vec![],
            asks: vec![],
            timestamp: None,
        };
        let update = PublicAggreDepthsV3Api {
            asks: vec![],
            bids: vec![],
            event_type: String::new(),
            from_version: "10".to_string(),
            to_version: "10".to_string(),
            last_order_create_time: 0,
        };
        let mut bootstrap = MexcSpotOrderBookBootstrap::new("BTCUSDT");
        bootstrap.push_update(&update).expect("buffer update");

        let outcome = bootstrap
            .initialize_from_snapshot(&snapshot)
            .expect("bootstrap from snapshot");
        assert!(matches!(
            outcome,
            SpotBookBootstrapOutcome::SnapshotTooOld {
                last_update_id: 9,
                first_from_version: 10
            }
        ));
    }

    #[test]
    fn bootstrap_applies_buffered_updates() {
        let snapshot = SpotOrderBook {
            last_update_id: 10,
            bids: vec![("100".to_string(), "2".to_string())],
            asks: vec![("101".to_string(), "3".to_string())],
            timestamp: None,
        };
        let first = PublicAggreDepthsV3Api {
            asks: vec![],
            bids: vec![PublicAggreDepthV3ApiItem {
                price: "100".to_string(),
                quantity: "5".to_string(),
            }],
            event_type: String::new(),
            from_version: "10".to_string(),
            to_version: "11".to_string(),
            last_order_create_time: 0,
        };
        let second = PublicAggreDepthsV3Api {
            asks: vec![],
            bids: vec![PublicAggreDepthV3ApiItem {
                price: "100.5".to_string(),
                quantity: "1".to_string(),
            }],
            event_type: String::new(),
            from_version: "12".to_string(),
            to_version: "12".to_string(),
            last_order_create_time: 0,
        };
        let mut bootstrap = MexcSpotOrderBookBootstrap::new("BTCUSDT");
        bootstrap.push_update(&first).expect("buffer first update");
        bootstrap
            .push_update(&second)
            .expect("buffer second update");

        let outcome = bootstrap
            .initialize_from_snapshot(&snapshot)
            .expect("bootstrap from snapshot");
        let SpotBookBootstrapOutcome::Ready(book) = outcome else {
            panic!("expected ready bootstrap");
        };
        assert_eq!(book.last_update_id, 12);
        assert_eq!(book.best_bid().expect("bid").0.to_string(), "100.5");
    }
}
