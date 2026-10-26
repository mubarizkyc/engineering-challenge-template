use crate::side::Side;
use crate::util::dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain};
use crate::util::{AlignedSbeReader, OrderbookSchema};
use chrono::{DateTime, Utc};
use polyfill_rs::book::OrderBook;
use polyfill_rs::types::{BookUpdate, OrderDelta, OrderSummary, Side as PolyfillSide};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

const BOOK_DEPTH: usize = 200;
const SCALE: i64 = 10_000;
/// How many books to show in the stderr summary.
const SAMPLE_SIZE: usize = 5;

#[derive(Debug, Default)]
pub struct DumpSummary {
    pub snapshots: u64,
    pub updates: u64,
    pub symbols: usize,
    pub updates_without_snapshot: u64,
    pub crossed_after_update: u64,
    pub apply_errors: u64,
    pub books: HashMap<String, OrderBook>,
    pub first_ts: Option<DateTime<Utc>>,
    pub last_ts: Option<DateTime<Utc>>,
}

impl DumpSummary {
    pub fn is_healthy(&self) -> bool {
        self.updates_without_snapshot == 0
            && self.crossed_after_update == 0
            && self.apply_errors == 0
    }
}

impl std::fmt::Display for DumpSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "messages: {} ({} snapshots + {} updates)",
            self.snapshots + self.updates,
            self.snapshots,
            self.updates,
        )?;
        writeln!(f, "symbols:  {}", self.symbols)?;
        if let (Some(min), Some(max)) = (self.first_ts, self.last_ts) {
            let secs = max.signed_duration_since(min).num_milliseconds() as f64 / 1000.0;
            writeln!(
                f,
                "time:     {} → {} ({:.1}s)",
                min.to_rfc3339(),
                max.to_rfc3339(),
                secs,
            )?;
        }
        writeln!(
            f,
            "health:   {}",
            if self.is_healthy() { "OK" } else { "FAIL" }
        )?;
        if !self.is_healthy() {
            writeln!(
                f,
                "  updates_without_snapshot: {}",
                self.updates_without_snapshot
            )?;
            writeln!(
                f,
                "  crossed_after_update:     {}",
                self.crossed_after_update
            )?;
            writeln!(f, "  apply_errors:             {}", self.apply_errors)?;
        }

        let sample = SAMPLE_SIZE.min(self.books.len());
        if sample > 0 {
            writeln!(f)?;
            writeln!(f, "sample ({} of {}):", sample, self.books.len())?;
            for (symbol, book) in self.books.iter().take(sample) {
                let bb = book
                    .best_bid()
                    .map(|l| format!("{}@{}", l.size, l.price))
                    .unwrap_or_else(|| "-".into());
                let ba = book
                    .best_ask()
                    .map(|l| format!("{}@{}", l.size, l.price))
                    .unwrap_or_else(|| "-".into());
                writeln!(
                    f,
                    "  {:<16}  bids={:>3} asks={:>3}  bb={bb}  ba={ba}",
                    &symbol[..symbol.len().min(16)],
                    book.bids(None).len(),
                    book.asks(None).len(),
                )?;
            }
        }
        Ok(())
    }
}

pub fn dump(path: &Path, quiet: bool, out: &mut impl Write) -> anyhow::Result<DumpSummary> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut s = DumpSummary::default();
    let mut sequence: u64 = 0;

    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(dec) => {
                let snap = SnapshotDomain::try_from_decoder(dec)?;
                s.snapshots += 1;
                observe_ts(&mut s, snap.exchange_timestamp);

                if !quiet {
                    writeln!(
                        out,
                        "SNAPSHOT {} recv={} exch={} bids=[{}] asks=[{}]",
                        snap.symbol,
                        snap.received_at.to_rfc3339(),
                        snap.exchange_timestamp.to_rfc3339(),
                        fmt_levels(snap.bids.iter().map(|l| (l.price_level, l.quantity))),
                        fmt_levels(snap.asks.iter().map(|l| (l.price_level, l.quantity))),
                    )?;
                }

                let update = BookUpdate {
                    asset_id: snap.symbol.clone(),
                    market: String::new(),
                    timestamp: snap.exchange_timestamp.timestamp_millis().max(0) as u64,
                    bids: snap
                        .bids
                        .iter()
                        .map(|l| OrderSummary {
                            price: ticks_to_decimal(l.price_level),
                            size: ticks_to_decimal(l.quantity),
                        })
                        .collect(),
                    asks: snap
                        .asks
                        .iter()
                        .map(|l| OrderSummary {
                            price: ticks_to_decimal(l.price_level),
                            size: ticks_to_decimal(l.quantity),
                        })
                        .collect(),
                    hash: None,
                };

                let book = s
                    .books
                    .entry(snap.symbol.clone())
                    .or_insert_with(|| OrderBook::new(snap.symbol.clone(), BOOK_DEPTH));
                if book.apply_book_update(&update).is_err() {
                    s.apply_errors += 1;
                }
            }
            AnyMessage::PriceLevelUpdate(dec) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(dec)?;
                s.updates += 1;
                observe_ts(&mut s, u.exchange_timestamp);

                if !quiet {
                    writeln!(
                        out,
                        "UPDATE {} recv={} exch={} {:?} {}@{}",
                        u.symbol,
                        u.backtest_timestamp.to_rfc3339(),
                        u.exchange_timestamp.to_rfc3339(),
                        u.side,
                        u.quantity,
                        u.price_level,
                    )?;
                }

                let Some(book) = s.books.get_mut(&u.symbol) else {
                    s.updates_without_snapshot += 1;
                    continue;
                };

                sequence = sequence.wrapping_add(1);
                let delta = OrderDelta {
                    token_id: u.symbol.clone(),
                    timestamp: u.exchange_timestamp,
                    side: match u.side {
                        Side::Buy => PolyfillSide::BUY,
                        Side::Sell => PolyfillSide::SELL,
                    },
                    price: ticks_to_decimal(u.price_level),
                    size: ticks_to_decimal(u.quantity),
                    sequence,
                };

                if book.apply_delta(delta).is_err() {
                    s.apply_errors += 1;
                } else if !book.is_valid() {
                    s.crossed_after_update += 1;
                }
            }
            _ => {}
        }
    }
    s.symbols = s.books.len();
    Ok(s)
}

fn observe_ts(s: &mut DumpSummary, ts: DateTime<Utc>) {
    if s.first_ts.is_none_or(|m| ts < m) {
        s.first_ts = Some(ts);
    }
    if s.last_ts.is_none_or(|m| ts > m) {
        s.last_ts = Some(ts);
    }
}

fn ticks_to_decimal(ticks: i64) -> Decimal {
    Decimal::from(ticks) / Decimal::from(SCALE)
}

fn fmt_levels(levels: impl Iterator<Item = (i64, i64)>) -> String {
    levels
        .map(|(p, q)| format!("{q}@{p}"))
        .collect::<Vec<_>>()
        .join(",")
}
