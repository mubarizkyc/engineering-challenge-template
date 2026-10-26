//! Polymarket CLOB market channel: discovery, decoding and book validation.

use std::collections::HashSet;
use std::sync::Arc;

use crate::side::Side;
use anyhow::Context;
use chrono::{DateTime, Utc};
use polyfill_rs::types::{decimal_to_price_exact, decimal_to_qty};
use polyfill_rs::{OrderBookManager, OrderDelta};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::EventsRequest;
use rust_decimal::Decimal;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tracing::{debug, warn};

pub const DEFAULT_REST_URL: &str = "https://gamma-api.polymarket.com";
pub const DEFAULT_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

const PAGE_SIZE: i32 = 1000;
const MAX_MARKETS: usize = 1_000_000;
const BOOK_DEPTH: usize = 200;

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Snapshot {
        symbol: String,
        exchange_ts: Option<DateTime<Utc>>,
    },
    Delta {
        symbol: String,
        side: Side,
        best_bid: Option<i64>,
        best_ask: Option<i64>,
        price: i64,
        quantity: i64,
        exchange_ts: Option<DateTime<Utc>>,
    },
    Desync {
        symbol: String,
        reason: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Discovered {
    pub symbols: Vec<String>,
    pub complete: bool,
}

pub struct Polymarket {
    gamma: GammaClient,
    ws_url: String,
}

impl Polymarket {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            gamma: GammaClient::default(),
            ws_url: DEFAULT_WS_URL.to_string(),
        })
    }

    pub fn connect_request(&self) -> anyhow::Result<Request> {
        Ok(self.ws_url.as_str().into_client_request()?)
    }

    pub fn decoder(&self) -> PolymarketDecoder {
        PolymarketDecoder::new(Arc::new(OrderBookManager::new(BOOK_DEPTH)))
    }

    pub async fn discover(&self) -> anyhow::Result<Discovered> {
        let mut tokens = HashSet::new();
        let mut offset = 0usize;
        let mut complete = true;

        loop {
            let page = match self.fetch_events_page(offset).await {
                Ok(p) => p,
                // Gamma caps offset pagination; this is the end of what it can return.
                Err(e) if offset > 0 && is_offset_cap(&e) => break,
                Err(e) if offset > 0 => {
                    warn!(error = %format!("{e:#}"), offset, "polymarket listing incomplete");
                    complete = false;
                    break;
                }
                Err(e) => return Err(e).context("fetching polymarket events"),
            };

            if page.is_empty() {
                break;
            }
            let got = page.len();
            tokens.extend(page);
            offset += got;
            if offset > MAX_MARKETS {
                break;
            }
        }

        let mut symbols: Vec<_> = tokens.into_iter().collect();
        symbols.sort_unstable();
        Ok(Discovered { symbols, complete })
    }

    async fn fetch_events_page(&self, offset: usize) -> anyhow::Result<Vec<String>> {
        let request = EventsRequest::builder()
            .active(true)
            .closed(false)
            .limit(PAGE_SIZE)
            .offset(offset as i32)
            .build();
        let events = self.gamma.events(&request).await?;

        let mut asset_ids = Vec::new();
        for event in events {
            let Some(markets) = event.markets else {
                continue;
            };
            for market in markets {
                let live = market.enable_order_book == Some(true)
                    && market.accepting_orders == Some(true)
                    && market.closed == Some(false)
                    && market.active != Some(false);
                if !live {
                    continue;
                }
                //remoev non MVEs
                // Skip ghost markets: no orders and no trades means no WS data.
                let has_activity = market.liquidity.is_some_and(|l| l > Decimal::ZERO)
                    || market.volume.is_some_and(|v| v > Decimal::ZERO);
                if !has_activity {
                    continue;
                }
                if let Some(ids) = market.clob_token_ids {
                    asset_ids.extend(ids.into_iter().map(|id| id.to_string()));
                }
            }
        }

        Ok(asset_ids)
    }
}

pub struct PolymarketDecoder {
    books: Arc<OrderBookManager>,
    synced: HashSet<String>,
    sequence: u64,
}

impl PolymarketDecoder {
    pub fn new(books: Arc<OrderBookManager>) -> Self {
        Self {
            books,
            synced: HashSet::new(),
            sequence: 0,
        }
    }

    pub fn books(&self) -> Arc<OrderBookManager> {
        Arc::clone(&self.books)
    }

    pub fn decode(&mut self, text: &str, out: &mut Vec<Event>) -> anyhow::Result<()> {
        for msg in polyfill_rs::decode::parse_stream_messages(text).unwrap_or_default() {
            self.handle(msg, out);
        }
        Ok(())
    }

    fn handle(&mut self, message: polyfill_rs::types::StreamMessage, out: &mut Vec<Event>) {
        match message {
            polyfill_rs::types::StreamMessage::Book(book) => self.handle_book(book, out),
            polyfill_rs::types::StreamMessage::PriceChange(pc) => self.handle_price_change(pc, out),
            //we dont discover markets through Ws
            _ => {}
        }
    }

    fn handle_book(&mut self, book: polyfill_rs::types::BookUpdate, out: &mut Vec<Event>) {
        let symbol = book.asset_id.clone();
        let exchange_ts = DateTime::from_timestamp_millis(book.timestamp as i64);
        let snapshot_ts = book.timestamp;

        // Reset stale-check guards so a reconnect can reload the same book.
        if !self.synced.contains(&symbol) {
            let _ = self.books.with_book_mut(&symbol, |b| {
                b.last_snapshot_timestamp_ms = 0;
                b.last_delta_sequence = 0;
                b.sequence = 0;
                Ok(())
            });
        }

        if let Err(e) = self.books.apply_book_update(&book) {
            self.synced.remove(&symbol);
            out.push(Event::Desync {
                symbol,
                reason: format!("apply_book_update rejected: {e}"),
            });
            return;
        }

        let (loaded_ts, crossed) = self
            .books
            .with_book_mut(&symbol, |b| {
                Ok((b.last_snapshot_timestamp_ms, is_crossed(b)))
            })
            .unwrap_or((0, false));

        if loaded_ts == 0 {
            self.synced.remove(&symbol);
            out.push(Event::Desync {
                symbol,
                reason: format!("snapshot ts {snapshot_ts} did not populate book"),
            });
            return;
        }
        if crossed {
            self.synced.remove(&symbol);
            out.push(Event::Desync {
                symbol,
                reason: "snapshot produced crossed book".into(),
            });
            return;
        }

        self.synced.insert(symbol.clone());
        out.push(Event::Snapshot {
            symbol,
            exchange_ts,
        });
    }

    fn handle_price_change(
        &mut self,
        pc_msg: polyfill_rs::types::PriceChange,
        out: &mut Vec<Event>,
    ) {
        let exchange_ts = DateTime::from_timestamp_millis(pc_msg.timestamp as i64);

        let mut by_asset: Vec<(String, Vec<&polyfill_rs::types::PriceChangeEntry>)> = Vec::new();
        for entry in &pc_msg.price_changes {
            if let Some((_, group)) = by_asset.iter_mut().find(|(id, _)| id == &entry.asset_id) {
                group.push(entry);
            } else {
                by_asset.push((entry.asset_id.clone(), vec![entry]));
            }
        }

        for (asset, entries) in by_asset {
            if !self.synced.contains(&asset) {
                continue;
            }

            let snapshot_ms = self
                .books
                .with_book_mut(&asset, |b| Ok(b.last_snapshot_timestamp_ms))
                .unwrap_or(0);
            if snapshot_ms > 0 && pc_msg.timestamp < snapshot_ms {
                continue;
            }

            let last = entries.last().expect("grouped entries are non-empty");
            let their_bid = last
                .best_bid
                .and_then(|d| decimal_to_price_exact(d).ok())
                .map(i64::from);
            let their_ask = last
                .best_ask
                .and_then(|d| decimal_to_price_exact(d).ok())
                .map(i64::from);

            // One lock per asset: apply batch, then read validity and TOB.
            let outcome = self.books.with_book_mut(&asset, |b| {
                for entry in &entries {
                    self.sequence = self.sequence.wrapping_add(1);
                    let delta = OrderDelta {
                        token_id: asset.clone(),
                        timestamp: exchange_ts.unwrap_or_else(Utc::now),
                        side: entry.side,
                        price: entry.price,
                        size: entry.size.unwrap_or(Decimal::ZERO),
                        sequence: self.sequence,
                    };
                    b.apply_delta(delta)?;
                }
                Ok((
                    is_crossed(b),
                    (
                        b.best_bid_fast().map(|l| l.price as i64),
                        b.best_ask_fast().map(|l| l.price as i64),
                    ),
                ))
            });

            let (crossed, (our_bid, our_ask)) = match outcome {
                Ok(v) => v,
                Err(e) => {
                    self.synced.remove(&asset);
                    out.push(Event::Desync {
                        symbol: asset,
                        reason: format!("apply_delta rejected: {e}"),
                    });
                    continue;
                }
            };

            if crossed {
                self.synced.remove(&asset);
                out.push(Event::Desync {
                    symbol: asset,
                    reason: "crossed book after delta".into(),
                });
                continue;
            }

            // Informational: exchange TOB may reflect a later moment than ours.
            if (their_bid.is_some() || their_ask.is_some())
                && (their_bid != our_bid || their_ask != our_ask)
            {
                debug!(
                    %asset,
                    theirs = ?(their_bid, their_ask),
                    ours = ?(our_bid, our_ask),
                    "exchange TOB differs from applied book (informational)"
                );
            }

            for entry in entries {
                let Ok(price_i64) = decimal_to_price_exact(entry.price) else {
                    continue;
                };
                let size_i64 = entry.size.and_then(|s| decimal_to_qty(s).ok()).unwrap_or(0);
                out.push(Event::Delta {
                    symbol: asset.clone(),
                    side: if entry.side == polyfill_rs::types::Side::BUY {
                        Side::Buy
                    } else {
                        Side::Sell
                    },
                    price: i64::from(price_i64),
                    quantity: size_i64,
                    best_bid: entry
                        .best_bid
                        .and_then(|d| decimal_to_price_exact(d).ok())
                        .map(i64::from),
                    best_ask: entry
                        .best_ask
                        .and_then(|d| decimal_to_price_exact(d).ok())
                        .map(i64::from),
                    exchange_ts,
                });
            }
        }
    }

    pub fn subscribe(&self, symbols: &[String]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        vec![
            serde_json::json!({
                "type": "market",
                "operation": "subscribe",
                "markets": [],
                "assets_ids": symbols,
                "initial_dump": true,
            })
            .to_string(),
        ]
    }

    pub fn unsubscribe(&self, symbols: &[String]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        vec![
            serde_json::json!({
                "type": "market",
                "operation": "unsubscribe",
                "markets": [],
                "assets_ids": symbols,
            })
            .to_string(),
        ]
    }

    pub fn keepalive(&self) -> String {
        "PING".to_string()
    }

    pub fn drain_outbox(&mut self) -> Vec<String> {
        Vec::new()
    }
}

fn is_crossed(book: &polyfill_rs::book::OrderBook) -> bool {
    match (book.best_bid_fast(), book.best_ask_fast()) {
        (Some(bid), Some(ask)) => bid.price > ask.price,
        _ => false,
    }
}
fn is_offset_cap(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains("offset too large")
}
