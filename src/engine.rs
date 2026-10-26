//! Book building, source selection and subscription health. Exactly one replica
//! per symbol is the source; its deltas are written, and a source switch emits
//! a full snapshot from the new source's book.

use crate::output::Output;
use crate::polymarket::Event;
use crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use crate::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use chrono::{DateTime, Utc};
use crossbeam::channel::Sender;
use polyfill_rs::book::OrderBookManager;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info};

pub type ConnId = usize;

const TICK_MS: u64 = 250;

#[derive(Debug, PartialEq, Eq)]
pub enum ConnCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    /// Unsubscribe and subscribe again to get a fresh snapshot.
    Resubscribe(Vec<String>),
}

pub enum EngineMsg {
    Feed {
        conn: ConnId,
        received_at: DateTime<Utc>,
        events: Vec<Event>,
    },
    ConnUp(ConnId),
    ConnDown(ConnId),
    /// Each connection owns its own `OrderBookManager`; the engine reads from it
    /// when that connection is the source for a symbol.
    Register {
        conn: ConnId,
        commands: mpsc::UnboundedSender<ConnCommand>,
        books: Arc<OrderBookManager>,
    },
    AddSymbols {
        symbols: Vec<String>,
        conns: Vec<ConnId>,
    },
    RemoveSymbols(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub snapshot_interval: Duration,
    pub rotate_interval: Option<Duration>,
    /// Never resubscribe the same symbol on the same connection more often.
    pub resubscribe_backoff: Duration,
    /// Retry a subscription that has not produced a snapshot after this long.
    pub resync_timeout: Duration,
    /// A replica silent this much longer than a sibling is missing data.
    pub replica_silence: Duration,
    /// Resubscribe a symbol silent on every replica for `quiet_factor` × its
    /// usual gap, clamped to `[quiet_min, quiet_max]`.
    pub quiet_factor: f64,
    pub quiet_min: Duration,
    pub quiet_max: Duration,
    /// Switch to a replica whose connection latency is lower by this much.
    pub latency_switch_ms: f64,
    /// Minimum time between source switches for latency reasons.
    pub min_switch_interval: Duration,
    pub stats_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            snapshot_interval: Duration::from_secs(60),
            rotate_interval: None,
            resubscribe_backoff: Duration::from_secs(10),
            resync_timeout: Duration::from_secs(30),
            replica_silence: Duration::from_secs(60),
            quiet_factor: 20.0,
            quiet_min: Duration::from_secs(5 * 60),
            quiet_max: Duration::from_secs(60 * 60),
            latency_switch_ms: 2000.0,
            min_switch_interval: Duration::from_secs(30),
            stats_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct EngineStats {
    pub events: u64,
    pub updates_written: u64,
    pub snapshots_written: u64,
    pub source_switches: u64,
    pub resubscribes: u64,
    pub desyncs: u64,
    pub ignored: u64,
}

struct ConnState {
    up: bool,
    /// EWMA of receive time minus exchange time, in milliseconds.
    latency_ms: Option<f64>,
    commands: Option<mpsc::UnboundedSender<ConnCommand>>,
}

struct Replica {
    conn: ConnId,
    synced: bool,
    last_msg: Option<Instant>,
    /// Last level change (not snapshot) on this replica.
    last_delta: Option<Instant>,
    synced_at: Option<Instant>,
    last_subscribe: Instant,
    resubscribe_pending: bool,
}

struct SymbolState {
    replicas: Vec<Replica>,
    source: Option<usize>,
    last_switch: Option<Instant>,
    last_activity: Option<Instant>,
    mean_gap: Option<f64>,
    last_source_delta: Option<Instant>,
    last_quiet_resubscribe: Option<Instant>,
    added_at: Instant,
    /// Immutable hash of the symbol, used to phase its periodic snapshots.
    snapshot_hash: u64,
}

pub struct Engine {
    books_by_conn: HashMap<ConnId, Arc<OrderBookManager>>,
    config: EngineConfig,
    symbols: HashMap<String, SymbolState>,
    conns: HashMap<ConnId, ConnState>,
    output: Sender<Output>,
    stats: EngineStats,
    last_stats: EngineStats,
    next_snapshot: DateTime<Utc>,
    next_rotation: Option<DateTime<Utc>>,
    next_stats: Instant,
    next_health: Instant,
}

fn next_boundary(now: DateTime<Utc>, interval: Duration) -> DateTime<Utc> {
    let step = interval.as_millis().max(1) as i64;
    let next = (now.timestamp_millis() / step + 1) * step;
    DateTime::from_timestamp_millis(next).unwrap_or(now)
}

fn fnv1a(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ b as u64).wrapping_mul(0x100_0000_01b3)
    })
}

impl Engine {
    pub fn new(
        config: EngineConfig,
        output: Sender<Output>,
        now: Instant,
        wall: DateTime<Utc>,
    ) -> Self {
        Self {
            books_by_conn: HashMap::new(),
            next_snapshot: next_boundary(wall, config.snapshot_interval),
            next_rotation: config.rotate_interval.map(|i| next_boundary(wall, i)),
            next_stats: now + config.stats_interval,
            next_health: now,
            config,
            symbols: HashMap::new(),
            conns: HashMap::new(),
            output,
            stats: EngineStats::default(),
            last_stats: EngineStats::default(),
        }
    }

    pub fn stats(&self) -> &EngineStats {
        &self.stats
    }

    pub async fn run(mut self, mut messages: mpsc::UnboundedReceiver<EngineMsg>) -> EngineStats {
        let mut ticker = tokio::time::interval(Duration::from_millis(TICK_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                message = messages.recv() => match message {
                    Some(message) => self.handle(message, Instant::now()),
                    None => break,
                },
                _ = ticker.tick() => self.tick(Instant::now(), Utc::now(), messages.len()),
            }
        }
        info!(stats = ?self.stats, "engine stopped");
        self.stats
    }

    pub fn handle(&mut self, message: EngineMsg, now: Instant) {
        match message {
            EngineMsg::Feed {
                conn,
                received_at,
                events,
            } => {
                for event in events {
                    self.stats.events += 1;
                    self.handle_event(conn, received_at, event, now);
                }
            }
            EngineMsg::ConnUp(conn) => {
                self.conn(conn).up = true;
                // A fresh connection resubscribes everything it carries.
                for state in self.symbols.values_mut() {
                    for replica in state.replicas.iter_mut().filter(|r| r.conn == conn) {
                        replica.synced = false;
                        replica.last_subscribe = now;
                        replica.resubscribe_pending = false;
                    }
                }
            }
            EngineMsg::ConnDown(conn) => {
                self.conn(conn).up = false;
                // Keep the book Arc: the connection reuses it on reconnect.
                let wall = Utc::now();
                let lost: Vec<String> = self
                    .symbols
                    .iter_mut()
                    .filter_map(|(symbol, state)| {
                        let idx = state.replicas.iter().position(|r| r.conn == conn)?;
                        state.replicas[idx].synced = false;
                        (state.source == Some(idx)).then(|| symbol.clone())
                    })
                    .collect();
                for symbol in lost {
                    self.select_source(&symbol, now, wall);
                }
            }
            EngineMsg::Register {
                conn,
                commands,
                books,
            } => {
                self.books_by_conn.insert(conn, books);
                self.conn(conn).commands = Some(commands);
            }
            EngineMsg::AddSymbols { symbols, conns } => {
                for symbol in symbols {
                    let snapshot_hash = fnv1a(&symbol);
                    self.symbols.entry(symbol).or_insert_with(|| SymbolState {
                        replicas: conns
                            .iter()
                            .map(|&conn| Replica {
                                conn,
                                synced: false,
                                last_msg: None,
                                last_delta: None,
                                synced_at: None,
                                last_subscribe: now,
                                resubscribe_pending: false,
                            })
                            .collect(),
                        source: None,
                        last_switch: None,
                        last_activity: None,
                        mean_gap: None,
                        last_source_delta: None,
                        last_quiet_resubscribe: None,
                        added_at: now,
                        snapshot_hash,
                    });
                }
            }
            EngineMsg::RemoveSymbols(symbols) => {
                let wall = Utc::now();
                for symbol in symbols {
                    // An empty snapshot marks the book closed for downstream readers.
                    if self
                        .symbols
                        .get(&symbol)
                        .is_some_and(|s| s.source.is_some())
                    {
                        self.write_retirement_snapshot(&symbol, wall);
                    }
                    self.symbols.remove(&symbol);
                }
            }
        }
    }

    fn conn(&mut self, conn: ConnId) -> &mut ConnState {
        self.conns.entry(conn).or_insert(ConnState {
            up: false,
            latency_ms: None,
            commands: None,
        })
    }

    fn handle_event(
        &mut self,
        conn: ConnId,
        received_at: DateTime<Utc>,
        event: Event,
        now: Instant,
    ) {
        let symbol = match &event {
            Event::Snapshot { symbol, .. }
            | Event::Delta { symbol, .. }
            | Event::Desync { symbol, .. } => symbol,
        };
        let Some(state) = self.symbols.get_mut(symbol) else {
            self.stats.ignored += 1;
            return;
        };
        let Some(idx) = state.replicas.iter().position(|r| r.conn == conn) else {
            self.stats.ignored += 1;
            return;
        };
        let is_source = state.source == Some(idx);

        match event {
            // The decoder already loaded and validated this snapshot.
            Event::Snapshot { symbol, .. } => {
                let replica = &mut state.replicas[idx];
                replica.synced = true;
                replica.synced_at = Some(now);
                replica.last_msg = Some(now);

                if is_source {
                    self.write_snapshot(&symbol, received_at);
                } else if !state
                    .source
                    .is_some_and(|s| Self::eligible(&self.conns, &state.replicas[s]))
                {
                    self.select_source(&symbol, now, received_at);
                }
            }

            // The decoder already applied the delta and checked crossing / TOB.
            Event::Delta {
                symbol,
                side,
                price,
                quantity,
                exchange_ts,
                ..
            } => {
                let replica = &mut state.replicas[idx];
                if !replica.synced {
                    return;
                }
                replica.last_msg = Some(now);
                replica.last_delta = Some(now);

                if let Some(ts) = exchange_ts {
                    let sample = (received_at - ts).num_microseconds().unwrap_or(0) as f64 / 1000.0;
                    let conn_state = self.conns.entry(conn).or_insert(ConnState {
                        up: true,
                        latency_ms: None,
                        commands: None,
                    });
                    conn_state.latency_ms = Some(match conn_state.latency_ms {
                        Some(mean) => mean + 0.05 * (sample - mean),
                        None => sample,
                    });
                }

                state.last_activity = Some(now);

                if is_source {
                    // One gap sample per frame; deltas in a frame share `now`.
                    if let Some(last) = state.last_source_delta
                        && now > last
                    {
                        let gap = now.saturating_duration_since(last).as_secs_f64();
                        state.mean_gap = Some(match state.mean_gap {
                            Some(mean) => mean + 0.1 * (gap - mean),
                            None => gap,
                        });
                    }
                    state.last_source_delta = Some(now);
                    self.stats.updates_written += 1;
                    let _ = self.output.send(Output::Update(PriceLevelUpdateDomain {
                        backtest_timestamp: received_at,
                        exchange_timestamp: exchange_ts.unwrap_or(received_at),
                        price_level: price,
                        quantity,
                        side,
                        symbol,
                    }));
                } else if state.source.is_none() {
                    self.select_source(&symbol, now, received_at);
                }
            }

            Event::Desync { symbol, reason } => self.desync(&symbol, idx, &reason, now),
        }
    }

    fn desync(&mut self, symbol: &str, idx: usize, reason: &str, now: Instant) {
        self.stats.desyncs += 1;
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        let replica = &mut state.replicas[idx];
        debug!(symbol, conn = replica.conn, reason, "replica desynced");
        replica.synced = false;
        replica.resubscribe_pending = true;
        let was_source = state.source == Some(idx);
        self.resubscribe(symbol, idx, now);
        if was_source {
            self.select_source(symbol, now, Utc::now());
        }
    }

    fn resubscribe(&mut self, symbol: &str, idx: usize, now: Instant) {
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        let replica = &mut state.replicas[idx];
        if now.saturating_duration_since(replica.last_subscribe) < self.config.resubscribe_backoff {
            return;
        }
        let Some(conn) = self.conns.get(&replica.conn) else {
            return;
        };
        if !conn.up {
            return;
        }
        if let Some(commands) = &conn.commands {
            replica.last_subscribe = now;
            replica.resubscribe_pending = false;
            self.stats.resubscribes += 1;
            let _ = commands.send(ConnCommand::Resubscribe(vec![symbol.to_owned()]));
        }
    }

    fn eligible(conns: &HashMap<ConnId, ConnState>, replica: &Replica) -> bool {
        replica.synced && conns.get(&replica.conn).is_some_and(|c| c.up)
    }

    fn latency(conns: &HashMap<ConnId, ConnState>, replica: &Replica) -> f64 {
        conns
            .get(&replica.conn)
            .and_then(|c| c.latency_ms)
            .unwrap_or(f64::MAX / 4.0)
    }

    /// Lowest latency first, then freshest; replicas clearly behind their
    /// siblings are skipped so failover never lands on a stale subscription.
    fn best_replica(
        conns: &HashMap<ConnId, ConnState>,
        state: &SymbolState,
        stale_after: Duration,
    ) -> Option<usize> {
        let freshness = |r: &Replica| r.last_delta.max(r.synced_at);
        let freshest = state
            .replicas
            .iter()
            .filter(|r| Self::eligible(conns, r))
            .filter_map(|r| r.last_delta)
            .max();
        state
            .replicas
            .iter()
            .enumerate()
            .filter(|(_, r)| Self::eligible(conns, r))
            .filter(|(_, r)| match (freshest, freshness(r)) {
                (Some(freshest), Some(mine)) => {
                    freshest.saturating_duration_since(mine) <= stale_after
                }
                _ => true,
            })
            .min_by(|(_, a), (_, b)| {
                Self::latency(conns, a)
                    .total_cmp(&Self::latency(conns, b))
                    .then_with(|| b.last_msg.cmp(&a.last_msg))
            })
            .map(|(idx, _)| idx)
    }

    fn select_source(&mut self, symbol: &str, now: Instant, wall: DateTime<Utc>) {
        let Some(state) = self.symbols.get(symbol) else {
            return;
        };
        let best = Self::best_replica(&self.conns, state, self.config.replica_silence / 2);
        if best != state.source || best.is_none() {
            self.switch_source(symbol, best, now, wall);
        }
    }

    fn switch_source(
        &mut self,
        symbol: &str,
        to: Option<usize>,
        now: Instant,
        wall: DateTime<Utc>,
    ) {
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        let from = state.source;
        state.source = to;
        let Some(_) = to else {
            if from.is_some() {
                debug!(symbol, "uncovered until a replica resyncs");
            }
            return;
        };
        state.last_switch = Some(now);
        if from.is_some() {
            self.stats.source_switches += 1;
            debug!(symbol, ?from, ?to, "switched source");
        }
        self.write_snapshot(symbol, wall);
    }

    /// Emit a snapshot from the current source replica's own book.
    fn write_snapshot(&mut self, symbol: &str, wall: DateTime<Utc>) {
        let Some(state) = self.symbols.get(symbol) else {
            return;
        };
        let Some(source_idx) = state.source else {
            return;
        };
        let Some(replica) = state.replicas.get(source_idx) else {
            return;
        };
        let Some(books) = self.books_by_conn.get(&replica.conn) else {
            return;
        };

        let Ok((bids, asks, timestamp)) = books.with_book_mut(symbol, |b| {
            Ok((b.bids_fast(None), b.asks_fast(None), b.timestamp))
        }) else {
            return;
        };

        self.stats.snapshots_written += 1;
        self.emit_snapshot(symbol, wall, timestamp, bids, asks);
    }

    fn write_retirement_snapshot(&mut self, symbol: &str, wall: DateTime<Utc>) {
        self.stats.snapshots_written += 1;
        self.emit_snapshot(symbol, wall, wall, Vec::new(), Vec::new());
    }

    fn emit_snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        bids: Vec<polyfill_rs::types::FastBookLevel>,
        asks: Vec<polyfill_rs::types::FastBookLevel>,
    ) {
        let bids: Vec<SnapshotBidsEntryDomain> = bids
            .into_iter()
            .map(|l| SnapshotBidsEntryDomain {
                price_level: i64::from(l.price),
                quantity: l.size,
            })
            .collect();
        let asks: Vec<SnapshotAsksEntryDomain> = asks
            .into_iter()
            .map(|l| SnapshotAsksEntryDomain {
                price_level: i64::from(l.price),
                quantity: l.size,
            })
            .collect();

        let _ = self.output.send(Output::Snapshot(SnapshotDomain {
            received_at,
            exchange_timestamp,
            bids,
            asks,
            symbol: symbol.to_owned(),
        }));
    }

    /// Spread periodic snapshots by a stable per-symbol phase so the writer
    /// never sees the whole symbol set at one instant.
    fn spread_snapshots(&mut self, wall: DateTime<Utc>) {
        let slots = (self.config.snapshot_interval.as_millis() as u64 / TICK_MS).max(1);
        let current_slot = (wall.timestamp_millis() as u64 / TICK_MS) % slots;

        let due: Vec<String> = self
            .symbols
            .iter()
            .filter(|(_, s)| s.source.is_some() && s.snapshot_hash % slots == current_slot)
            .map(|(symbol, _)| symbol.clone())
            .collect();

        for symbol in &due {
            self.write_snapshot(symbol, wall);
        }
    }

    pub fn tick(&mut self, now: Instant, wall: DateTime<Utc>, queue_len: usize) {
        if now >= self.next_health {
            self.next_health = now + Duration::from_secs(1);
            self.check_health(now, wall);
        }

        if wall >= self.next_snapshot {
            self.next_snapshot = next_boundary(wall, self.config.snapshot_interval);
            if let (Some(rotation), Some(interval)) =
                (self.next_rotation, self.config.rotate_interval)
                && wall >= rotation
            {
                self.next_rotation = Some(next_boundary(wall, interval));
                let _ = self.output.send(Output::Rotate);
            }
        }

        self.spread_snapshots(wall);

        if now >= self.next_stats {
            self.next_stats = now + self.config.stats_interval;
            self.log_stats(queue_len);
        }
    }

    fn check_health(&mut self, now: Instant, wall: DateTime<Utc>) {
        let config = &self.config;
        let mut desyncs = Vec::new();
        let mut resubscribes = Vec::new();
        let mut reselect = Vec::new();
        let mut uncovered = Vec::new();

        for (symbol, state) in &mut self.symbols {
            let source_ok = state
                .source
                .is_some_and(|s| Self::eligible(&self.conns, &state.replicas[s]));
            if !source_ok
                && state
                    .replicas
                    .iter()
                    .any(|r| Self::eligible(&self.conns, r))
            {
                uncovered.push(symbol.clone());
            }

            // Compare level changes only: a sibling's fresh snapshot after a
            // resubscribe must not make this replica look silent.
            let freshest = state.replicas.iter().filter_map(|r| r.last_delta).max();
            for (idx, replica) in state.replicas.iter().enumerate() {
                if !self.conns.get(&replica.conn).is_some_and(|c| c.up) {
                    continue;
                }
                if !replica.synced {
                    let since = now.saturating_duration_since(replica.last_subscribe);
                    if (replica.resubscribe_pending && since >= config.resubscribe_backoff)
                        || since > config.resync_timeout
                    {
                        resubscribes.push((symbol.clone(), idx));
                    }
                } else if let (Some(freshest), Some(last)) =
                    (freshest, replica.last_delta.max(replica.synced_at))
                    && freshest.saturating_duration_since(last) > config.replica_silence
                {
                    desyncs.push((symbol.clone(), idx, "replica silent while siblings active"));
                }
            }

            // Every replica quiet for far longer than usual: rotate one at a time.
            let usual = state.mean_gap.unwrap_or(config.quiet_min.as_secs_f64());
            let threshold = Duration::from_secs_f64((usual * config.quiet_factor).clamp(
                config.quiet_min.as_secs_f64(),
                config.quiet_max.as_secs_f64(),
            ));
            let last_seen = state
                .last_activity
                .into_iter()
                .chain(state.last_quiet_resubscribe)
                .max()
                .unwrap_or(state.added_at);
            if now.saturating_duration_since(last_seen) > threshold {
                let candidate = state
                    .replicas
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| state.replicas.len() == 1 || Some(*idx) != state.source)
                    .min_by_key(|(_, r)| r.last_subscribe)
                    .map(|(idx, _)| idx);
                if let Some(idx) = candidate {
                    state.last_quiet_resubscribe = Some(now);
                    resubscribes.push((symbol.clone(), idx));
                }
            }

            // Prefer a clearly faster connection, with hysteresis.
            if let Some(source) = state.source
                && !state
                    .last_switch
                    .is_some_and(|t| now.saturating_duration_since(t) < config.min_switch_interval)
                && let Some(best) =
                    Self::best_replica(&self.conns, state, config.replica_silence / 2)
                && best != source
                && Self::latency(&self.conns, &state.replicas[source])
                    - Self::latency(&self.conns, &state.replicas[best])
                    > config.latency_switch_ms
            {
                reselect.push((symbol.clone(), source, best));
            }
        }

        for (symbol, idx, reason) in desyncs {
            self.desync(&symbol, idx, reason, now);
        }
        for (symbol, idx) in resubscribes {
            self.resubscribe(&symbol, idx, now);
        }
        for symbol in uncovered {
            self.select_source(&symbol, now, wall);
        }
        for (symbol, source, best) in reselect {
            // Earlier actions in this pass may have changed things.
            let Some(state) = self.symbols.get(&symbol) else {
                continue;
            };
            if state.source == Some(source) && Self::eligible(&self.conns, &state.replicas[best]) {
                self.switch_source(&symbol, Some(best), now, wall);
            }
        }
    }

    fn log_stats(&mut self, queue_len: usize) {
        let covered = self.symbols.values().filter(|s| s.source.is_some()).count();
        let uncovered = self.symbols.len().saturating_sub(covered);
        let conns_up = self.conns.values().filter(|c| c.up).count();
        let delta = |now: u64, then: u64| now - then;
        info!(
            symbols = self.symbols.len(),
            covered,
            uncovered,
            conns_up,
            conns = self.conns.len(),
            events = delta(self.stats.events, self.last_stats.events),
            updates = delta(self.stats.updates_written, self.last_stats.updates_written),
            snapshots = delta(
                self.stats.snapshots_written,
                self.last_stats.snapshots_written
            ),
            switches = delta(self.stats.source_switches, self.last_stats.source_switches),
            desyncs = delta(self.stats.desyncs, self.last_stats.desyncs),
            resubscribes = delta(self.stats.resubscribes, self.last_stats.resubscribes),
            queue_len,
            "engine stats (since last report)"
        );
        self.last_stats = self.stats.clone();
    }
}
