use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use polyfill_rs::book::OrderBookManager;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::connection::{ConnConfig, ConnectLimiter, Connection, sleep_or_shutdown};
use crate::engine::{ConnCommand, ConnId, EngineMsg};
use crate::polymarket::{Discovered, Polymarket};

const BOOK_DEPTH: usize = 200;

#[derive(Clone, Debug)]
pub struct SupervisorConfig {
    pub replicas: usize,
    pub symbols_per_connection: usize,
    pub discovery_interval: Duration,
    pub removal_confirmations: u32,
    pub connects_per_second: f64,
    pub connection: ConnConfig,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            replicas: 2,
            symbols_per_connection: 200,
            discovery_interval: Duration::from_secs(60),
            removal_confirmations: 2,
            connects_per_second: 5.0,
            connection: ConnConfig::default(),
        }
    }
}

struct Shard {
    symbols: HashSet<String>,
    conns: Vec<(ConnId, mpsc::UnboundedSender<ConnCommand>)>,
}

pub struct Supervisor {
    exchange: Arc<Polymarket>,
    config: SupervisorConfig,
    engine: mpsc::UnboundedSender<EngineMsg>,
    shutdown: watch::Receiver<bool>,
    limiter: Arc<ConnectLimiter>,
    shards: Vec<Shard>,
    location: HashMap<String, usize>,
    tasks: JoinSet<()>,
    next_conn: ConnId,
    missing: HashMap<String, u32>,
}

impl Supervisor {
    pub fn new(
        exchange: Arc<Polymarket>,
        config: SupervisorConfig,
        engine: mpsc::UnboundedSender<EngineMsg>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            limiter: Arc::new(ConnectLimiter::new(config.connects_per_second)),
            exchange,
            config,
            engine,
            shutdown,
            shards: Vec::new(),
            location: HashMap::new(),
            tasks: JoinSet::new(),
            next_conn: 0,
            missing: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        loop {
            let discovered = tokio::select! {
                result = self.exchange.discover() => result,
                _ = self.shutdown.changed() => break,
            };
            match discovered {
                Ok(d) => self.reconcile(d),
                Err(e) => warn!(error = %format!("{e:#}"), "symbol discovery failed"),
            }
            if sleep_or_shutdown(self.config.discovery_interval, &mut self.shutdown).await {
                break;
            }
        }

        info!("stopping connections");
        self.shards.clear();
        let tasks = std::mem::take(&mut self.tasks);
        if tokio::time::timeout(Duration::from_secs(10), tasks.join_all())
            .await
            .is_err()
        {
            warn!("connections did not stop in time");
        }
    }

    fn reconcile(&mut self, discovered: Discovered) {
        let live: HashSet<String> = discovered.symbols.into_iter().collect();
        self.missing.retain(|s, _| !live.contains(s));

        let mut removed = HashSet::new();
        if discovered.complete {
            let threshold = self.config.removal_confirmations.max(1);
            for symbol in self.location.keys().filter(|s| !live.contains(*s)) {
                *self.missing.entry(symbol.clone()).or_default() += 1;
            }
            self.missing.retain(|symbol, count| {
                if *count >= threshold {
                    removed.insert(symbol.clone());
                    false
                } else {
                    true
                }
            });
        }

        let mut added: Vec<String> = live
            .into_iter()
            .filter(|s| !self.location.contains_key(s))
            .collect();
        added.sort_unstable();

        self.remove(&removed);
        self.add(&added);
        info!(
            added = added.len(),
            removed = removed.len(),
            symbols = self.location.len(),
            shards = self.shards.len(),
            connections = self.shards.len() * self.config.replicas,
            "symbols reconciled"
        );
    }

    fn remove(&mut self, removed: &HashSet<String>) {
        if removed.is_empty() {
            return;
        }
        let _ = self
            .engine
            .send(EngineMsg::RemoveSymbols(removed.iter().cloned().collect()));

        let mut by_shard: Vec<Vec<String>> = vec![Vec::new(); self.shards.len()];
        for symbol in removed {
            if let Some(idx) = self.location.remove(symbol) {
                by_shard[idx].push(symbol.clone());
            }
        }

        for (idx, symbols) in by_shard.into_iter().enumerate() {
            if symbols.is_empty() {
                continue;
            }
            let shard = &mut self.shards[idx];
            for symbol in &symbols {
                shard.symbols.remove(symbol);
            }
            for (_, commands) in &shard.conns {
                let _ = commands.send(ConnCommand::Unsubscribe(symbols.clone()));
            }
        }
    }

    fn add(&mut self, added: &[String]) {
        let capacity = self.config.symbols_per_connection.max(1);
        let mut remaining = added;

        for idx in 0..self.shards.len() {
            if remaining.is_empty() {
                break;
            }
            let free = capacity.saturating_sub(self.shards[idx].symbols.len());
            if free == 0 {
                continue;
            }
            let (batch, rest) = remaining.split_at(free.min(remaining.len()));
            remaining = rest;

            let shard = &mut self.shards[idx];
            for symbol in batch {
                shard.symbols.insert(symbol.clone());
                self.location.insert(symbol.clone(), idx);
            }
            // Engine must learn the symbols before any data can arrive.
            let _ = self.engine.send(EngineMsg::AddSymbols {
                symbols: batch.to_vec(),
                conns: shard.conns.iter().map(|(id, _)| *id).collect(),
            });
            for (_, commands) in &shard.conns {
                let _ = commands.send(ConnCommand::Subscribe(batch.to_vec()));
            }
        }

        for batch in remaining.chunks(capacity) {
            self.spawn_shard(batch);
        }
    }

    fn spawn_shard(&mut self, symbols: &[String]) {
        let idx = self.shards.len();
        let replicas = self.config.replicas.max(1);
        let conn_ids: Vec<ConnId> = (0..replicas).map(|r| self.next_conn + r).collect();
        self.next_conn += replicas;

        let _ = self.engine.send(EngineMsg::AddSymbols {
            symbols: symbols.to_vec(),
            conns: conn_ids.clone(),
        });

        let shard_up = Arc::new(AtomicUsize::new(0));
        let shard_offset = Duration::from_secs_f64(
            rand::Rng::gen_range(&mut rand::thread_rng(), 0.0..1.0) / replicas as f64,
        );
        let mut conns = Vec::new();

        for (replica, &id) in conn_ids.iter().enumerate() {
            let (commands_tx, commands_rx) = mpsc::unbounded_channel();

            // One book per connection; the engine holds the same Arc.
            let books = Arc::new(OrderBookManager::new(BOOK_DEPTH));

            let _ = self.engine.send(EngineMsg::Register {
                conn: id,
                commands: commands_tx.clone(),
                books: Arc::clone(&books),
            });

            let next_recycle = self.config.connection.recycle_interval.map(|interval| {
                tokio::time::Instant::now()
                    + shard_offset.mul_f64(interval.as_secs_f64())
                    + interval.mul_f64((replica + 1) as f64 / replicas as f64)
            });

            let connection = Connection {
                id,
                exchange: Arc::clone(&self.exchange),
                symbols: symbols.iter().cloned().collect::<BTreeSet<_>>(),
                commands: commands_rx,
                books,
                engine: self.engine.clone(),
                limiter: Arc::clone(&self.limiter),
                config: self.config.connection.clone(),
                shutdown: self.shutdown.clone(),
                next_recycle,
                shard_up: Arc::clone(&shard_up),
                replicas,
            };
            self.tasks.spawn(connection.run());
            conns.push((id, commands_tx));
        }

        for symbol in symbols {
            self.location.insert(symbol.clone(), idx);
        }
        self.shards.push(Shard {
            symbols: symbols.iter().cloned().collect(),
            conns,
        });
    }
}
