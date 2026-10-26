use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::engine::{ConnCommand, ConnId, EngineMsg};
use crate::polymarket::{Event, Polymarket, PolymarketDecoder};
use chrono::Utc;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use polyfill_rs::book::OrderBookManager;
use rand::Rng;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::{debug, info, warn};
const DEFAULT_KEEPALIVE: Duration = Duration::from_secs(10);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RECYCLE_INTERVAL: Duration = Duration::from_secs(30 * 60);
const DEFAULT_SUBSCRIBE_BATCH: usize = 100;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const STABLE_AFTER: Duration = Duration::from_secs(30);
const RECYCLE_POSTPONE: Duration = Duration::from_secs(30);
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub struct ConnConfig {
    pub keepalive_interval: Duration,
    pub idle_timeout: Duration,
    pub recycle_interval: Option<Duration>,
    pub subscribe_batch: usize,
    pub connect_timeout: Duration,
}

impl Default for ConnConfig {
    fn default() -> Self {
        Self {
            keepalive_interval: DEFAULT_KEEPALIVE,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            recycle_interval: Some(DEFAULT_RECYCLE_INTERVAL),
            subscribe_batch: DEFAULT_SUBSCRIBE_BATCH,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }
}

pub struct ConnectLimiter {
    next: Mutex<Instant>,
    gap: Duration,
}

impl ConnectLimiter {
    pub fn new(per_second: f64) -> Self {
        Self {
            next: Mutex::new(Instant::now()),
            gap: Duration::from_secs_f64(1.0 / per_second.max(0.01)),
        }
    }

    pub async fn wait(&self) {
        let at = {
            let mut next = self.next.lock().unwrap_or_else(|e| e.into_inner());
            let at = (*next).max(Instant::now());
            *next = at + self.gap;
            at
        };
        sleep_until(at).await;
    }
}

pub struct Connection {
    pub id: ConnId,
    pub exchange: Arc<Polymarket>,
    pub symbols: BTreeSet<String>,
    pub commands: mpsc::UnboundedReceiver<ConnCommand>,
    pub books: Arc<OrderBookManager>,
    pub engine: mpsc::UnboundedSender<EngineMsg>,
    pub limiter: Arc<ConnectLimiter>,
    pub config: ConnConfig,
    pub shutdown: watch::Receiver<bool>,
    pub next_recycle: Option<Instant>,
    pub shard_up: Arc<AtomicUsize>,
    pub replicas: usize,
}

enum Exit {
    Shutdown,
    Recycle,
    Lost(String),
}

fn jitter(duration: Duration, fraction: f64) -> Duration {
    let factor = 1.0 + rand::thread_rng().gen_range(-fraction..=fraction);
    duration.mul_f64(factor.max(0.0))
}

impl Connection {
    pub async fn run(mut self) {
        let mut backoff = Duration::from_secs(1);
        while !*self.shutdown.borrow() {
            let limiter = Arc::clone(&self.limiter);
            if self.wait_handling_commands(limiter.wait()).await.is_err() {
                return;
            }

            let attempt = Instant::now();
            let exit = self
                .connect_and_stream()
                .await
                .unwrap_or_else(|e| Exit::Lost(format!("{e:#}")));
            // Only a connection that stayed up resets backoff, so a server
            // that accepts then immediately drops us isn't hammered.
            if attempt.elapsed() >= STABLE_AFTER {
                backoff = Duration::from_secs(1);
            }
            match exit {
                Exit::Shutdown => return,
                Exit::Recycle => {
                    info!(conn = self.id, "recycling connection");
                    continue;
                }
                Exit::Lost(reason) => {
                    warn!(conn = self.id, %reason, retry_in = ?backoff, "connection lost");
                }
            }

            let wake = Instant::now() + jitter(backoff, 0.25);
            backoff = (backoff * 2).min(Duration::from_secs(60));
            if self
                .wait_handling_commands(sleep_until(wake))
                .await
                .is_err()
            {
                return;
            }
        }
    }

    /// Awaits `until`, applying commands and aborting on shutdown.
    async fn wait_handling_commands(&mut self, until: impl Future<Output = ()>) -> Result<(), ()> {
        tokio::pin!(until);
        loop {
            tokio::select! {
                _ = &mut until => return Ok(()),
                Some(cmd) = self.commands.recv() => self.apply_bookkeeping(&cmd),
                _ = self.shutdown.changed() => return Err(()),
            }
        }
    }

    fn apply_bookkeeping(&mut self, command: &ConnCommand) {
        match command {
            ConnCommand::Subscribe(symbols) => self.symbols.extend(symbols.iter().cloned()),
            ConnCommand::Unsubscribe(symbols) => {
                for symbol in symbols {
                    self.symbols.remove(symbol);
                }
            }
            ConnCommand::Resubscribe(_) => {}
        }
    }

    async fn connect_and_stream(&mut self) -> anyhow::Result<Exit> {
        let request = self.exchange.connect_request()?;
        let (socket, _) = timeout(
            self.config.connect_timeout,
            tokio_tungstenite::connect_async_with_config(request, None, true),
        )
        .await
        .map_err(|_| anyhow::anyhow!("connect timed out"))??;
        let (mut sink, mut stream) = socket.split();

        let mut codec = PolymarketDecoder::new(Arc::clone(&self.books));

        debug!(conn = self.id, symbols = self.symbols.len(), "connected");
        let _ = self.engine.send(EngineMsg::ConnUp(self.id));
        self.shard_up.fetch_add(1, Ordering::SeqCst);
        let exit = self.stream(&mut sink, &mut stream, &mut codec).await;
        self.shard_up.fetch_sub(1, Ordering::SeqCst);

        if !matches!(exit, Exit::Shutdown) {
            let _ = self.engine.send(EngineMsg::ConnDown(self.id));
        }

        let _ = timeout(Duration::from_secs(2), async {
            let _ = sink.send(Message::Close(None)).await;
            let _ = sink.close().await;
        })
        .await;
        Ok(exit)
    }

    async fn stream<S, R>(
        &mut self,
        sink: &mut S,
        stream: &mut R,
        codec: &mut PolymarketDecoder,
    ) -> Exit
    where
        S: Sink<Message, Error = WsError> + Unpin,
        R: Stream<Item = Result<Message, WsError>> + Unpin,
    {
        let symbols: Vec<String> = self.symbols.iter().cloned().collect();
        for batch in symbols.chunks(self.config.subscribe_batch.max(1)) {
            for message in codec.subscribe(batch) {
                if let Err(error) = sink.send(Message::text(message)).await {
                    return Exit::Lost(format!("subscribe failed: {error}"));
                }
            }
        }
        self.advance_recycle();

        let mut postponed: Option<Instant> = None;
        let mut keepalive = tokio::time::interval(self.config.keepalive_interval);
        keepalive.reset();
        let mut last_rx = Instant::now();
        let mut events: Vec<Event> = Vec::new();

        loop {
            let recycle_at = postponed.or(self.next_recycle);
            tokio::select! {
                frame = stream.next() => {
                    let received_at = Utc::now();
                    last_rx = Instant::now();
                    let Some(frame) = (match frame {
                        Some(Ok(Message::Text(text))) => {
                            self.decode(codec, &text, received_at, &mut events);
                            Some(true)
                        }
                        Some(Ok(Message::Close(frame))) => {
                            return Exit::Lost(format!("closed by exchange: {frame:?}"));
                        }
                        Some(Ok(_)) => Some(true),
                        Some(Err(error)) => return Exit::Lost(error.to_string()),
                        None => return Exit::Lost("stream ended".into()),
                    }) else { return Exit::Lost("stream ended".into()) };
                    let _ = frame;
                    for message in codec.drain_outbox() {
                        if let Err(error) = sink.send(Message::text(message)).await {
                            return Exit::Lost(format!("send failed: {error}"));
                        }
                    }
                }
                command = self.commands.recv() => {
                    let Some(command) = command else { return Exit::Shutdown };
                    self.apply_bookkeeping(&command);
                    let messages = match command {
                        ConnCommand::Subscribe(symbols) => symbols
                            .chunks(self.config.subscribe_batch.max(1))
                            .flat_map(|batch| codec.subscribe(batch))
                            .collect(),
                        ConnCommand::Unsubscribe(symbols) => codec.unsubscribe(&symbols),
                        ConnCommand::Resubscribe(symbols) => {
                            let filtered: Vec<String> = symbols
                                .into_iter()
                                .filter(|s| self.symbols.contains(s))
                                .collect();
                            let mut messages = codec.unsubscribe(&filtered);
                            messages.extend(codec.subscribe(&filtered));
                            messages
                        }
                    };
                    for message in messages {
                        if let Err(error) = sink.send(Message::text(message)).await {
                            return Exit::Lost(format!("send failed: {error}"));
                        }
                    }
                }
                _ = keepalive.tick() => {
                    if last_rx.elapsed() > self.config.idle_timeout {
                        return Exit::Lost(format!("no frames for {:?}", last_rx.elapsed()));
                    }
                    if let Err(error) = sink.send(Message::text(codec.keepalive())).await {
                        return Exit::Lost(format!("keepalive failed: {error}"));
                    }
                }
                _ = async { sleep_until(recycle_at.unwrap()).await }, if recycle_at.is_some() => {
                    // Never recycle while a sibling is down: it would leave
                    // this shard's symbols uncovered.
                    if self.shard_up.load(Ordering::SeqCst) < self.replicas {
                        postponed = Some(Instant::now() + RECYCLE_POSTPONE);
                        continue;
                    }
                    self.advance_recycle();
                    return Exit::Recycle;
                }
                _ = self.shutdown.changed() => {
                    let _ = sink.send(Message::Close(None)).await;
                    let _ = timeout(SHUTDOWN_DRAIN, async {
                        while let Some(frame) = stream.next().await {
                            match frame {
                                Ok(Message::Text(text)) => {
                                    let received_at = Utc::now();
                                    self.decode(codec, &text, received_at, &mut events);
                                }
                                Ok(Message::Close(_)) | Err(_) => break,
                                Ok(_) => {}
                            }
                        }
                    })
                    .await;
                    return Exit::Shutdown;
                }
            }
        }
    }

    /// Decodes one frame and forwards non-empty event batches to the engine.
    fn decode(
        &self,
        codec: &mut PolymarketDecoder,
        text: &str,
        received_at: chrono::DateTime<Utc>,
        events: &mut Vec<Event>,
    ) {
        if let Err(error) = codec.decode(text, events) {
            debug!(conn = self.id, %error, "undecodable frame");
        }
        if !events.is_empty() {
            let _ = self.engine.send(EngineMsg::Feed {
                conn: self.id,
                received_at,
                events: std::mem::take(events),
            });
        }
    }

    fn advance_recycle(&mut self) {
        if let (Some(next), Some(interval)) =
            (self.next_recycle.as_mut(), self.config.recycle_interval)
        {
            while *next <= Instant::now() {
                *next += interval;
            }
        }
    }
}

pub async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = sleep(duration) => false,
        _ = shutdown.changed() => true,
    }
}
