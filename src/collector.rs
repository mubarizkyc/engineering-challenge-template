use std::future::Future;
use std::sync::Arc;

use crate::engine::{Engine, EngineConfig, EngineStats};
use crate::output::{self, Target, WriterStats};
use crate::polymarket::Polymarket;
use crate::supervisor::{Supervisor, SupervisorConfig};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tracing::{error, info};

#[derive(Clone, Debug)]
pub struct CollectorConfig {
    pub target: Target,
    pub engine: EngineConfig,
    pub supervisor: SupervisorConfig,
}

#[derive(Debug)]
pub struct Summary {
    pub engine: EngineStats,
    pub writer: WriterStats,
}
pub async fn run(
    exchange: Polymarket,
    config: CollectorConfig,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<Summary> {
    let CollectorConfig {
        target,
        engine,
        supervisor,
    } = config;

    let mut output = output::spawn(target)?;

    let (engine_tx, engine_rx) = mpsc::unbounded_channel();
    let engine = Engine::new(
        engine,
        output.sender.clone(),
        Instant::now(),
        chrono::Utc::now(),
    );
    let engine_task = tokio::spawn(engine.run(engine_rx));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = Supervisor::new(Arc::new(exchange), supervisor, engine_tx, shutdown_rx);
    let supervisor_task = tokio::spawn(supervisor.run());

    // Either the caller stops us, or the writer thread exits (panic or I/O error).
    tokio::select! {
        _ = shutdown => info!("shutdown requested: closing websockets"),
        _ = output.finished.recv() => error!("output writer stopped unexpectedly"),
    }
    let _ = shutdown_tx.send(true);
    //drop supervisro(engine_tx),engine then writter
    let supervisor = supervisor_task.await;
    let engine = engine_task.await;
    info!("engine drained: closing output");

    let writer = tokio::task::spawn_blocking(move || output.finish()).await??;
    info!(?writer, "output closed");

    supervisor?;
    Ok(Summary {
        engine: engine?,
        writer,
    })
}
