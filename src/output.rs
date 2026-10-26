use std::path::PathBuf;
use std::thread::JoinHandle;

use anyhow::Context;
use chrono::Utc;
use crossbeam::channel::{Receiver, Sender, unbounded};
use tracing::info;

use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

pub const CHUNK_SIZE: usize = 64 << 20;

pub enum Output {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
    /// Close the current file and start a new one.
    Rotate,
}

#[derive(Clone, Debug)]
pub enum Target {
    File(PathBuf),
    Directory { dir: PathBuf, prefix: String },
}

impl Target {
    fn next_path(&self) -> PathBuf {
        match self {
            Target::File(path) => path.clone(),
            Target::Directory { dir, prefix } => dir.join(format!(
                "{prefix}-{}.sbe",
                Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
            )),
        }
    }

    fn rotates(&self) -> bool {
        matches!(self, Target::Directory { .. })
    }
}

#[derive(Debug, Default)]
pub struct WriterStats {
    pub snapshots: u64,
    pub updates: u64,
    pub files: u64,
}

pub struct OutputHandle {
    pub sender: Sender<Output>,
    thread: JoinHandle<anyhow::Result<WriterStats>>,
    pub finished: tokio::sync::mpsc::Receiver<()>,
}

impl OutputHandle {
    pub fn finish(self) -> anyhow::Result<WriterStats> {
        drop(self.sender);
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("output thread panicked"))?
    }
}

pub fn spawn(target: Target) -> anyhow::Result<OutputHandle> {
    if let Target::Directory { dir, .. } = &target {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating output directory {}", dir.display()))?;
    }
    let writer = Writer::create(target.next_path())?;
    let (sender, receiver) = unbounded();
    let (finished_tx, finished_rx) = tokio::sync::mpsc::channel(1);
    let thread = std::thread::Builder::new()
        .name("sbe-writer".into())
        .spawn(move || {
            let result = run(target, writer, receiver);
            //inform finish
            let _ = finished_tx.blocking_send(());
            result
        })?;
    Ok(OutputHandle {
        sender,
        thread,
        finished: finished_rx,
    })
}
struct Writer {
    sbe: AlignedSbeWriter,
    path: PathBuf,
}
//writter for sbe
impl Writer {
    fn create(path: PathBuf) -> anyhow::Result<Self> {
        let sbe = AlignedSbeWriter::create(&path, CHUNK_SIZE)
            .with_context(|| format!("opening output {}", path.display()))?;
        info!(path = %path.display(), "writing to");
        Ok(Self { sbe, path })
    }

    fn write_snapshot(&mut self, msg: &SnapshotDomain) -> anyhow::Result<()> {
        self.sbe.write_msg(msg)
    }

    fn write_update(&mut self, msg: &PriceLevelUpdateDomain) -> anyhow::Result<()> {
        self.sbe.write_msg(msg)
    }

    fn rotate(&mut self, target: &Target) -> anyhow::Result<()> {
        self.sbe.flush()?;
        info!(path = %self.path.display(), "closed output");
        *self = Self::create(target.next_path())?;
        Ok(())
    }

    fn close(mut self) -> anyhow::Result<()> {
        self.sbe.flush()?;
        info!(path = %self.path.display(), "closed output");
        Ok(())
    }
}

//writter core
fn run(
    target: Target,
    mut writer: Writer,
    receiver: Receiver<Output>,
) -> anyhow::Result<WriterStats> {
    let mut stats = WriterStats {
        files: 1,
        ..WriterStats::default()
    };
    for message in receiver {
        match message {
            Output::Snapshot(snapshot) => {
                writer.write_snapshot(&snapshot)?;
                stats.snapshots += 1;
            }
            Output::Update(update) => {
                writer.write_update(&update)?;
                stats.updates += 1;
            }
            Output::Rotate if target.rotates() => {
                writer.rotate(&target)?;
                stats.files += 1;
            }
            Output::Rotate => {}
        }
    }

    writer.close()?;
    Ok(stats)
}
