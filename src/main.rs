use std::io::IsTerminal;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use clap::{Args, Parser, Subcommand};
use engineering_challenge::collector::{self, CollectorConfig};
use engineering_challenge::connection::ConnConfig;
use engineering_challenge::engine::EngineConfig;
use engineering_challenge::output::Target;
use engineering_challenge::polymarket::Polymarket;
use engineering_challenge::supervisor::SupervisorConfig;
use tracing::{info, warn};

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Polymarket {
        #[command(flatten)]
        common: Common,
    },
    Dump {
        path: PathBuf,
        #[arg(long)]
        quiet: bool,
    },
}

#[derive(Args)]
struct Common {
    /// Output file; `-` writes to stdout.
    #[arg(
        long,
        short,
        env = "EC_OUTPUT",
        default_value = "book.sbe",
        conflicts_with = "output_dir"
    )]
    output: PathBuf,

    /// Write rotating, timestamped files to this directory instead.
    #[arg(long, env = "EC_OUTPUT_DIR")]
    output_dir: Option<PathBuf>,

    /// How often to start a new file in `--output-dir` (minutes).
    #[arg(long, env = "EC_ROTATE_MINUTES", default_value_t = 60)]
    rotate_minutes: u64,

    /// Redundant websockets per group of symbols.
    #[arg(long, env = "EC_REPLICAS", default_value_t = 2)]
    replicas: usize,

    /// Symbols per websocket (default: 200 for Polymarket, 250 for Kalshi).
    #[arg(long, env = "EC_SYMBOLS_PER_CONNECTION")]
    symbols_per_connection: Option<usize>,

    /// How often to look for new and closed symbols (seconds).
    #[arg(long, env = "EC_DISCOVERY_INTERVAL_SECS", default_value_t = 60)]
    discovery_interval_secs: u64,

    /// How often to write a full snapshot of every book (seconds).
    #[arg(long, env = "EC_SNAPSHOT_INTERVAL_SECS", default_value_t = 60)]
    snapshot_interval_secs: u64,

    /// Recycle every websocket this often; 0 disables recycling.
    #[arg(long, env = "EC_RECYCLE_MINUTES", default_value_t = 30)]
    recycle_minutes: u64,

    /// Upper bound on new websocket connections per second.
    #[arg(long, env = "EC_CONNECTS_PER_SECOND", default_value_t = 5.0)]
    connects_per_second: f64,
}

impl Common {
    fn into_config(
        self,
        exchange: &str,
        default_per_connection: usize,
    ) -> anyhow::Result<CollectorConfig> {
        let target = self.target(exchange)?;
        Ok(CollectorConfig {
            engine: EngineConfig {
                snapshot_interval: Duration::from_secs(self.snapshot_interval_secs.max(1)),
                rotate_interval: self
                    .output_dir
                    .as_ref()
                    .map(|_| Duration::from_secs(self.rotate_minutes.max(1) * 60)),
                ..EngineConfig::default()
            },
            supervisor: SupervisorConfig {
                replicas: self.replicas.max(1),
                symbols_per_connection: self
                    .symbols_per_connection
                    .unwrap_or(default_per_connection),
                discovery_interval: Duration::from_secs(self.discovery_interval_secs.max(1)),
                removal_confirmations: 2,
                connects_per_second: self.connects_per_second,
                connection: ConnConfig {
                    recycle_interval: NonZeroU64::new(self.recycle_minutes)
                        .map(|n| Duration::from_secs(n.get() * 60)),
                    ..ConnConfig::default()
                },
            },
            target,
        })
    }

    /// Where the writer should send its output.
    fn target(&self, exchange: &str) -> anyhow::Result<Target> {
        if let Some(dir) = &self.output_dir {
            return Ok(Target::Directory {
                dir: dir.clone(),
                prefix: exchange.to_owned(),
            });
        }
        if self.output.as_os_str() == "-" {
            if std::io::stdout().is_terminal() {
                bail!(
                    "refusing to write binary output to a terminal; \
                     use --output <file> or --output-dir <dir>"
                );
            }
            return Ok(Target::File(PathBuf::from("/dev/stdout")));
        }
        Ok(Target::File(self.output.clone()))
    }
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }

    info!("signal received; shutting down");
    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
        warn!("forced exit");
        std::process::exit(130);
    });
}

async fn collect(exchange: Polymarket, config: CollectorConfig) -> anyhow::Result<()> {
    let summary = collector::run(exchange, config, shutdown_signal()).await?;
    info!(?summary, "done");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // stdout carries the data, so logs go to stderr.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Polymarket { common } => {
            // 200 symbols per connection is a good default for Polymarket.
            let config = common.into_config("polymarket", 200)?;
            collect(Polymarket::new()?, config).await
        }
        Command::Dump { path, quiet } => {
            use std::io::Write as _;
            let mut stdout = std::io::stdout().lock();
            let s = engineering_challenge::dump::dump(&path, quiet, &mut stdout)?;
            let _ = stdout.flush();
            drop(stdout);

            if !quiet {
                eprintln!("--- {} ---", path.display());
                eprintln!("{s}");
            }
            if s.is_healthy() {
                Ok(())
            } else {
                anyhow::bail!("dump unhealthy")
            }
        }
    }
}
