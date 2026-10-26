# Polymarket Order Book Collector

A Rust binary that streams the full Polymarket CLOB order book (every live
symbol, redundant websockets, per-replica books) and writes snapshots plus
deltas to an aligned SBE file.

Overview. `main.rs` parses CLI, sets up tracing, dispatches.
`collector.rs` wires the writer, engine, and supervisor together and owns
shutdown ordering. `supervisor.rs` handles discovery, sharding, and the
lifecycle of connection tasks. `connection.rs` is one websocket task , it
connects, keepalives, recycles, and backs off. `polymarket.rs` does Gamma
discovery and CLOB decoding. `engine.rs` is the per-symbol state machine:
source selection, health checks, snapshot cadence. `output.rs` is a
dedicated writer thread. `dump.rs` reads the SBE file back and verifies it.

The four modules that matter are `supervisor`, `connection`, `polymarket`,
`engine`. The rest is scaffolding.

### Redundant websockets

Each shard is up to 200 symbols, carried by N independent connections.
Every connection owns its own `OrderBookManager`. we are using one book per connection
My first instinct was a single shared manager, with
all sockets writing into it. That looked simpler. But it meant every apply
had cross-socket lock contention, and any desync on one socket poisoned the
book every other socket was reading from. 

One replica per symbol is the *source* — only its deltas go to output.
The others stay fully synced in the background, When the source switches, we write a fresh snapshot
from the new source's own book, because the two replicas' books may have
drifted and the consumer needs an anchor.

### Subscription monitoring

My first version desynced on every locked book — bid equal
to ask — because I trusted polyfill's `is_valid()`, which uses strict
`best_bid < best_ask`. Locked books happen constantly in binary markets
during tight consensus. I spent hours chasing phantom desyncs before I
noticed the semantics. Fixing it meant writing my own `is_crossed` with
strict `>` and remembering to use it in every check.

The health check runs every second. For each replica it asks: has this
replica ever synced? If not, and `resync_timeout` has passed, resubscribe.
If it has synced but has gone silent while its sibling is still receiving
deltas, desync it and resubscribe once `replica_silence` has elapsed. There
is a per-symbol `mean_gap` — an EWMA of the gaps between source deltas — so
quiet symbols can be probed on their own schedule instead of a fixed
timeout; when every replica has been quiet for far longer than that symbol
usually is, we rotate one replica (never the source, while another exists)
to check the market is really idle. Latency reselection only fires if a
sibling is 2 seconds faster, with a 30-second hysteresis, and
`resubscribe_backoff` prevents tight loops by holding a resubscribe until
its cooldown has expired.

### polymarket_client_sdk_v2

The official Polymarket Rust SDK. I use exactly one surface of it:
`GammaClient::events()` with an `EventsRequest` for paginated discovery
against `gamma-api.polymarket.com/events`. Nothing else — no CLOB access,
no order submission, no credentials.

The filter is the important part. A market has to have `enable_order_book`
true, `accepting_orders` true, `closed` false, and — the clause that
actually made the system work — **liquidity or volume greater than zero**.
Polymarket serves many ghost markets with zero liquidity and zero volume;
but we dont subscibe

### polyfill-rs

A high-performance order book library that handles everything on the hot
path. Fixed-point integers internally: `Price` is a `u32` in ticks at
1/10,000 of the base unit, `Qty` is an `i64` in units at 1/10,000. The
`OrderBookManager` is sharded into 64 `RwLock<HashMap<String, OrderBook>>`
partitions, so writes to different symbols rarely contend. Inside each
book, `BookSide` is a sorted `Vec<(Price, StoredLevel)>` with binary-search
insertion — bids highest-first, asks lowest-first, so top-of-book is
always `levels[0]`.

Snapshots go through `apply_book_update`. It validates the entire snapshot
before mutating anything, so a malformed level leaves the existing book
untouched. It replaces both sides atomically using generation counters.
It rejects stale-timestamp replays and orders same-timestamp distinct-hash
states by arrival.

Deltas go through `with_book_mut(symbol, |b| b.apply_delta(...))`. One lock
per asset-batch, not per entry. `size == 0` removes the level. It rejects
stale sequences — I feed it a monotonic counter per decoder, because
Polymarket has no usable server-side sequence. Reads for emission use
`bids_fast` and `asks_fast`, which return `Vec<FastBookLevel>` directly
from the sorted vector — no `Decimal` allocation, no re-sort. Top-of-book
checks use `best_bid_fast` and `best_ask_fast`, which are O(1) reads of
`levels[0]`.

The performance character is roughly five nanoseconds per operation versus
a hundred for a Decimal-based book — twenty-odd times faster on the hot
path, zero per-message allocation. The wins come from integer comparisons
instead of `Decimal::Ord`, a sorted `Vec` instead of a `BTreeMap` so
top-of-book stays cache-local, and generation-based snapshot replacement so
clearing stale levels doesn't require a scan.

I don't use `set_tick_size` — Polymarket prices are always exactly
four-decimal-representable and `decimal_to_price_exact` already rejects
anything that isn't. I don't use `BookAnalytics` — there's no metrics
surface yet.

### Generic decode/encode — attempted, then removed

I built a generic `Decoder` trait first, with associated error and book
types, thinking about a hypothetical second exchange.
I spent a whole session confused by my own API before
deleting it. The current decoder is concrete —
`pub fn decode(&mut self, text: &str, out: &mut Vec<Event>)` — with no
generic parameters and no associated types. If a second exchange lands, the
abstraction can come back with two real implementors to design against.

### Shortcomings

I trust the library's `is_valid()` and didn't. That's the first one. It
uses strict `best_bid < best_ask`, so locked books read as invalid, and
locked books are normal in binary markets. I spent hours chasing phantom
desyncs before I noticed. The lesson: read the semantics, don't assume a
method named "is_valid" means what you think it means.

I trusted the exchange's reported top-of-book. My first decoder desynced
when the exchange's `best_bid` and `best_ask` on a `price_change` didn't
match my post-delta book. Structurally wrong — the exchange samples TOB at
a different moment than we apply the batch. This caused a false-desync
storm on every reconnect. The fix is to log mismatch at `debug!` and never
desync on it. The book we built is authoritative.

I tuned the thresholds wrong. `latency_switch_ms = 50` caused a
1,100-switch-per-window storm because fifty milliseconds of EWMA jitter is
noise, not signal. `idle_timeout = 30s` caused reconnect churn on quiet
symbols. `resubscribe_backoff = 10s` retried before the exchange had a
chance to send anything. Each was a small number that felt safe and wasn't.
Current values are `latency_switch_ms = 2000`, `min_switch_interval = 30s`,
`replica_silence = 60s`.

### Latency and throughput

The polyfill fixed-point book means no `Decimal` allocation on the delta
path. Batching the lock per asset in the decoder means one `with_book_mut`
per asset-batch, not one per entry. The snapshot spreading eliminates the
writer-queue burst at each 60s boundary. The aligned SBE writer thread
keeps a 64 MiB buffer and issues one `write` syscall per minute at steady
state — encoding and disk I/O happen off-runtime, so a slow disk can never
block a socket read. Every connection carries its own latency EWMA, used
only for source reselection and never for hot-path decisions. Thresholds
are tuned for a data collector rather than a trader, which is why
`latency_switch_ms` is two seconds and not fifty milliseconds.

### Polymarket module

`Polymarket` is discovery plus the WS entry point. `new()` builds one
`GammaClient` and stores the WS URL. `discover()` paginates `/events`,
dedupes tokens, and returns `{symbols, complete}`. `fetch_events_page`
makes one SDK call, applies the filter, and extracts `clob_token_ids`.
`decoder()` creates a fresh `PolymarketDecoder` with a fresh
`OrderBookManager`. `connect_request()` builds the WS `Request`.

`PolymarketDecoder` is the per-connection WS decoder. It holds an
`Arc<OrderBookManager>`, a `HashSet<String>` of synced symbols, and a
monotonic `sequence: u64`. `handle_book` is where the reconnect-reset
happens: on the first snapshot for a symbol on this connection it resets
the book's `last_snapshot_timestamp_ms`, `last_delta_sequence`, and
`sequence` to zero so a reconnect can reload the same symbol. Then it calls
`apply_book_update`, then verifies two things — that
`last_snapshot_timestamp_ms` is nonzero (the update actually landed, not
silently rejected) and that the book is not crossed. On success it marks
the symbol synced and emits a `Snapshot`; on any failure it removes the
symbol from `synced` and emits a `Desync` for the engine to handle.

`handle_price_change` groups entries by asset first, because one
`price_change` message can carry both the YES and NO sides of a market.
Then for each asset it skips if not synced, skips if the message timestamp
predates the snapshot (would double-count), takes one `with_book_mut` lock,
applies every entry for that asset, and reads back the crossed state and
top-of-book. It emits one `Delta` per entry, with the entry's own TOB. The
comparison against the exchange's reported TOB is `debug!` only — the book
we built is authoritative and the exchange samples at a different moment.

`is_crossed` is my helper, not polyfill's — strict `best_bid > best_ask`.
Locked is normal and is not corruption.

### Engine module

`Engine` is the per-symbol state machine. It holds four maps: per-connection
books, per-symbol state (replicas, source, activity/gap EWMAs, timing),
per-connection state (up flag, latency EWMA, command sender), and cumulative
stats.

`handle` dispatches the messages. `ConnUp` marks the connection up and
marks every replica on it as unsynced, because a fresh connection implies
a fresh subscription. `ConnDown` marks the connection down, drops replica
sync, and reselects any source that was on it — but it does not drop the
book `Arc`, because the connection reuses it on reconnect. `Register`
stores the book manager and command sender. `AddSymbols` creates
`SymbolState` and caches the FNV-1a hash used for snapshot spreading.
`RemoveSymbols` emits an empty retirement snapshot before removing state,
so a downstream reader can tell "book closed" apart from "book still open,
we just stopped."

`handle_event` handles the three event kinds. `Snapshot` marks the replica
synced, re-emits if the replica is the source, and tries source selection
if the replica is a backup and there's no eligible source. `Delta` updates
the connection's latency EWMA and, if the replica is the source, updates
`mean_gap`, writes `Output::Update`, and tries source selection if there's
no source yet. `Desync` marks the replica unsynced, sets
`resubscribe_pending`, resubscribes, and reselects if it was the source.

`tick` runs at 250ms granularity. Every second it calls `check_health`,
which collects desyncs, resubscribes, reselections, and uncovered symbols
in a single scan, then applies them all afterwards so a symbol can't be
affected mid-scan. Every `snapshot_interval` it bumps `next_snapshot` and
emits `Rotate` if a file rotation is due. Every tick it calls
`spread_snapshots`. Every `stats_interval` it logs the summary line.

The counters are deliberately split so that `updates_written` equals the
number of source deltas equals the writer's update count, and
`snapshots_written` equals every `Output::Snapshot` (periodic, source
switch, retirement) equals the writer's snapshot count. Both equalities are
what proves the pipeline at shutdown.
## Usage

### Build

```bash
cargo build --release
```

Binary: `target/release/engineering-challenge`.

### Collect

```bash
cargo run --release polymarket
```

Streams Polymarket order books to `book.sbe`. Stop with `Ctrl-C`; final
snapshots are emitted and the file is closed cleanly.

```bash
cargo run --release polymarket --output my-book.sbe    # custom file
cargo run --release polymarket --output-dir ./caps/    # rotate per minute
cargo run --release polymarket --output - > book.sbe   # stdout
```

### Dump

```bash
cargo run --release dump book.sbe                      # full text dump
```

### Configuration

Every flag has an env var. Precedence: **CLI > env > default**.

| Flag | Env | Default |
|---|---|---|
| `--output` | `EC_OUTPUT` | `book.sbe` |
| `--output-dir` | `EC_OUTPUT_DIR` | — |
| `--replicas` | `EC_REPLICAS` | `2` |
| `--symbols-per-connection` | `EC_SYMBOLS_PER_CONNECTION` | `200` |
| `--discovery-interval-secs` | `EC_DISCOVERY_INTERVAL_SECS` | `60` |
| `--snapshot-interval-secs` | `EC_SNAPSHOT_INTERVAL_SECS` | `60` |
| `--recycle-minutes` | `EC_RECYCLE_MINUTES` | `30` |
| `--connects-per-second` | `EC_CONNECTS_PER_SECOND` | `5.0` |

```bash
EC_REPLICAS=3 cargo run --release polymarket
```

### Logs

Logs go to **stderr**; SBE data owns **stdout**.

```bash
RUST_LOG=engineering_challenge=info    # default
RUST_LOG=engineering_challenge=debug   # per-replica detail
```
# Read before getting started!!

First, read [src/main.rs](https://github.com/voluntas-io/engineering-challenge/blob/main/src/main.rs). Then, some background on this repo:

This repo uses the [FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for writing binary messages. The rust code abstracts this away for you, and you can simply use the helpers, as demonstrated in `main.rs`.

### More background, in case you are curious!

FIX SBE is defined in XML, which I find extremely cumbersome to edit, especially
when updating schemas frequently. To solve this, I have also introduced some
helper code that takes our own custom YAML format, and turns it into FIX SBE
XML, which in turn is used to generate rust code to encode and decode the
messages. **You don't need to edit any of these files**. This explanation is
simply to help you understand the origins of the various files in this repo.

If you're curious to see the YAML schema, take a look at [challenge_orderbook.yaml](https://github.com/voluntas-io/engineering-challenge/blob/main/sbe_schemas/engineering_challenge/challenge_orderbook.yaml).
We also use a custom timestamp field, so that the Rust struct can use
`chrono::DateTime` in the generated struct, and in SBE schema, it is a uint64
representing the timestamp as the number of microseconds since unix epoch.
