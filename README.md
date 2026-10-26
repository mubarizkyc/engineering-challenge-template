# Polymarket Order Book Collector

A Rust binary that streams the full Polymarket CLOB order book (every live
symbol, redundant websockets, per-replica books) and writes snapshots plus
deltas to an aligned SBE file.

---
## Project

### Structure

```
src/
  main.rs        — CLI, tracing, dispatch
  collector.rs   — wires writer + engine + supervisor, owns shutdown order
  supervisor.rs  — discovery, sharding, connection lifecycle
  connection.rs  — one websocket task: connect, keepalive, recycle, backoff
  polymarket.rs  — Gamma discovery + CLOB decoder
  engine.rs      — per-symbol source selection, health, snapshots
  output.rs      — dedicated writer thread (AlignedSbeWriter)
  dump.rs        — offline replay + verification
  util/          — SBE types, aligned writer/reader, DTOs
```

`main`, `dump`, `output` are scaffolding. The real work is in `supervisor`,
`connection`, `polymarket`, `engine`.

### Redundant websockets

I wanted the system to keep producing if any single socket died, without a
central coordinator.

- Each shard is up to 200 symbols, carried by N independent connections.
- **Each connection owns its own `OrderBookManager`.** This was the biggest
  decision — sharing one manager across all sockets was my first instinct,
  but it meant every apply had cross-socket lock contention, and a desync on
  one socket poisoned the shared book. Separate books was ~40 lines more
  wiring and removed a whole class of bug.
- One replica is the **source**; its deltas are written, others stay hot but
  silent. Picking the source: eligible → fresh → low-latency → recent. On
  switch, emit a snapshot from the new source's book, because they may
  differ.

### Subscription monitoring

The hard part wasn't detecting failure — it was detecting failure *without
false positives*. My first version desynced on every locked book (bid ==
ask) because I trusted polyfill's `is_valid()`, which uses strict `<`. That
was the whole reason my desync counters kept spiking in early runs. Fixing
it required writing my own `is_crossed` (strict `>`) and remembering to use
it everywhere.

What the health check actually does every 1s:

- Never-synced replica → retry after `resync_timeout`.
- Replica silent while siblings active → desync + resubscribe at 60s.
- All replicas quiet for far longer than that symbol's usual gap → rotate
  one replica to probe (never the source while another exists).
- Latency reselect: only if a sibling is 2s faster, with 30s hysteresis.
- `resubscribe_backoff` prevents tight loops — a desync sets
  `resubscribe_pending`, and the actual command only fires once backoff
  elapsed.

### polymarket_client_sdk_v2

Only used for discovery. `GammaClient::events()` with
`active/closed/limit/offset` — nothing else.

**The filter is the fix.** `enable_order_book && accepting_orders &&
!closed && (liquidity > 0 || volume > 0)`. That last clause is why
`covered == symbols` holds. Polymarket serves "ghost" markets with zero
liquidity and zero volume; the WS feed never sends them a snapshot, so
subscribing left them permanently uncovered. Excluding them at discovery
removed the problem — dropped symbol count from ~4140 to ~2372, and
`uncovered` from 2 to 0.

**422 is not an error.** Gamma caps offset pagination with a 422 saying "use
/events/keyset." My first version treated it as failure, which set
`complete = false`, which meant removals never fired, which meant closed
markets stayed subscribed forever. The fix is one branch: on 422, `break`
and leave `complete = true`.

### polyfill-rs

High-performance book library. Fixed-point ticks (`Price = u32` at
1/10,000), sharded `RwLock<HashMap<..>>`, sorted `Vec` per side so
top-of-book is `levels[0]`.

How I use it:

- **Snapshots** → `apply_book_update(&BookUpdate)`. Validates everything
  before mutating; replaces both sides atomically via generation counters.
- **Deltas** → `with_book_mut(symbol, |b| { for e in batch {
  b.apply_delta(...)?; } Ok(..) })`. One lock per asset-batch, not per
  entry. `size == 0` removes. Rejects stale sequences — I feed a monotonic
  counter per decoder, since Polymarket has no usable sequence.
- **Reads** → `bids_fast` / `asks_fast` / `best_bid_fast` /
  `best_ask_fast` return fixed-point structs — no `Decimal` allocation, no
  re-sort, straight from the sorted vector.

Why it matters: `~5ns` per operation versus `~100ns` for a Decimal-based
book. The wins are integer compares vs `Decimal::Ord`, sorted `Vec` vs
`BTreeMap` (top-of-book cache-local), and generation-based snapshot
replacement (no full scan to clear stale levels).

Not used: `set_tick_size` (Polymarket prices are always 4-decimal exact —
`decimal_to_price_exact` already rejects garbage). `BookAnalytics` (no
metrics surface yet).

### Shortcomings — honest list

**I built a generic decoder first, then tore it out.** I wrote a `Decoder`
trait with associated error/book types, thinking "second exchange someday."
In practice the trait had one implementor and 30 lines of `where` clauses.
It obscured the actual Polymarket logic behind an abstraction for a future
that hasn't arrived. I spent a full session confused by my own API before
deleting it. The current `PolymarketDecoder` is concrete, half the size,
twice as legible. **Premature abstraction was my biggest waste of time.**

**I trusted the library's validator.** `polyfill::OrderBook::is_valid()` uses
strict `<`, so locked books (`bid == ask`) return false. Locked books happen
constantly in binary markets during tight consensus. I spent hours chasing
phantom desyncs before noticing the semantics. Fixing this — writing my own
`is_crossed` with strict `>` — was the single highest-impact fix in the
whole project. **Read the library's docs; don't assume a method named
"is_valid" means what you think it means.**

**I over-trusted exchange-reported TOB.** My first decoder desynced when
the exchange's `best_bid`/`best_ask` on a `price_change` didn't match my
post-delta book. Structurally wrong: the exchange samples TOB at a
different moment than we apply the batch. This caused a false-desync storm
on every reconnect. The fix is to log mismatch at `debug!` and never
desync on it — the book we built is authoritative.

**Thresholds were tuned wrong.** `latency_switch_ms = 50` caused a
1,100-switch-per-window storm because 50ms of EWMA jitter is noise, not
signal. `idle_timeout = 30s` caused reconnect churn on quiet symbols.
`resubscribe_backoff = 10s` caused retries before the exchange had a chance
to send anything. Each was a small number that felt safe and wasn't.

**No tests.** I have `dump --quiet` as a file-level check, but no unit
tests for the engine, no decoder tests, no integration tests for the
writer. My confidence in correctness comes from logs, not from asserted
behaviour. **This is the biggest gap between my submission and a
production-grade one.**

### What's actually special

Three things, in order of how much they mattered:

1. **Per-connection books.** Removes cross-socket contention, isolates
   desyncs, makes source-switch clean (each replica has its own consistent
   view). Costs ~40 lines more wiring; eliminates a whole bug class.
2. **Deterministic snapshot spreading.** FNV-1a hash of symbol % slots
   phases each symbol's periodic snapshot. A 2,400-symbol set becomes ~10
   snapshots per 250ms tick instead of 2,400 at one boundary. Costs nothing
   at write time.
3. **Shutdown-time proof.** `EngineStats.updates_written` must equal
   `WriterStats.updates`; same for snapshots. If they don't, a message was
   lost between engine and writer. Two counter equalities prove the whole
   pipeline at process exit.

### Latency and throughput

- polyfill fixed-point book → no `Decimal` allocation on the delta path.
- Batch lock per asset in the decoder → one `with_book_mut`, not one per
  entry.
- Snapshot spreading → no writer-queue burst on the 60s boundary.
- Aligned SBE writer thread → 64 MiB buffer, one `write` syscall per minute
  at steady state. Off-runtime so disk I/O never blocks a socket read.
- EWMA latency per connection, used only for source reselection, never on
  the hot path.
- Threshold discipline: `latency_switch_ms = 2000`, `min_switch_interval =
  30s`, `replica_silence = 60s`. All tuned down from what felt right after
  watching the switch-storm logs.

### Polymarket module

`Polymarket` — discovery + WS entry. `new()`, `discover()` (paginated
`/events` → `{symbols, complete}`), `fetch_events_page(offset)`,
`decoder()` (fresh `PolymarketDecoder` with a fresh `OrderBookManager`),
`connect_request()`.

`PolymarketDecoder` — per-connection WS decoder. State: `books: Arc<
OrderBookManager>`, `synced: HashSet<String>`, `sequence: u64`.

- `handle_book`: on first snapshot for a symbol on this connection, reset
  the book's stale-check guards (`last_snapshot_timestamp_ms`,
  `last_delta_sequence`, `sequence`) so a reconnect can reload the same
  symbol. `apply_book_update`. Verify `loaded_ts != 0` (update actually
  landed, not silently rejected) and not crossed. On success: mark synced,
  emit Snapshot. On any failure: remove from synced, emit Desync.
- `handle_price_change`: group by asset (one message can carry YES and NO).
  Skip unsynced. Skip if `pc_msg.timestamp < last_snapshot_timestamp_ms`
  (would double-count). One `with_book_mut` per asset — apply all entries,
  then read crossed state and TOB. Emit Delta per entry. TOB mismatch is
  `debug!` only.
- `is_crossed` — strict `>`. Locked is normal in binary markets and is not
  corruption.

### Engine module

`Engine` — per-symbol state machine.

State: `books_by_conn`, `symbols` (per-symbol replicas, source, activity/
gap EWMAs), `conns` (up, latency EWMA, command sender), `stats`.

`handle(msg)`:
- `Feed` → dispatch each event.
- `ConnUp` → mark up, mark all this-connection replicas as `synced =
  false` (fresh subscription implied).
- `ConnDown` → mark down, drop replica sync, reselect source if it was
  the source. Does **not** drop the book Arc — the connection reuses it.
- `Register` → store book manager + command sender.
- `AddSymbols` → create `SymbolState`, cache `snapshot_hash` (FNV-1a).
- `RemoveSymbols` → if the symbol had a source, emit an **empty retirement
  snapshot**, then remove.

`handle_event`:
- `Snapshot` → mark replica synced; if source, re-emit; if backup and no
  eligible source, `select_source`.
- `Delta` → update latency EWMA; if source, update `mean_gap` and write
  `Output::Update`; if backup and `source.is_none()`, `select_source`.
- `Desync` → unsync, set `resubscribe_pending`, `resubscribe`, reselect
  source if it was the source.

`tick(now, wall, queue_len)`:
- Every 1s: `check_health` — collect desyncs/resubscribes/reselections/
  uncovered in one scan, apply after so a symbol can't be affected
  mid-scan.
- Every snapshot_interval: bump `next_snapshot`, emit `Rotate` if due.
- Every tick: `spread_snapshots`.
- Every stats_interval: log summary.

Counter design: `updates_written` = source deltas = writer updates;
`snapshots_written` = every Snapshot (periodic + switch + retirement) =
writer snapshots. Both equalities hold at shutdown — that's the pipeline
proof.

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
