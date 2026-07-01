# ClickHouse Native RowBinary Write Sink — Software Design

**Status:** implemented, tested against ClickHouse 26.x (Testcontainers) and validated end-to-end for the
diamond serving load.
**Branch:** `feat/clickhouse-native-sink` (fork).
**Scope:** the connector's *write* path only (INSERT / CREATE TABLE AS SELECT / MERGE-insert). The read
path, type mapping DDL, and pushdown are unchanged except where explicitly noted.
**Audience:** maintainers of the in-house `trino-clickhouse` connector.

---

## 0. TL;DR

**What this is.** A drop-in replacement for the connector's *write* path that streams rows to ClickHouse
in its native **RowBinary** format using the **ClickHouse Java Client V2**, instead of the JDBC
`PreparedStatement.executeBatch()` loop. Same SQL, same DDL, same types — just a faster, steadier writer.

**How the native client works.** ClickHouse Java **Client V2** (`com.clickhouse.client.api.Client`) exposes
a streaming insert: `client.insert(table, columns, InputStream, RowBinary)`. You hand it an `InputStream`;
it uploads the bytes to ClickHouse's HTTP interface as a **single continuous request** and the server
decodes them directly into the table. **RowBinary** is ClickHouse's compact binary row format: values are
written back-to-back, positionally, in the table's column order, with a one-byte null flag in front of each
`Nullable(T)` value — no column names, no SQL text, no server-side `VALUES` parsing. We encode each Trino
page straight into that byte stream.

**How it differs from the JDBC path.**

| | JDBC path (`JdbcPageSink`) | Native path (this design) |
|---|---|---|
| Wire protocol | SQL `INSERT … VALUES` + parameter binding | RowBinary byte stream |
| Cadence | accumulate `write_batch_size` rows → **synchronous** `executeBatch()` → repeat | **one continuous** streaming insert for the whole sink |
| Pipelining | none — reader idles while a batch flushes (sawtooth) | full — encoder and HTTP upload run concurrently across a pipe |
| Server work | parse `VALUES`, bind params, convert | decode RowBinary directly (near-zero parse) |
| Write memory | O(`write_batch_size` × row width) per writer × dbt threads | O(1 MiB pipe + one page), constant |
| Tuning knob | `write_batch_size` (throughput/memory tension) | none needed |

**Pros**
- ~1.7× faster than *tuned* JDBC, and **steady** throughput (no sawtooth stalls).
- **Constant, bounded write memory**, independent of `write_batch_size` and table size — removes the
  batch-size vs worker-memory tuning tension that caused prod stalls at 1M.
- Compact wire format; ClickHouse does ~no parsing → CPU stays idle for real work.
- Reuses the existing connector wholesale (types, DDL, MERGE plumbing) — small, contained change.

**Cons / costs**
- **Partial type coverage** — covers 100% of prod gold's types (verified; §5.4), but not a full superset:
  JSON/IP/`time` fail fast if ever introduced.
- We now **own the RowBinary encoding** and must keep its null-flag layout byte-exact against the DDL
  (`getColumnDefinitionSql`) — a correctness burden the JDBC driver otherwise carried.
- Depends on **Client V2** (bundled in the shaded `clickhouse-jdbc:all` jar; a driver bump must keep it).
- One extra `system.columns` round-trip at sink open (negligible; §11).

**The numbers** (local ClickHouse, relative; the ceiling is prod-node-independent):

| Path | Throughput | Note |
|---|---|---|
| JDBC `write_batch_size=1000` (old default) | ~0, blips 2k–14k rows/s | broken default |
| JDBC `write_batch_size=500k` (tuned) | ~815K–1.16M rows/s | **the primary fix — one property, ~60–80×** |
| **Native RowBinary sink** | ~1.38M rows/s (60M whale) | +~1.7× on tuned JDBC, no sawtooth |
| pure in-DB `INSERT … SELECT` | ~8.7M rows/s | **unreachable ceiling** (no Trino/JDBC) |

Head-to-head, same tables: lineitem 6M → native **5.2s** vs JDBC-500k **8.7s** (~1.68×); whale 60M →
native **43.4s** vs **73.6s** (~1.70×). Batch-size sweep (§9): JDBC throughput is flat 200k→2M while peak
heap rises with batch size, so **do not push `write_batch_size` past 500k**.

**Bottom line.** `write_batch_size` tuning was the real win and is free. The native sink is worth keeping
**only because we already maintain this fork for ClickHouse type support** — it then adds the last ~1.7×,
kills the sawtooth, and makes write memory constant, at near-zero marginal cost. It is **not** a path to the
8.7M rows/s ceiling (that needs in-DB ingest, which is out of scope; §10).

---

## 1. Problem statement

The stock `trino-clickhouse` connector is built on `trino-base-jdbc`. Writes go through
`io.trino.plugin.jdbc.JdbcPageSink`, which accumulates rows into a `PreparedStatement` via `addBatch()`
and flushes with a **synchronous** `executeBatch()` every `write_batch_size` rows, over the
clickhouse-jdbc HTTP driver.

Loading the diamond serving layer (a ClickHouse shadow of the Iceberg gold tables, written
Trino → ClickHouse) exposed two problems with this path:

1. **Pathological default throughput.** With the default `write_batch_size = 1000`, each flush is a full
   HTTP round-trip, so per-batch overhead dominates. Observed: **~0, blipping to 2k–14k rows/s** — a
   1.9M-row table would not finish in 120s.
2. **No read/write pipelining even when tuned.** While `executeBatch()` blocks, the Trino reader produces
   nothing (worker parallelism drops to 0.00), then bursts on the next accumulation. The result is a
   **sawtooth** that never sustains throughput, and at very large batches (1M) the synchronous stalls got
   *worse*, not better.

### What we measured (and what actually fixed it)

Two numbers that are easy to conflate:

| Path | Throughput | What it measures |
|---|---|---|
| Iceberg → ClickHouse, JDBC `write_batch_size=1000` (default) | ~0, blips 2k–14k rows/s | the broken default |
| Iceberg → ClickHouse, JDBC `write_batch_size=500k` (tuned) | ~815K–1.16M rows/s | **the real fix — one session property** |
| Native RowBinary sink (this design) | ~1.38M rows/s (60M-row whale) | tuned JDBC + streaming |
| Pure in-DB `INSERT … SELECT` (no Trino, no JDBC) | ~8.7M rows/s | an **unreachable ceiling** |

- The often-quoted **~43×** is `default-JDBC` vs `pure-in-DB` — an unreachable ceiling, *not* a target for
  any Trino sink. It quantifies how bad the default is and how idle ClickHouse is, nothing more.
- **`write_batch_size` tuning (1000 → 500k) recovered ~60–80× on its own, with no code change.** That is
  the primary fix and it ships as a catalog/session property.
- The native sink adds a **further ~1.7×** on top of *tuned* JDBC (whale 60M: 43.4s vs 73.6s; lineitem 6M:
  5.2s vs 8.7s) **and removes the sawtooth** (steady throughput, worker parallelism stays > 0).
- A follow-up batch-size sweep confirmed JDBC throughput is **flat from 200k upward** (~1.1–1.23M rows/s at
  200k/500k/1M/2M) while **peak client-side heap rises with batch size** — so pushing `write_batch_size`
  past 500k buys nothing and costs Trino-worker memory. See §9.

### Why build the sink at all, given tuning did most of the work

The native sink is justified **only** because we already maintain this fork for ClickHouse type support
(timestamp(6), array, map, row/Tuple, row-level delete). Given that, the sink rides along at near-zero
marginal maintenance cost and provides:

- the last ~1.7× and, more importantly, **steady (non-sawtooth) throughput** for the whale tables
  (10M–172M rows) that dominate the refresh SLA;
- **constant, bounded write memory** independent of `write_batch_size` (see §4.3), removing the
  batch-size/worker-memory tuning tension entirely on the write side.

It is **not** justified as a way to approach the 8.7M rows/s ceiling — that requires bypassing Trino/JDBC
(in-database `icebergS3()` ingest), which would re-implement dbt's incremental logic outside dbt and is
explicitly out of scope (see §10, Non-goals).

---

## 2. Goals and non-goals

### Goals
- Replace the JDBC `PreparedStatement.executeBatch()` write path with a **streaming RowBinary insert** via
  the ClickHouse **Java Client V2**, so page production and HTTP upload pipeline instead of stalling.
- Preserve the connector's existing external contract: same DDL, same type mapping, same
  INSERT/CTAS/MERGE semantics, same page-sink-id / fault-tolerant-execution behavior. A caller (dbt, SQL)
  should see identical results, only faster.
- Bounded, `write_batch_size`-independent write memory.
- Support exactly the types the diamond gold tables use: the scalar set, `Decimal`, `timestamp(p)`,
  `timestamp(p) with time zone`, `varbinary`/`varchar`/`char`, `Array`, `Map`, and `Row` (→ ClickHouse
  `Tuple`), with correct `Nullable(...)` handling at every nesting level.

### Non-goals
- **Not** native-S3 / `icebergS3()` ingest. That bypasses Trino and dbt entirely and re-implements
  incremental/merge logic; explicitly rejected (see §10).
- **Not** physical row deletion. MERGE stays an INSERT-only upsert into a ReplacingMergeTree (see §6).
- **Not** a full type superset. The supported set covers 100% of prod gold (§5.4), but JSON/IPv4/IPv6/`time`
  and other slice-mapped-to-String types are **not** supported and fail fast. They remain writable via the
  JDBC path if ever needed.
- **No changes to the read path** beyond what the type work (separate commits) already landed.

---

## 3. Key design decisions

| # | Decision | Rationale | Alternatives rejected |
|---|---|---|---|
| D1 | **Streaming single INSERT per sink**, not batched | Eliminates per-batch round-trips and the synchronous stall; gives read/write pipelining | Keep JDBC batching (the problem); async_insert (server-side buffering, weaker delivery guarantees) |
| D2 | **RowBinary format**, encoded by us | Most compact, positional, zero server-side parsing; matches Client V2's streaming `insert(table, cols, stream, RowBinary)` | RowBinaryWithNamesAndTypes (larger, redundant — we already know the schema); Values/CSV (parse cost) |
| D3 | **`PipedOutputStream` → `PipedInputStream` + a dedicated insert thread** | Client V2's `insert(...)` drains the `InputStream` **synchronously on the calling thread**; a dedicated thread is mandatory to let `appendPage` and the HTTP upload run concurrently. The pipe applies natural backpressure | Buffer the whole table in memory (unbounded); write to a temp file then upload (disk + no pipelining) |
| D4 | **Derive the HTTP endpoint from the existing `connection-url`** | Single source of truth for host/port/TLS; no new catalog property | A separate `native-endpoint` property (config drift, another thing to get wrong) |
| D5 | **Nullability = read from `system.columns`, fall back to the connector's own DDL rule** | The RowBinary null-flag prefix must match the created table's `Nullable(T)` exactly; `system.columns` is ground truth for an existing table, and the DDL rule (`isNullableElement`) is ground truth when the table isn't visible yet (CTAS) | Query `system.columns` only (breaks CTAS — see §7); trust handle metadata only (handle carries no nullability — see §7) |
| D6 | **Insert target = `getTemporaryTableName().orElse(finalTable)`** | CTAS/FTE stage rows into a temp table that `finishInsert`/`finishCreateTable` renames; must match `BaseJdbcClient.buildInsertSql` | Always use the final table name (breaks CTAS with `UNKNOWN_TABLE` — see §7) |
| D7 | **MERGE = discard deletions, insert insertions** into a ReplacingMergeTree | dbt-trino's `merge` never emits a delete clause; a matched row is overwritten and dedup'd on read via `FINAL` | Real MERGE / row-level delete (ClickHouse mutations are async & heavy; not what dbt needs) |
| D8 | **Bind the native provider via `OptionalBinder.setBinding()`** over the base `JdbcPageSinkProvider` | Overrides the default sink without forking the module graph; MERGE plumbing (all-columns `$merge_row_id`, etc.) from the base module still applies | A new `@ForClickHouse` provider + manual rewiring (more surface, more drift) |
| D9 | **Reuse `clickhouse-jdbc:all` (shaded)**, which bundles Client V2 | Client V2 (`com.clickhouse.client.api.*`) is already on the classpath via the shaded driver we depend on; no new dependency | Add `client-v2` as a separate dependency (duplicate classes, version-skew risk) |

---

## 4. Architecture

### 4.1 Component overview

```
Trino engine
  └─ ConnectorPageSinkProvider  (ClickHousePageSinkProvider  extends JdbcPageSinkProvider)
        ├─ createPageSink(OutputTableHandle)  ─┐   (CREATE TABLE AS SELECT)
        ├─ createPageSink(InsertTableHandle)  ─┤──► ClickHouseNativePageSink   (INSERT / CTAS)
        └─ createMergeSink(MergeTableHandle)   ─┘        │
              └─ ClickHouseMergeSink ── wraps ──────────►┘   (MERGE-insert; discards deletes)

ClickHouseNativePageSink
  appendPage(Page) ──encode RowBinary──► PipedOutputStream ═══ pipe (1 MiB) ═══► PipedInputStream
                                                                                     │
                                              dedicated daemon thread ──────────────►│
                                              client.insert(table, cols, in, RowBinary)  ──HTTP──► ClickHouse
```

### 4.2 Modules / files changed on the branch

| File | Change | Role |
|---|---|---|
| `ClickHouseNativePageSink.java` | **new (~563 lines)** | The streaming RowBinary sink: endpoint derivation, nullability resolution, per-type RowBinary encoders, the pipe + insert thread, `appendPage`/`finish`/`abort` lifecycle |
| `ClickHousePageSinkProvider.java` | **rewritten** | Overrides both `createPageSink` overloads and `createMergeSink` to build the native sink; injects `BaseJdbcConfig` (for `connection-url`) and `CredentialProvider` |
| `ClickHouseMergeSink.java` | modified (~12 lines) | Takes a `ConnectorPageSink insertSink` (the native sink) instead of constructing a `JdbcPageSink`; unchanged INSERT-only upsert semantics |
| `ClickHouseClient.java` | modified (~150 lines, incl. the separate type commits) | DDL nullability rule (`getColumnDefinitionSql`), `isClickHouseNonNullableContainer`, ROW↔Tuple read/write mapping — the ground truth the sink mirrors |
| `ClickHouseClientModule.java` | 1 line | `newOptionalBinder(...).setBinding().to(ClickHousePageSinkProvider.class)` |
| `TestClickHouseNativeSinkCtas.java` | **new (~156 lines)** | End-to-end CTAS/INSERT/ROW coverage (see §8) |

### 4.3 Threading, backpressure, and memory model

- `appendPage` runs on the Trino driver thread; it encodes each row of the page directly into
  `PipedOutputStream` (`pipeSink`).
- A single dedicated **daemon** thread (`clickhouse-native-insert-<pageSinkId>`, from
  `newSingleThreadExecutor`) runs `client.insert(table, columns, pipeSource, RowBinary).get()`. Client V2's
  `insert` is synchronous, so this call blocks the thread for the whole upload — which is exactly why it
  must be off the driver thread.
- The `PipedInputStream` has a **1 MiB buffer** (`PIPE_BUFFER_SIZE`). When the encoder outruns the HTTP
  upload, `PipedOutputStream.write` blocks → **backpressure** propagates to `appendPage` naturally. When
  the upload outruns the encoder, the insert thread blocks reading the pipe. Neither side buffers the
  whole table.
- **Memory is O(pipe buffer + one page), independent of `write_batch_size` and of table size.** This is
  the structural advantage over JDBC, whose in-flight buffer is O(`write_batch_size` × row width) per
  writer and multiplies by dbt thread count.

---

## 5. RowBinary encoding

### 5.1 The invariant

RowBinary is **positional and schema-less on the wire**: the server decodes bytes against the target
table's declared column types, in insert-column order. Therefore the sink's byte layout **must** match the
created-table DDL exactly — in particular the `Nullable(T)` null-flag prefix. The DDL is produced by
`ClickHouseClient.getColumnDefinitionSql`; the sink mirrors its rule (see §5.3).

### 5.2 Per-type encoders

`valueEncoder(Type)` returns a `ValueEncoder` (a functional interface `encode(out, block, position)`),
recursively for containers. Mapping (all via `com.clickhouse.data.format.BinaryStreamUtils`):

| Trino type | ClickHouse type | Encoding |
|---|---|---|
| `boolean` | `Bool` | `writeBoolean` |
| `tinyint`/`smallint`/`integer`/`bigint` | `Int8/16/32/64` | `writeInt8/16/32/64` |
| `real`/`double` | `Float32/64` | `writeFloat32/64` |
| `decimal(p,s)` | `Decimal(p,s)` | `writeDecimal` (short → from `long`; long → from `Int128.toBigInteger()`) |
| `char`/`varchar` | `String` | `writeString(slice.getBytes())` |
| `varbinary` | `String` | `writeString(raw bytes)` — CH `String` is an arbitrary byte string |
| `uuid` | `UUID` | `writeUuid(trinoUuidToJavaUuid(slice))` — CH's 16-byte UUID order via the same helper as `uuidWriteFunction` |
| `date` | `Date` | `writeDate(LocalDate.ofEpochDay(days))` |
| `timestamp(p)` | `DateTime64(p)` | `writeDateTime64` in UTC (`fromTrinoTimestamp` / `fromLongTrinoTimestamp`) |
| `timestamp(p) with time zone` | `DateTime64(p)` | normalize to `Instant`, `writeDateTime64` in UTC |
| `array(T)` | `Array(T)` | `writeVarInt(count)` then each element |
| `map(K,V)` | `Map(K,V)` | `writeVarInt(size)` then each key,value pair |
| `row(f…)` | `Tuple(f…)` | fields back-to-back, **no** count/length prefix |

### 5.3 Nullability at every level (`ColumnEncoder` + `isNullableElement`)

A `ColumnEncoder(ValueEncoder, boolean nullable)` wraps a value encoder with the null-flag protocol:

- if `nullable`: emit `0x00` (non-null) or `0x01`+nothing (null) via `BinaryStreamUtils.writeNonNull` /
  `writeNull`, then the value when non-null;
- if `!nullable`: a null in the block is a hard error (`checkState`) — writing a null flag would desync the
  stream against a non-Nullable column.

`isNullableElement(type)` is the single rule shared by top-level columns and nested elements/fields:

> a value is `Nullable(T)` **iff it is a scalar** — i.e. `!(ArrayType || MapType || RowType)`.

This mirrors `ClickHouseClient.isClickHouseNonNullableContainer` / `clickHouseElementDataType`: ClickHouse
does not allow `Nullable(Array(...))`, `Nullable(Map(...))`, or `Nullable(Tuple(...))`, so containers are
always emitted bare; scalars are wrapped. **Map keys are never Nullable** (`keyEncoder` uses
`nullable=false` unconditionally).

### 5.4 Supported-type coverage and the unsupported tail

**The supported set (§5.2) covers 100% of the live prod `nvmap.gold` schema** — verified by scanning
`nvmap.information_schema.columns` (115 tables, 2778 columns, 34 distinct types): every type resolves to an
encoder, including the complex ones (`array(row(...))`, `map(boolean, array(varchar))`,
`map(varchar, array(bigint))` multimaps, `varbinary` WKB, `decimal(38,24)`, and — after this change —
`date` (5 cols) and `uuid` (1 col)).

JSON, IPv4/IPv6, `time`, and any other type the connector maps to a ClickHouse `String`/other via a slice
are **still out of scope** (none appear in gold). `valueEncoder` throws
`TrinoException(JDBC_ERROR, "…does not support column type: …")` rather than silently corrupting the
positional stream. If such a column ever appears, the load fails loudly and we extend the encoder
deliberately (the DATE/UUID additions are the template: one `valueEncoder` case + the matching
`BinaryStreamUtils` writer + a round-trip test).

---

## 6. MERGE semantics (`ClickHouseMergeSink`)

Scoped to what dbt-trino's incremental `merge` strategy needs: an **upsert into a ReplacingMergeTree**.

- dbt-trino's `merge` emits only `WHEN MATCHED THEN UPDATE` / `WHEN NOT MATCHED THEN INSERT` — **never** a
  delete clause. A matched row is "effectively deleted" by being overwritten and later collapsed by the
  engine; reads use `FINAL` (applied only to engines that support it) so the latest version wins.
- Trino's merge protocol delivers, per row, an insert-row (for INSERT and the insert half of an UPDATE)
  and/or a delete-row (for DELETE and the delete half of an UPDATE). This sink **appends every insert-row
  and discards every delete-row**, delegating the appends to the wrapped native insert sink.
- An explicit `WHEN MATCHED … THEN DELETE` (only from hand-written SQL, never from dbt) is **rejected** —
  physical row deletion is intentionally unsupported.

The wrapped sink is a full `ClickHouseNativePageSink`, so MERGE inserts get the same streaming RowBinary
path (and the same CTAS/temp-table correctness) as plain INSERT.

---

## 7. Key issues encountered and how they were resolved

### 7.1 CTAS failure — the central bug (two facets)

The first end-to-end diamond build failed on `CREATE TABLE AS SELECT`. Root-caused to two distinct
problems, both stemming from the same wrong assumption ("the sink writes to the final table, and it already
exists"):

**Facet A — `UNKNOWN_TABLE` on insert.** The sink issued `client.insert()` against
`handle.getRemoteTableName()` (the *final* table). But `BaseJdbcClient.buildInsertSql` targets
`getTemporaryTableName()` when present: base-jdbc's CTAS (and fault-tolerant INSERT) stage rows into a
**temporary table** that `finishCreateTable`/`finishInsert` later renames. At sink-open the final table
does not exist yet → `Code: 60 … Table … does not exist`.
**Fix:** insert into `getTemporaryTableName().orElse(remoteTableName.getTableName())`, matching base-jdbc
exactly (decision D6).

**Facet B — `No columns found in system.columns`.** The nullability resolver queried
`SELECT name, type FROM system.columns WHERE database=? AND table=?` and asserted the result was non-empty
(`checkState(!nullableByName.isEmpty(), …)`). During a CTAS the target isn't visible in `system.columns`
when the sink opens, so the assert threw.
**Fix:** (1) query the *effective* (temp) table, and (2) **fall back to the connector's own DDL rule**
(`isNullableElement`) for any column not found — which is exactly the `Nullable(...)` decision
`getColumnDefinitionSql` just made (an output column is nullable in Trino, so scalars → `Nullable`,
containers → bare). No hard failure; the RowBinary null-flags still match the created table (decision D5).

**Why not read nullability off the handle instead of `system.columns`?** Investigated and rejected:
`JdbcOutputTableHandle` carries only `getColumnNames()` + `getColumnTypes()` — **no per-column
nullability**. So `system.columns` (for existing tables) + the DDL-rule fallback (for not-yet-visible
tables) is the correct pairing.

### 7.2 Client V2 `insert` is synchronous
Discovered that `client.insert(...)` drains the `InputStream` on the calling thread and only returns once
the upload completes. Running it inline would deadlock against `appendPage` writing the same pipe. Resolved
with the dedicated daemon insert thread (decision D3).

### 7.3 Endpoint derivation from a JDBC URL
`connection-url` is `jdbc:clickhouse://host:port/db[?params]`; Client V2 wants an HTTP(S) endpoint. The
sink strips the `jdbc:` prefix, parses the `URI`, infers TLS from the scheme
(`clickhouses`/`https` → 8443, else 8123), and rebuilds `http(s)://host:port`. Credentials come from the
`CredentialProvider` (per-identity), not the URL.

### 7.4 `Nullable` sort key (surfaced by tests, not a sink bug)
An early test used `order_by = ARRAY['id']` where `id` is `Nullable(Int64)`; ClickHouse rejects a nullable
sort key when `allow_nullable_key` is off (`Code: 44`). This is table-property behavior, not a sink
concern; tests use `ORDER BY tuple()` (the connector's default when `order_by` is unset), which is also
what the diamond unkeyed models emit.

---

## 8. Testing

### 8.1 Test harness
`TestClickHouseNativeSinkCtas` boots a real in-process Trino via `ClickHouseQueryRunner` (which installs
`ClickHousePlugin` + `TpchPlugin`) against a **Testcontainers ClickHouse** (`CLICKHOUSE_LATEST_IMAGE`), so
every case exercises the *actual* native sink code path, not a mock.

Cases:
1. `testCreateTableAsSelectScalars` — bare CTAS of scalar columns (the original failing path).
2. `testCreateTableAsSelectMultiRow` — CTAS of ~63K rows from `tpch.tiny.orders`.
3. `testCreateTableAsSelectWithNulls` — CTAS with NULLs, asserting the nullability fallback is correct.
4. `testInsertIntoExistingStillWorks` — INSERT into a pre-existing table (regression guard for the
   non-temp-table path).
5. `testCreateTableAsSelectRowType` — CTAS of a `row(a integer, b varchar)` → `Tuple(...)` column, proving
   the container-is-bare rule at CTAS time.
6. `testCreateTableAsSelectDate` — CTAS of a `date` → `Date` column with a NULL, exercising the
   `Nullable(Date)` null-flag path (the 5 gold `date` columns).
7. `testCreateTableAsSelectUuid` — CTAS of a `uuid` → `UUID` column, asserting byte-exact round-trip via the
   string form (the 1 gold `uuid` column).

### 8.2 The failing-first → fixed story (TDD-style record)

The tests were written to reproduce the reported CTAS failure, then iterated to green. Each intermediate
failure taught something and is preserved here because it documents *why* the fix is shaped the way it is.

| Iteration | Symptom | Diagnosis | Change |
|---|---|---|---|
| 0 (baseline, no fix) | Whole class errors in `@BeforeAll`: `No columns found in system.columns for default.tpch.nation` while `copyTpchTables` runs its `CREATE TABLE … AS SELECT` | The connector was **unusable for any fresh-table build** — even the test fixtures couldn't load | (this is the bug we're fixing) |
| 1 | `Code: 44 … Sorting key contains nullable columns` on all cases | Test fixture used `order_by = ARRAY['id']` with a `Nullable` key | Switch test tables to `ORDER BY tuple()` (§7.4) |
| 2 | 3/5 cases: `Code: 60 … Table … does not exist (UNKNOWN_TABLE)` at insert time | **Facet A** — sink inserted into the final table, not the temp table | Route insert through `getTemporaryTableName().orElse(...)` (D6) |
| 2 | `does not support column type: date` (multi-row case) | Test selected `orderdate` (DATE); at the time the sink had no DATE encoder | Test change: select `orderstatus` (DATE later added as a first-class encoder — see below) |
| 3 | `testInsertIntoExistingStillWorks`: `mismatched column types: Table [bigint, varbinary], Query [integer, varchar(1)]`; `testCreateTableAsSelectScalars`: `expected "a" but was [97]` | Connector maps CH `String` ↔ Trino `varbinary`; test assumed `varchar` round-trip | Test change: use `varbinary` literals / assert bytes (`containsExactly('a')`) — validates the *sink*, not the read mapping |
| 4 | **all 5 pass** (`Tests run: 5, Failures: 0, Errors: 0`) | — | done |

**Proof the fix is what mattered:** on the clean baseline the connector smoke test cannot even complete
`@BeforeAll` (it fails copying TPCH fixtures with the exact `No columns found in system.columns` error).
With the fix, setup succeeds and 29 additional smoke-test methods run. The 6 remaining smoke-test failures
(`testShowCreateTable` expecting engine `LOG` vs the branch default `MERGETREE`; `testMerge`,
`testRowLevelUpdate`, and the `verifySupports{Delete,RowLevelUpdate,RowLevelDelete}Declaration` capability
checks) are **pre-existing, deliberate branch decisions** (`DEFAULT_TABLE_ENGINE = MERGETREE` for dbt
delete+insert; INSERT-only merge) and are unrelated to the write sink — they were simply invisible before
because setup crashed.

### 8.3 Running the tests locally

Testcontainers over Colima needs Ryuk disabled and the socket pointed at Colima:

```bash
export JAVA_HOME=<jdk-25>
export TESTCONTAINERS_RYUK_DISABLED=true
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
./mvnw -q -pl plugin/trino-clickhouse test -Dair.check.skip-all=true \
    -Dmaven.source.skip=true -Dtest=TestClickHouseNativeSinkCtas -DfailIfNoTests=false
```

### 8.4 Type round-trip validation (from the ROW-support commit)
Verified end-to-end (write DDL → native INSERT → Trino read → raw ClickHouse) for
`row(double,double)`, `row(varchar,varchar)`, `row(decimal(18,4), bigint)` (decimal-in-row), and
`array(row(varchar,double))` (nested array-of-row) — exact values in all cases.

---

## 9. Performance notes and tuning guidance

- **On the JDBC path, keep `write_batch_size = 500k`; do not push higher.** A controlled sweep (6M rows,
  clickhouse-jdbc 0.9.8, mirroring `JdbcPageSink`'s loop) showed throughput flat from 200k upward
  (~1.1–1.23M rows/s at 200k/500k/1M/2M) while peak client-side heap rose with batch size (transient
  spikes ~0.5–1.25 GB). Bigger batches buy **0%** throughput and cost Trino-worker memory, multiplied by
  dbt thread count — the exact pressure behind the observed sawtooth-to-0 at 1M. If anything, 200k–300k is
  a safer operating point on a memory-constrained worker.
- **The native sink is insensitive to `write_batch_size`** — it ignores the property (one streaming insert,
  1 MiB pipe). Write memory is constant regardless (§4.3).
- **Aggregate throughput scales with concurrency, not batch size.** ClickHouse is ~90% idle during
  ingestion (CPU ~8–25% of 16 cores, memory ~3–5% of 96 GiB); a single table can't pipeline the whole
  node, but N concurrent dbt threads fill the headroom. Do **not** add ClickHouse CPU/memory/replicas for
  ingestion.

---

## 10. Alternatives considered (and why rejected)

| Alternative | Verdict | Reason |
|---|---|---|
| **Just tune `write_batch_size`** and keep JDBC | Adopted as the *primary* fix; kept for the JDBC path | Recovers ~60–80×; but caps at ~1.1M rows/s with sawtooth stalls and batch-size/memory tuning tension |
| **`async_insert`** on the JDBC path | Rejected | Server-side buffering changes delivery/visibility semantics; still per-batch on the client |
| **In-database `INSERT … SELECT FROM icebergS3(...)`** | Rejected (out of scope) | Only path to ~8.7M rows/s, but bypasses Trino *and* dbt — would re-implement dbt's incremental/merge logic outside dbt. A last resort, not a plan |
| **Stage Trino → RowBinary/Parquet on S3, then CH `INSERT … FROM s3(...)`** | Rejected (out of scope) | Same dbt-bypass problem; adds an object-store staging hop |
| **RowBinaryWithNamesAndTypes** | Rejected | Larger payload; redundant — the sink already knows the exact schema |
| **Read nullability from the output-table handle** | Not possible | `JdbcOutputTableHandle` carries no per-column nullability (§7.1) |

---

## 11. Reuse vs. rewrite, and native/JDBC coexistence

### 11.1 This is a surgical replacement, not a new connector

**We reuse the existing `trino-clickhouse` connector and gut out only the write sink.** Everything else —
the JDBC connection factory, type mapping and DDL (`ClickHouseClient`), metadata, predicate/pushdown, the
read path, table properties, session properties, MERGE plumbing — is untouched connector code that the
native sink *depends on* (it mirrors `getColumnDefinitionSql` for nullability, reuses `RemoteTableName`,
`JdbcOutputTableHandle`, `CredentialProvider`, `BaseJdbcConfig`, etc.).

Concretely, the only write-path substitution is one Guice line:

```java
newOptionalBinder(binder, ConnectorPageSinkProvider.class)
        .setBinding().to(ClickHousePageSinkProvider.class).in(Scopes.SINGLETON);
```

and `ClickHousePageSinkProvider extends JdbcPageSinkProvider` — so the base JDBC sink is still fully
constructible; we simply return the native sink from the three `create*Sink` overrides. Net change is 5
files / ~929 lines (§4.2), most of which is the RowBinary encoder table and tests. A **from-scratch
connector** was never on the table: it would duplicate thousands of lines of correct type/DDL/pushdown code
for a write-path change, and diverge on every upstream fix.

### 11.2 Can native and JDBC coexist behind a config knob? — Yes, cleanly

Because `ClickHousePageSinkProvider` already `extends JdbcPageSinkProvider` and holds everything needed to
build **either** sink, a toggle is a small, low-risk addition. Recommended shape:

1. **A config property** on `ClickHouseConfig`, following the existing `clickhouse.use-final` idiom:

   ```java
   private boolean nativeWrite = true;                    // default on for this fork

   @Config("clickhouse.native-write.enabled")
   @ConfigDescription("Use the native RowBinary write sink instead of the JDBC batch sink")
   public ClickHouseConfig setNativeWriteEnabled(boolean value) { this.nativeWrite = value; return this; }

   public boolean isNativeWriteEnabled() { return nativeWrite; }
   ```

2. **A session-property override** on `ClickHouseSessionProperties` (existing `booleanProperty` idiom), so
   it can be flipped per-query without a restart — useful to A/B or to fall back for one problematic load:

   ```java
   booleanProperty("native_write_enabled",
       "Use the native RowBinary write sink instead of the JDBC batch sink",
       config.isNativeWriteEnabled(), false);
   ```

3. **The provider branches** in each `create*Sink`:

   ```java
   return ClickHouseSessionProperties.isNativeWriteEnabled(session)
           ? createNativePageSink(session, (JdbcOutputTableHandle) tableHandle, pageSinkId)
           : super.createPageSink(transactionHandle, session, tableHandle, tableCredentials, pageSinkId);
   ```

   The `else` path is the **unmodified base `JdbcPageSink`**, which still honors `write_batch_size`.

**Why this is safe and worth having:**
- **Per-query granularity** — one whale can run native while everything else stays on JDBC, or vice-versa,
  no redeploy.
- **Instant fallback** — if the native sink ever hits an unsupported type or a driver regression, set
  `native_write_enabled=false` for that model and the load still completes on JDBC.
- **A/B benchmarking in one deployment** — flip the session property between runs on the same catalog.

**Caveats:**
- **MERGE always needs the connector's INSERT-only semantics** (`ClickHouseMergeSink`, targeting
  ReplacingMergeTree). When the knob selects JDBC, the merge sink should wrap a base `JdbcPageSink` rather
  than the native one — the `ClickHouseMergeSink` already takes a `ConnectorPageSink insertSink`, so this is
  just choosing which sink to pass; no semantic change.
- **Type-coverage divergence** — a table the native sink rejects (§5.4) writes fine on JDBC. The knob makes
  this a per-load choice instead of a hard failure, which is a feature, but it means "does this load work?"
  now depends on the flag. Document the supported-type set (§5.2) alongside the property.
- **Keep one code path warm** — if we default native-on and rarely exercise JDBC, JDBC bit-rot is a risk.
  The coexistence tests should cover both settings for at least the core INSERT/CTAS/MERGE cases.

**Recommendation on the knob:** add it. It is cheap (one config prop, one session prop, a branch in three
methods), it de-risks rollout (instant fallback), and it lets us keep the JDBC path as the reference
implementation. Default `native-write.enabled = true` on this fork; document `false` as the escape hatch.

---

## 12. Recommendation: native vs. JDBC

**Primary recommendation, unchanged:** the highest-leverage fix is the **`write_batch_size = 500k` session
property** on the existing JDBC path — one line, no code, ~60–80× over the broken default, and it alone
loaded ~87/88 diamond tables. If you do nothing else, do that. (Do **not** push `write_batch_size` past
500k; §9.)

**On top of that, adopt the native sink — with the coexistence knob (§11.2), defaulting native-on.** Take:

- **Adopt native because** we are already maintaining the fork for ClickHouse type support, so the sink's
  marginal cost is low, and it delivers (a) +~1.7× throughput, (b) **steady throughput with no sawtooth**,
  and (c) **constant write memory** — the last point matters most operationally, because it removes the
  batch-size↔worker-memory tuning tension that caused the prod stalls at large batches.
- **Keep JDBC reachable via the knob because** it is the reference implementation, covers the full type set,
  and is the instant fallback if the native path ever misbehaves on a specific load or driver version.
- **Do not** pursue in-DB `icebergS3()` ingest for throughput (§10): it is the only path to the ~8.7M
  rows/s ceiling but abandons Trino and dbt's incremental logic — a last resort, not a plan.

**Net:** JDBC-tuned is the floor and the safety net; native-on-by-default is the operating mode; the ceiling
is deliberately left on the table.

---

## 13. Benchmark environment and expected behavior on a real server

**Where the numbers came from — a memory-tight laptop, not a server.** All relative throughput numbers in
this doc were measured on a single developer laptop (Apple silicon, 12 logical cores, 64 GB), inside a
**Colima** Linux VM capped at **24 GB RAM** (CPUs left at Colima's default = all host cores). Critically,
**ClickHouse and up to two Trino processes were co-resident in that one VM**, contending for the same cores
and the same 24 GB. So each engine saw only a fraction of an already-small machine, and native/JDBC runs
were executed **sequentially** to avoid OOM. These are **relative** comparisons (native vs JDBC under
identical constraints), **not** absolute capacity numbers, and there was significant run-to-run variance.

**Why a real server should widen the native advantage, not shrink it:**

- **CPU headroom for pipelining.** On the laptop, the Trino encoder thread and the ClickHouse ingest thread
  competed for the same cores, so the native sink's read/write *pipelining* — its core advantage — was
  partly serialized. On prod's **16-core / 96 GB** ClickHouse node (observed ~2–4 cores and ~3–4 GB used
  during ingest — i.e. ~75–90% idle), the encoder and the upload genuinely overlap, and multiple dbt
  threads can drive several concurrent streaming inserts into the idle capacity. The JDBC path can't use
  that headroom the same way because it stalls synchronously between batches (worker parallelism → 0.00).
- **Memory stops being the limiter.** The native sink's **constant** write memory (§4.3) means adding cores
  and RAM converts almost directly into more concurrent table loads. The JDBC path's memory grows with
  `write_batch_size` × writers, so on a big node you're still forced to trade batch size against worker
  heap — the very tension the native sink removes. More RAM helps native scale *out* (more concurrent
  loads); it only lets JDBC push a bigger batch that (per §9) buys no throughput.
- **Network/HTTP amortization.** A single sustained RowBinary upload amortizes connection and compression
  setup over the whole table; JDBC re-incurs per-batch request overhead. On a fast server NIC this favors
  the streaming path further.

**Expectation (not yet measured on prod):** the ~1.68–1.70× seen on the constrained laptop is a
**conservative floor**. On the 16-core/96-GB node with dbt running several threads, expect the *aggregate*
diamond-load wall-clock advantage to be larger than 1.7×, driven mostly by concurrency filling the idle
ClickHouse capacity — while the native sink keeps per-writer memory flat. **This should be confirmed with a
prod-node benchmark** (same table mix, native-on vs native-off via the §11.2 knob) before quoting a prod
number; the laptop figures should be cited only as relative, constrained-environment results.

---

## 14. Risks and open items

- **Type coverage matches prod gold today (§5.4), but is not a full superset.** A future diamond column of
  an unsupported type (JSON/IP/`time`) fails the load loudly; extend `valueEncoder` deliberately when that
  happens (the DATE/UUID additions are the worked example).
- **`system.columns` lookup adds one round-trip at sink open.** Negligible vs the insert, and skipped-effect
  when the fallback fires; acceptable.
- **Client V2 lives inside the shaded `clickhouse-jdbc:all` jar (D9).** A future driver bump must keep
  Client V2 (`com.clickhouse.client.api.*`) present; a build-time check or an explicit `client-v2` dep is a
  possible hardening if the shading ever changes.
- **This connector stays on the fork.** It is not proposed upstream; the design assumes we own the
  maintenance and the ClickHouse-version matrix we test against.
