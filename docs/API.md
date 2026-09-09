# miniwaldb API Notes

This is the current public-ish surface area of `miniwaldb`: small, sturdy, and increasingly difficult to break.

The project is still early, so this is more a field guide than a grand constitutional document. Covers what exists today, how the pieces fit together, and what each part promises.

## Fixed-schema relational API

The logical table is `items(id INTEGER PRIMARY KEY, value TEXT)`. Physical storage
remains `Db`'s integer/string map. `ItemsTable` is a non-owning wrapper: its `Db`
must outlive it. Multiple wrappers over the same `Db` view the same single table,
not separate tables. Transaction ownership stays on `Db`.

Header: [include/table/items_table.h](../include/table/items_table.h)

| Operation | Result and rules |
|---|---|
| `insert(id, value)` | Add a row; duplicate ID throws |
| `update(id, value)` | Replace an existing value; missing ID throws |
| `erase(id)` | Delete an existing row; missing ID throws |
| `lookup(id)` | `optional<ItemRow>`; absent ID returns `nullopt` |
| `scan()` | `vector<ItemRow>` sorted by ascending ID; both columns |
| `select_value(value)` | Exact, case-sensitive text equality; matching rows sorted by ID |
| `project_ids(rows)` | `vector<int64_t>` containing only IDs |
| `project_values(rows)` | `vector<string>` containing only values |

Rows have concrete `id` and `value` fields. Text may be empty; neither column is
nullable. Projection functions work on scans or selections and preserve input order
and duplicates, like SQL SELECT without DISTINCT. They have no storage side effects.
Returning rows already provides both columns; no runtime column-selection enum is needed.

All relational mutations delegate to `Db::put` or `Db::erase` after checking row
existence via `Db::get`. Thus they require an active transaction and use the same
WAL and commit/recovery path. Constraint checks occur first: a duplicate/missing-row
error can precede the no-transaction error. Constraint errors do not end a transaction.
Reads see working state during a transaction and committed state otherwise. Abort
discards relational changes just as it discards storage changes. Persistence errors
also block table operations, including scans.

`Db::entries()` is the only new engine primitive: it checks usability and copies
visible entries in unspecified order. The table converts those entries to rows and
sorts them. Results are detached copies, so callers cannot modify the engine through
returned data. Scan costs O(n log n) time and O(n) result space; selection currently
uses that sorted scan for straightforward control flow. No additional index is built.

```cpp
miniwaldb::Db db("./dbdata");
miniwaldb::ItemsTable items(db);
db.begin();
items.insert(7, "blue");
items.insert(2, "blue");
db.commit();

auto row = items.lookup(7);                  // ItemRow{7, "blue"}
auto matching = items.select_value("blue"); // rows 2 and 7
auto ids = miniwaldb::project_ids(matching); // {2, 7}
auto values = miniwaldb::project_values(matching); // {"blue", "blue"}
```

On restart, construct a new `Db` for the same directory, then an `ItemsTable` over
it. Existing snapshot/WAL recovery reconstructs the same rows without any table-specific
recovery logic. The public low-level `put` remains an upsert; use `ItemsTable` when
insert-versus-update constraints matter.

Scope: one fixed table, two columns, integer primary key, text value, lookup/scan/
selection/projection. No SQL parser, joins, arbitrary schemas, catalogs, optimizer,
or indexes beyond the existing key lookup. The REPL exposes this table through insert/update/delete/get, scan, select-value,
ids, and values. Transaction commands and checkpoint call Db directly; see the
README for syntax and exit behavior.

## Db

Header: [include/db/db.h](/home/scott/projects/miniwaldb/include/db/db.h)

`miniwaldb::Db` is the main database object. It owns:
- the committed in-memory key/value state and a separate transaction working copy
- the snapshot file path
- the WAL file path
- the WAL writer used for runtime changes

Construction:

```cpp
explicit Db(std::string dir,
            wal::SyncHook sync_hook = {},
            wal::WriteHook write_hook = {});
```

`dir` is the database directory on disk. On construction, `Db`:
1. ensures the directory exists
2. loads `snapshot.dat` if present
3. replays `wal.log` on top of the snapshot
4. opens a WAL writer for future changes

Public methods:

```cpp
void begin();
void commit();
void abort();
void checkpoint();
void put(std::int64_t key, std::string value);
void erase(std::int64_t key);
std::optional<std::string> get(std::int64_t key) const;
std::vector<std::pair<std::int64_t, std::string>> entries() const;
bool has_persistence_error() const noexcept;
```

What they do:
- `begin()`: copies committed state into a transaction working map and writes a `Begin` WAL record. Nested transactions are rejected.
- `commit()`: writes a `Commit` WAL record, synchronizes the WAL, then swaps the working map into committed state and clears the old map.
- `abort()`: writes an `Abort` WAL record and discards the working map. Committed state is unchanged.
- `checkpoint()`: writes only committed state to `snapshot.dat`, then resets `wal.log` to empty.
- `put(key, value)`: requires an active transaction, writes a `Set` WAL record, then updates the working map.
- `erase(key)`: requires an active transaction, writes a `Delete` WAL record, then removes the key from the working map.
- `get(key)`: reads the working map during a transaction and committed state otherwise; returns `std::nullopt` if the key is absent.

Current behavior notes:
- `checkpoint()` throws if called during an open transaction.
- `put()`, `erase()`, `commit()`, and `abort()` throw if no transaction is active.
- aborted changes cannot enter a checkpoint: they only existed in the discarded working map.
- beginning a transaction copies the entire database, trading time and memory for simple rollback behavior.
- `Db` enables WAL sync-on-commit both at startup and after checkpointing. Optional hooks replace system calls for tests.
- after a WAL append/sync failure, normal operations throw until destruction/reopen. `has_persistence_error()` remains available for status inspection so the REPL can exit immediately.
- recovery is redo-only: committed work comes back, uncommitted work does not.
- startup recovery is automatic; there is no separate `open()` call at the moment.
- the in-memory state is the live source of truth while the process is running.

## Transaction and durability contract — in brief

- Mutations require an active transaction and append mutation WAL records before `Commit`.
- Successful commit appends `Commit`, synchronizes the WAL, publishes working memory, clears transaction state, then returns.
- Recovery redoes only complete, valid committed transactions. Without a complete `Commit`, a transaction is ignored; aborted work is discarded.
- Startup truncates an incomplete EOF suffix to the last valid frame before accepting new appends. Complete corruption is rejected, not silently repaired.
- An error or missing response does not prove a transaction is absent: a complete commit may still recover. A failed sync alone proves neither outcome.
- Persistence errors make the current instance unusable until destruction/reopen. There is no automatic rollback of uncertain persistence.
- Redo recovery is intended to be idempotent. Reopening valid state preserves both its logical contents and its snapshot/WAL bytes.
- The supported model is Linux/WSL process interruption, one database owner, and the filesystem sync contract. Process exit leaves the kernel running; unconditional hardware or power-loss durability is not claimed.

### Regression evidence

| Behavior | Catch2 regression |
|---|---|
| Incomplete frames, repair, continued writes, repeated repaired recovery | `Recovery repairs incomplete tails before subsequent commits` (length/header/payload/CRC/commit sections) |
| Failure before a complete commit | `Incomplete commit frame does not commit transaction and stops Db` |
| Complete commit despite observed failure | `Complete commit may recover after caller observes sync failure` |
| Successful sync followed by lost response and no destruction | `Process interruption after successful sync does not require graceful destruction` |
| Mixed snapshot/WAL state and unchanged files over repeated opens | `Mixed snapshot and WAL recovery is stable across repeated opens` |

Fault injection uses the existing `WriteHook` and `SyncHook`, plus deliberately
truncated fixtures. No additional commit stages or production code are needed.
The process test forks only after the parent's database is closed. In the child,
the sync hook calls real `fsync`, checks success, then calls `_exit` before returning
to `commit()`. Thus publication, a successful return, and C++ destruction cannot
occur. The parent checks the specific exit status with `waitpid` before reopening.
There are no sleeps or timing-dependent kill points. This demonstrates process
interruption, not loss of kernel caches or physical power.

## Commit persistence boundary

For a successful `Db::commit()`:

1. Mutation records have already been appended.
2. Append the complete `Commit` record.
3. Synchronize the WAL using `fdatasync`, with `fsync` fallback on `EINVAL`.
4. Swap the working map into committed state.
5. Clear the old map and transaction flags.
6. Return success.

The supported environment is Linux/WSL with one database owner and no concurrent
file modification. Production defaults require the OS/filesystem sync call to
report success before publishing memory. Hooks are testing seams; a fake hook
returning success does not prove physical durability.

`write()` can leave data in the kernel's cache. Process termination leaves the
kernel running; machine failure can lose that cache. Sync requests completion
through the filesystem's synchronization interface. This project does not claim
unconditional power-loss or hardware durability: device behavior, filesystem
semantics, and persistence of newly created directory entries are not fully
audited or crash-tested here. Destruction only closes the WAL; it does not commit.

A write or sync error makes the outcome uncertain. A complete commit may remain
readable even after sync reports failure. `Db` therefore records a persistence
error and rejects all public operations, including reads, abort, and checkpoint.
The failed instance does not append a compensating abort, retry the transaction,
or repair files. Startup handles incomplete EOF tails as described below.
Checkpoint I/O failures also stop the instance, as can an allocation failure while
updating working memory after its operation was logged. Ordinary invalid API
calls, such as committing without a transaction, do not stop the instance.

Destroy/reopen is required to inspect recovered state. Startup repairs a structurally
incomplete EOF suffix before opening the append writer, but refuses corruption.

Whole-file reads treat `ENOENT` as missing and report other open, seek, or short-read
failures. An existing empty snapshot still follows the existing empty-state
behavior; snapshot format/corruption policy is unchanged.

## WAL Types

Header: [include/wal/wal_writer.h](/home/scott/projects/miniwaldb/include/wal/wal_writer.h)

Core WAL aliases:

```cpp
using TxId = std::uint64_t;
using Lsn = std::uint64_t;
using SyncHook = std::function<int(int)>;
```

- `TxId`: transaction id used to group WAL records
- `Lsn`: append-order counter returned by the writer
- `SyncHook`: injectable durability hook used mainly for tests

Record kinds:

```cpp
enum class RecordType : std::uint8_t {
  Begin = 1,
  Set = 2,
  Commit = 3,
  Abort = 4,
  Checkpoint = 5,
  Delete = 6
};
```

What they mean:
- `Begin`: transaction starts here
- `Set`: key/value write
- `Commit`: complete transaction is eligible for replay; its presence alone does not prove that sync succeeded
- `Abort`: transaction should be discarded during recovery
- `Checkpoint`: reserved for future use
- `Delete`: key removal

Record container:

```cpp
struct WalRecord {
  RecordType type{};
  TxId txid{};
  std::vector<std::uint8_t> payload;
};
```

`payload` is record-specific:
- `Set`: `[int64 key][u32 value_len][value bytes]`
- `Delete`: `[int64 key]`
- `Begin`, `Commit`, `Abort`: empty payload today

## WalWriter

Header: [include/wal/wal_writer.h](/home/scott/projects/miniwaldb/include/wal/wal_writer.h)

Constructor:

```cpp
explicit WalWriter(std::string path,
                   bool flush_on_commit = false,
                   SyncHook sync_hook = {},
                   WriteHook write_hook = {});
```

Parameters:
- `path`: WAL file to append to
- `flush_on_commit`: whether `flush_on_commit()` actually performs a sync
- `sync_hook`: optional injected sync function; defaults to real `fdatasync`/`fsync`
- `write_hook`: optional function with the POSIX `write` signature and return/errno contract; defaults to `::write`

Public methods:

```cpp
Lsn append(const WalRecord& rec);
void flush();
void flush_on_commit();
```

Behavior:
- `append(rec)`: encodes a framed WAL record and appends it to disk
- `flush()`: syncs unconditionally, retrying `EINTR`
- `flush_on_commit()`: if enabled, syncs the WAL file descriptor to disk

The writer keeps an open POSIX file descriptor. Its write loop advances after short writes, retries `EINTR`, and throws on errors or zero progress. A successful append means all frame bytes were accepted by the OS; it does not itself perform a sync.

## WalReader

Header: [include/wal/wal_reader.h](/home/scott/projects/miniwaldb/include/wal/wal_reader.h)

Constructor:

```cpp
explicit WalReader(std::string path);
```

Public method:

```cpp
enum class ReadStopReason { CleanEof, IncompleteTail, Corruption };
struct WalReadResult {
  std::vector<WalRecord> records;
  std::size_t valid_bytes;
  ReadStopReason stop_reason;
};
WalReadResult read_all();
```

Behavior:
- reads the WAL sequentially
- validates framing and CRC
- returns decoded complete frames, the byte offset after the last valid frame, and a stop reason
- never changes the file
- reports clean EOF when all bytes form valid frames (even if a transaction has no commit)
- reports an incomplete tail if EOF cuts a length field or a frame body/CRC
- reports corruption for impossible frame lengths, contradictory header lengths, bad CRCs, or unknown complete record types

An available header is checked for consistent frame/payload lengths before treating
missing body bytes as an incomplete append. The frame-length prefix is not covered
by the CRC. If a damaged length claims bytes beyond EOF and there is no contradictory
header evidence, this format cannot distinguish corruption from interruption. The
simple policy classifies that case as incomplete; it does not search for later frames.

## WAL File Format

Each WAL record is written as:

```text
[u32 frame_len][record bytes][u32 crc32]
```

Where `record bytes` are:

```text
[u8 type][u64 txid][u32 payload_len][payload bytes]
```

Notes:
- integers are little-endian
- `frame_len` covers `type + txid + payload_len + payload`
- `crc32` is computed over those record bytes

This gives the reader enough structure to detect torn writes and stop safely at the ragged edge of misfortune.

## Snapshot Helpers

Header: [include/storage/file_io.h](/home/scott/projects/miniwaldb/include/storage/file_io.h)

Relevant types and functions:

```cpp
using KvSnapshot = std::unordered_map<std::int64_t, std::string>;

void save_snapshot(const std::string& path, const KvSnapshot& kv);
KvSnapshot load_snapshot(const std::string& path);
```

Snapshot format:

```text
[magic "MWS1":4][entry_count:u32][key:i64][value_len:u32][value bytes]...
```

Behavior:
- `save_snapshot(...)` writes a temp file, fsyncs it, renames it into place, then fsyncs the parent directory
- `load_snapshot(...)` loads the file into memory and validates the format
- missing snapshot file returns an empty state

Entries are written sorted by key so the format is deterministic. That makes tests calmer, diffing friendlier, and future debugging a touch less haunted.

## Startup Recovery Order

Today, opening a database follows this sequence:

1. load `snapshot.dat`
2. scan `wal.log`; refuse startup on framing/checksum corruption
3. validate every complete record payload before replay; refuse malformed payloads
4. replay committed transactions from the valid prefix; ignore aborted or unfinished transactions
5. for an incomplete EOF suffix only, truncate to `valid_bytes` and sync the truncated WAL
6. open the WAL writer for appending only after all previous steps succeed

SET payloads must contain an eight-byte key, a four-byte value length, and exactly
that many value bytes. DELETE payloads must be exactly eight bytes. Control records
must have empty payloads. Malformed records fail construction even if uncommitted
or orphaned; no partially replayed database object is exposed. Validation occurs
before truncation, so corruption errors leave the WAL unchanged.

`storage::truncate_file` opens the existing file without creating it, uses
`ftruncate`, then `fsync`, and checks open/truncate/sync/close errors. Interrupted
truncate/sync calls are retried. Any failure prevents construction from reaching
the append-writer step. A sync error may occur after the length has already changed;
startup still fails instead of claiming repair succeeded.

Syncing records the shortened file through the filesystem's synchronization
interface before further writes are accepted. This is a conservative ordering
choice, not a new unconditional power-loss guarantee. No rename occurs, so repair
itself does not create a directory entry. The existing single-owner, Linux/WSL,
filesystem and hardware assumptions still apply.

Example: with one-byte values, A's BEGIN/SET/COMMIT occupy 76 bytes. B's complete
BEGIN extends the valid prefix to byte 97. If B's next SET contains only its first
20 bytes, the file is 117 bytes long. Recovery restores A, ignores uncommitted B,
truncates to 97, and syncs. C's BEGIN starts at byte 97; C's complete transaction
adds 76 bytes. The next startup can reach C's COMMIT and restore A and C. Repeated
startup at clean EOF performs no truncation. Complete B records may remain in the
prefix; they stay unapplied without B's COMMIT.

Checkpointing then does the inverse dance:

1. save a durable snapshot of current in-memory state
2. reset `wal.log` to empty
3. reopen the WAL writer for new records

## Current Limits

The code is honest about its ambitions and its unfinished business.

What exists:
- single-process embedded DB shape
- transactional `put` and `erase`
- WAL-backed redo recovery
- snapshot/checkpoint support

What is still missing or deliberately simple:
- no SQL execution layer yet
- no separate persistent table file beyond snapshot/WAL
- no concurrent writer model
- transactions use a full working copy rather than an undo log
- no WAL checkpoint record semantics yet, even though the enum has a placeholder

## Practical Example

Typical use looks like this:

```cpp
miniwaldb::Db db("./dbdata");

db.begin();
db.put(1, "hello");
db.put(2, "world");
db.commit();

db.checkpoint();

auto value = db.get(1);
```

If the process later restarts, `Db("./dbdata")` reloads the snapshot, replays any newer committed WAL records, and picks up where it left off with a surprisingly cheerful amount of persistence for such a compact codebase.

## REPL implementation

`tools/shell.cpp` calls `run_shell("./dbdata", cin, cout, cerr)`. The implementation
in `tools/repl.cpp` contains a direct command loop and small helpers for IDs, final
text arguments, extra-argument rejection, and output. `run_shell` owns Db so return
also closes it; `run_repl` borrows Db for deterministic stream-based tests.

IDs are parsed as a whole token with `from_chars`; invalid suffixes and range errors
are rejected before table calls. Non-text commands reject extra tokens. Data commands
use ItemsTable, while begin/commit/abort/checkpoint call Db. Help and exit have no
storage effects. `get` prints a full row or `not found`; empty query results print
`(no rows)`; successful mutations and transaction commands print `ok`.

Ordinary errors go to the error stream and the loop continues. The Db persistence
status distinguishes fatal failures without matching exception text. Fatal errors
return 1. Normal quit/exit/EOF returns 0, without appending an automatic Commit or
Abort. Unfinished state disappears when the owning shell closes Db; recovery ignores
its uncommitted records. The existing failure model and recovery policy are unchanged.
