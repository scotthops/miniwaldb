# miniwaldb API Notes

This is the current public-ish surface area of `miniwaldb`: small, sturdy, and increasingly difficult to break.

The project is still early, so this is more a field guide than a grand constitutional document. Covers what exists today, how the pieces fit together, and what each part promises.

## Db

Header: [include/db/db.h](/home/scott/projects/miniwaldb/include/db/db.h)

`miniwaldb::Db` is the main database object. It owns:
- the in-memory key/value state
- the snapshot file path
- the WAL file path
- the WAL writer used for runtime changes

Construction:

```cpp
explicit Db(std::string dir);
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
```

What they do:
- `begin()`: starts a transaction and writes a `Begin` WAL record.
- `commit()`: writes a `Commit` WAL record and finishes the active transaction.
- `abort()`: writes an `Abort` WAL record and finishes the active transaction.
- `checkpoint()`: writes the current in-memory state to `snapshot.dat`, then resets `wal.log` to empty.
- `put(key, value)`: updates the in-memory map and, if a transaction is open, writes a `Set` WAL record.
- `erase(key)`: removes a key from the in-memory map and, if a transaction is open, writes a `Delete` WAL record.
- `get(key)`: returns the current value for a key, or `std::nullopt` if the key is absent.

Current behavior notes:
- `checkpoint()` throws if called during an open transaction.
- recovery is redo-only: committed work comes back, uncommitted work does not.
- startup recovery is automatic; there is no separate `open()` call at the moment.
- the in-memory state is the live source of truth while the process is running.

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
- `Commit`: transaction is durable enough to replay later
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
                   SyncHook sync_hook = {});
```

Parameters:
- `path`: WAL file to append to
- `flush_on_commit`: whether `flush_on_commit()` actually performs a sync
- `sync_hook`: optional injected sync function; defaults to real `fdatasync`/`fsync`

Public methods:

```cpp
Lsn append(const WalRecord& rec);
void flush();
void flush_on_commit();
```

Behavior:
- `append(rec)`: encodes a framed WAL record and appends it to disk
- `flush()`: placeholder for broader flush semantics later
- `flush_on_commit()`: if enabled, syncs the WAL file descriptor to disk

The writer keeps an open POSIX file descriptor. It writes using `::write()` in a loop, which is a sober and respectable way to avoid partial-write surprises.

## WalReader

Header: [include/wal/wal_reader.h](/home/scott/projects/miniwaldb/include/wal/wal_reader.h)

Constructor:

```cpp
explicit WalReader(std::string path);
```

Public method:

```cpp
std::vector<WalRecord> read_all();
```

Behavior:
- reads the WAL sequentially
- validates framing and CRC
- stops at the first truncated or corrupt tail record
- returns only the valid prefix of records

This is intentional. When the tail looks suspicious, the reader does not try to be clever. It simply bows out with dignity and preserves the last known-good state.

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
2. replay committed WAL records from `wal.log`
3. ignore uncommitted or aborted transactions
4. stop replay at the first torn or corrupt tail record

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
- no rollback of already-mutated in-memory state during an active aborted transaction
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
