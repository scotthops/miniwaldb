# miniwaldb 🪵💥

Tiny embedded database in C++, goal is: **correctness under crashes**.

## What it is
- A small fixed-schema relational database implemented over a key/value storage engine
- Built in **WSL2/Linux** with **CMake + Ninja**
- Tested with **Catch2**
- Comes with a little REPL: `miniwaldb_shell` (relational commands and explicit transactions)

## Cool part
**WAL-first durability.**  
**Crash-safe transactions** using **Write-Ahead Logging (WAL)**:
- recovery can rebuild state even if you messed it up
## Current 
- WAL records are framed and CRC-checked, golden-byte + torn-tail tested.
- Supports optional sync-on-commit (`fdatasync`/`fsync`) with deterministic hook-based tests.
- Startup recovery replays committed transactions, truncates and syncs incomplete EOF tails before appending, and refuses complete-but-corrupt WAL data.
- Supports transactional `DELETE` records and redo recovery for committed deletes.
- Has a simple snapshot/checkpoint path: `snapshot.dat` loads first on startup, WAL replays on top, and checkpoint resets WAL after saving a durable snapshot.
- Snapshot writes are atomic via temp-file + rename + directory sync, with tests covering replacement and restart behavior.

## Relational scope

`ItemsTable` presents one fixed table: `items(id INTEGER PRIMARY KEY, value TEXT)`.
It supports distinct insert/update/delete operations, row lookup, scans sorted by
ID, exact-value selection, and typed ID/value projections. Duplicate inserts and
updates/deletes of missing rows fail. Mutations require an explicit `Db` transaction.

The table wraps the existing engine; it uses the same working copy, WAL, snapshots,
and recovery. There is no second persistence format. The C++ table API and relational REPL are covered by Catch2 tests.

The educational scope is one table, two columns, a signed 64-bit integer primary
key, and non-null text (including empty strings). There is no SQL, joins, arbitrary
schemas, query optimizer, or extra indexing. Projections preserve order and duplicate
values; they do not implement `DISTINCT`. See [API notes](docs/API.md) for examples.

## Terminal demo

Build with `cmake --build build`, then run `./build/miniwaldb_shell`.
The database lives in `./dbdata` relative to the current working directory.
This is a small command syntax, not SQL. Use a fresh database for this example.

```text
> begin
ok
> insert 1 apple
ok
> insert 2 blue
ok
> commit
ok
> scan
1 | apple
2 | blue
> begin
ok
> update 2 dark blue
ok
> commit
ok
> select-value dark blue
2 | dark blue
> exit
```

Run the program again from the same directory; `get 2` returns `2 | dark blue`.

Commands: `begin`, `commit`, `abort`, `insert ID TEXT`, `update ID TEXT`,
`delete ID`, `get ID`, `scan`, `select-value TEXT`, `ids`, `values`, `checkpoint`,
`help`, `quit`, and `exit`. `put` has been replaced by distinct insert/update commands.
`checkpoint` saves committed state and is rejected inside a transaction.

IDs are signed 64-bit decimal integers (optional minus, no plus sign). Numeric
suffixes and overflow are errors. TEXT consumes the rest of the line: leading
separator whitespace is removed, while internal and trailing spaces are retained.
There is no quoting or escape syntax; quote characters are literal. The shell
requires non-whitespace text, so empty values are available only through the C++ API.
Selection uses exact case-sensitive text equality. Scans and projections follow ID
order; projections preserve duplicate values.

Malformed commands and ordinary API errors print `error:` and leave the session
running. Persistence failures print a fatal error and exit with status 1; startup
corruption also fails without opening a prompt. Quit, exit, and EOF return status 0
and never commit pending changes: the database closes, and restart ignores the
transaction without a Commit record. There are no confirmation prompts.

## Producer-consumer workload

```sh
./build/miniwaldb_workload         # 2 producers, 100 keys each, queue capacity 64
./build/miniwaldb_workload 25 1    # 2 producers, 25 keys each, capacity 1
```

Each producer submits an insert and lookup for each of its disjoint keys, so the
default run attempts 400 requests. One consumer owns the database and executes
complete mutation transactions. Producers never access Db. The queue blocks on
condition variables when full/empty, and closes to drain accepted work on normal
shutdown. Consumer failures close the queue and wake producers before all threads
are joined.

Each run prints its fresh temporary database directory (separate from `./dbdata`),
request counts, maximum queue depth, elapsed time, throughput and verification.
The directory is retained for inspection. Exit status is 0 only when request IDs
and reopened database contents verify successfully. Timing includes verification;
this is a concurrency exercise, not a performance benchmark. See
[workload design](docs/WORKLOAD.md) for invariants, failure behavior and test coverage.

## Standalone NAND simulator

```sh
./build/miniwaldb_flash_demo
```

This independent in-memory model demonstrates Free/Valid/Invalid pages, programming
only Free pages, whole-block erasure, logical-to-physical mappings, out-of-place updates,
and garbage collection that moves live pages before erasing a victim. It does not
replace database WAL or snapshot storage.

The demo prints physical pages, mappings and counters before/after automatic GC,
then verifies every current value. Geometry defaults to 8 blocks × 8 pages; the demo
uses 3 × 4 for readability. Each page holds a string. GC requires enough free pages
outside its victim for relocation and reports out of space rather than losing data.
See [flash model and invariants](docs/FLASH.md) for the allocation policy, counters,
example trace and deliberate differences from real SSDs.

## Sequential versus random flash writes

```sh
./build/miniwaldb_flash_compare
```

This runs fresh 8×8 simulators with a common 32-page population, then sequential
cycling or seeded random updates (seed 12345), for 1,000 successful writes including
setup. It reports physical programs, GC copies, erases, page-program write
amplification and final block/page usage. Incomplete runs are explicitly flagged.

The current default run measured sequential WA **1.000** (1,000 physical programs)
and random WA **1.451** (1,451 programs), with 118 and 176 block erases respectively.
WA divides by **successful** logical writes, not rejected attempts. These are
simulator-specific page counts, not real SSD timing or byte amplification. See
[comparison method, results and limitations](docs/FLASH_COMPARISON.md).
