# miniwaldb 🪵💥

Tiny embedded database in C++, goal is: **correctness under crashes**.

## What it is
- A mini DB project (eventually SQL-ish)
- Built in **WSL2/Linux** with **CMake + Ninja**
- Tested with **Catch2**
- Comes with a little REPL: `miniwaldb_shell`

## Cool part
**WAL-first durability.**  
**Crash-safe transactions** using **Write-Ahead Logging (WAL)**:
- recovery can rebuild state even if you messed it up
## Current 
- WAL records are framed and CRC-checked, golden-byte + torn-tail tested.
- Supports optional sync-on-commit (`fdatasync`/`fsync`) with deterministic hook-based tests.
- Startup recovery replays only committed transactions and stops safely at truncation/corruption.
