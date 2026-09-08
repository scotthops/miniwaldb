#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <memory>
#include "wal/wal_writer.h"

namespace miniwaldb {

class Db {
public:
  // Opens or creates a database rooted at `dir`.
  // `dir` is the filesystem directory that holds snapshot and WAL files.
  // Optional hooks are for deterministic tests; defaults use real POSIX I/O.
  explicit Db(std::string dir, wal::SyncHook sync_hook = {}, wal::WriteHook write_hook = {});

  // Starts a transaction with a working copy of committed state.
  // Throws if a transaction is already active.
  void begin();

  // Appends and syncs a COMMIT record, then publishes the transaction's working state.
  // Throws if no transaction is active.
  void commit();

  // Appends an ABORT record and discards the transaction's working state.
  // Throws if no transaction is active.
  void abort();

  // Saves committed state to a durable snapshot and resets the WAL.
  // Throws if called while a transaction is active.
  void checkpoint();

  // Stores `value` under `key`.
  // Requires an active transaction; logs the change and updates its working copy.
  // `key` is the integer key to update.
  // `value` is the string payload to store for that key.
  void put(std::int64_t key, std::string value);

  // Removes `key` from the database.
  // Requires an active transaction; logs the delete and updates its working copy.
  // `key` is the integer key to erase.
  void erase(std::int64_t key);

  // Reads the working copy during a transaction, or committed state otherwise.
  // Returns the stored string if present, otherwise `std::nullopt`.
  // `key` is the integer key to read.
  std::optional<std::string> get(std::int64_t key) const;

private:
  // Root directory that owns this database instance's files.
  std::string dir_;

  // Committed state only. Snapshot loading, recovery, and commit update this map.
  std::unordered_map<std::int64_t, std::string> kv_;

  // Private working copy for the active transaction; empty when idle.
  std::unordered_map<std::int64_t, std::string> working_kv_;

  // Full path to the durable snapshot file used by checkpoint/load.
  std::string snapshot_path_;

  // Full path to the write-ahead log file.
  std::string wal_path_;

  // WAL writer used for appending transactional records during runtime.
  std::unique_ptr<wal::WalWriter> wal_writer_;

  // A persistence failure blocks every public operation until destruction/reopen.
  bool persistence_error_{false};
  wal::SyncHook sync_hook_;
  wal::WriteHook write_hook_;

  void check_usable_() const;
  void open_wal_writer_();
  void append_wal_(const wal::WalRecord& record);

  // True while a transaction is currently open.
  bool in_tx_{false};

  // Next transaction id to assign on `begin()`.
  wal::TxId next_txid_{1};

  // Transaction id of the currently active transaction, or 0 if none.
  wal::TxId current_txid_{0};

  // Ensures the database directory exists on disk.
  void ensure_dir_();

  // Loads snapshot state from `snapshot_path_` into `kv_`.
  void load_snapshot_();

  // Validates and replays committed WAL records; repairs incomplete EOF tails before append.
  void recover_from_wal_();
};

} // namespace miniwaldb
