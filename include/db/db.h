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
  explicit Db(std::string dir);

  // Starts a new transaction. Throws if a transaction is already active.
  void begin();

  // Commits the active transaction by appending a COMMIT record to the WAL.
  // Throws if no transaction is active.
  void commit();

  // Aborts the active transaction by appending an ABORT record to the WAL.
  // Throws if no transaction is active.
  void abort();

  // Saves the current in-memory state to a durable snapshot and resets the WAL.
  // Throws if called while a transaction is active.
  void checkpoint();

  // Stores `value` under `key`.
  // If a transaction is active, the change is also recorded in the WAL.
  // `key` is the integer key to update.
  // `value` is the string payload to store for that key.
  void put(std::int64_t key, std::string value);

  // Removes `key` from the database.
  // If a transaction is active, the delete is also recorded in the WAL.
  // `key` is the integer key to erase.
  void erase(std::int64_t key);

  // Looks up `key` in the current in-memory state.
  // Returns the stored string if present, otherwise `std::nullopt`.
  // `key` is the integer key to read.
  std::optional<std::string> get(std::int64_t key) const;

private:
  // Root directory that owns this database instance's files.
  std::string dir_;

  // Current in-memory key/value state after snapshot load and WAL replay.
  std::unordered_map<std::int64_t, std::string> kv_;

  // Full path to the durable snapshot file used by checkpoint/load.
  std::string snapshot_path_;

  // Full path to the write-ahead log file.
  std::string wal_path_;

  // WAL writer used for appending transactional records during runtime.
  std::unique_ptr<wal::WalWriter> wal_writer_;

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

  // Replays committed WAL records from `wal_path_` into `kv_`.
  void recover_from_wal_();
};

} // namespace miniwaldb
