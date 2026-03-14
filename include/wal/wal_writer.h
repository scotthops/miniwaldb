#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace miniwaldb::wal {

// Transaction identifier used to group WAL records into a transaction.
using TxId = std::uint64_t;

// Logical sequence number returned by appends in write order.
using Lsn  = std::uint64_t;

// Optional durability hook used by tests and sync-on-commit behavior.
// The hook receives the writer's file descriptor and returns 0 on success.
using SyncHook = std::function<int(int)>;

enum class RecordType : std::uint8_t {
  // Marks the start of a transaction.
  Begin = 1,

  // Stores a key/value write operation in the WAL.
  Set   = 2,

  // Marks a transaction as committed and eligible for recovery replay.
  Commit= 3,

  // Marks a transaction as aborted and not replayable.
  Abort = 4,

  // Reserved for future checkpoint-related WAL records.
  Checkpoint = 5,

  // Stores a key deletion operation in the WAL.
  Delete = 6
};

struct WalRecord {
  // Kind of WAL record stored in this entry.
  RecordType type{};

  // Transaction id associated with this record.
  TxId txid{};

  // Record-specific bytes.
  // For example, `Set` encodes key/value data and `Delete` encodes a key.
  std::vector<std::uint8_t> payload; // keep flexible for now
};

class WalWriter {
public:
  // Opens or creates the WAL file at `path`.
  // `path` is the WAL file location on disk.
  // `flush_on_commit` enables sync-on-commit when `flush_on_commit()` is called.
  // `sync_hook` optionally overrides the real sync operation, mainly for tests.
  explicit WalWriter(std::string path,
                     bool flush_on_commit = false,
                     SyncHook sync_hook = {});

  // Closes the WAL file descriptor if it is open.
  ~WalWriter();

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  // Appends `rec` to the WAL using the framed on-disk format.
  // Returns the next logical sequence number for this writer.
  // `rec` is the record to encode and append.
  Lsn append(const WalRecord& rec);

  // Placeholder for general flush support outside commit-specific durability.
  void flush();          // fsync-ish later

  // Syncs the WAL to durable storage if sync-on-commit is enabled.
  void flush_on_commit(); // fsync on commit (placeholder for now)

private:
  // Open file descriptor for the WAL file, or -1 if closed.
  int fd_{-1};

  // Full path to the WAL file on disk.
  std::string path_;

  // Next logical sequence number returned from `append()`.
  Lsn next_lsn_{1};

  // Enables sync behavior inside `flush_on_commit()`.
  bool flush_on_commit_enabled_{false};

  // Sync implementation used by `flush_on_commit()`.
  SyncHook sync_hook_{};

  // Opens or creates the WAL file and stores the file descriptor in `fd_`.
  void open_or_create_();
};

} // namespace miniwaldb::wal
