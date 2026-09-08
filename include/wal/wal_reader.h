#pragma once
#include <string>
#include <vector>
#include "wal/wal_writer.h"

namespace miniwaldb::wal {

enum class ReadStopReason { CleanEof, IncompleteTail, Corruption };

struct WalReadResult {
  std::vector<WalRecord> records;
  std::size_t valid_bytes{0}; // Immediately after the last complete, valid frame.
  ReadStopReason stop_reason{ReadStopReason::CleanEof};
};

class WalReader {
public:
  // Creates a reader for the WAL file at `path`.
  // `path` is the WAL file location on disk.
  explicit WalReader(std::string path);

  // Reads WAL records sequentially from disk until EOF or the first invalid tail.
  // Reports the valid prefix and why reading stopped; never modifies the file.
  WalReadResult read_all();

private:
  // Full path to the WAL file being read.
  std::string path_;
};

} // namespace miniwaldb::wal
