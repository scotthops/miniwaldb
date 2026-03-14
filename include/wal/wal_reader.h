#pragma once
#include <string>
#include <vector>
#include "wal/wal_writer.h"

namespace miniwaldb::wal {

class WalReader {
public:
  // Creates a reader for the WAL file at `path`.
  // `path` is the WAL file location on disk.
  explicit WalReader(std::string path);

  // Reads WAL records sequentially from disk until EOF or the first invalid tail.
  // Returns only records from fully valid frames.
  std::vector<WalRecord> read_all();

private:
  // Full path to the WAL file being read.
  std::string path_;
};

} // namespace miniwaldb::wal
