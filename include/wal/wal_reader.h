#pragma once
#include <string>
#include <vector>
#include "wal/wal_writer.h"

namespace miniwaldb::wal {

class WalReader {
public:
  explicit WalReader(std::string path);
  std::vector<WalRecord> read_all();

private:
  std::string path_;
};

} // namespace miniwaldb::wal
