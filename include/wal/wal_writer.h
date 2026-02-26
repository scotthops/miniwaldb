#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace miniwaldb::wal {

using TxId = std::uint64_t;
using Lsn  = std::uint64_t;

enum class RecordType : std::uint8_t {
  Begin = 1,
  Set   = 2,
  Commit= 3,
  Abort = 4,
  Checkpoint = 5
};

struct WalRecord {
  RecordType type{};
  TxId txid{};
  std::vector<std::uint8_t> payload; // keep flexible for now
};

class WalWriter {
public:
  explicit WalWriter(std::string path);
  ~WalWriter();

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  Lsn append(const WalRecord& rec);
  void flush();          // fsync-ish later
  void flush_on_commit(); // fsync on commit (placeholder for now)

private:
  int fd_{-1};
  std::string path_;
  Lsn next_lsn_{1};

  void open_or_create_();
};

} // namespace miniwaldb::wal
