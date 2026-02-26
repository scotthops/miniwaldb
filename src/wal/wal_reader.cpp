#include "wal/wal_reader.h"
#include "storage/file_io.h"
#include <cstddef>
#include <cstdint>

namespace miniwaldb::wal {

static std::uint64_t read_u64(const std::vector<std::uint8_t>& b, std::size_t& i) {
  std::uint64_t v = 0;
  for (int k = 0; k < 8; k++) v |= (std::uint64_t)b[i++] << (8*k);
  return v;
}
static std::uint32_t read_u32(const std::vector<std::uint8_t>& b, std::size_t& i) {
  std::uint32_t v = 0;
  for (int k = 0; k < 4; k++) v |= (std::uint32_t)b[i++] << (8*k);
  return v;
}

WalReader::WalReader(std::string path) : path_(std::move(path)) {}

std::vector<WalRecord> WalReader::read_all() {
  const auto bytes = storage::read_file(path_);
  std::vector<WalRecord> out;

  std::size_t i = 0;
  while (i < bytes.size()) {
    if (i + 1 + 8 + 4 > bytes.size()) break; // truncated tail
    WalRecord r;
    r.type = static_cast<RecordType>(bytes[i++]);
    r.txid = read_u64(bytes, i);
    const auto n = read_u32(bytes, i);
    if (i + n > bytes.size()) break; // truncated tail
    r.payload.assign(bytes.begin() + static_cast<long>(i),
                     bytes.begin() + static_cast<long>(i + n));
    i += n;
    out.push_back(std::move(r));
  }
  return out;
}

} // namespace miniwaldb::wal
