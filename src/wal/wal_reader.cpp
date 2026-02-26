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
static std::uint32_t crc32_ieee(const std::vector<std::uint8_t>& bytes,
                                std::size_t offset,
                                std::size_t len) {
  std::uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t idx = 0; idx < len; ++idx) {
    crc ^= bytes[offset + idx];
    for (int bit = 0; bit < 8; ++bit) {
      const std::uint32_t mask = static_cast<std::uint32_t>(-(static_cast<std::int32_t>(crc & 1u)));
      crc = (crc >> 1u) ^ (0xEDB88320u & mask);
    }
  }
  return ~crc;
}

WalReader::WalReader(std::string path) : path_(std::move(path)) {}

std::vector<WalRecord> WalReader::read_all() {
  const auto bytes = storage::read_file(path_);
  std::vector<WalRecord> out;

  std::size_t i = 0;
  while (i < bytes.size()) {
    if (i + 4 > bytes.size()) break; // truncated tail
    const auto frame_len = read_u32(bytes, i);
    if (frame_len < 13) break;
    if (i + frame_len + 4 > bytes.size()) break; // truncated tail

    const std::size_t frame_start = i;
    const std::size_t frame_end = i + frame_len;
    const std::size_t payload_start = i + 1 + 8 + 4;
    if (payload_start > frame_end) break;

    std::size_t payload_len_i = i + 1 + 8;
    const auto payload_len = read_u32(bytes, payload_len_i);
    if (payload_start + payload_len != frame_end) break;

    std::size_t crc_i = frame_end;
    const auto expected_crc = read_u32(bytes, crc_i);
    const auto computed_crc = crc32_ieee(bytes, frame_start, frame_len);
    if (computed_crc != expected_crc) break; // torn/corrupt tail

    WalRecord r;
    r.type = static_cast<RecordType>(bytes[i++]);
    r.txid = read_u64(bytes, i);
    const auto n = read_u32(bytes, i);
    r.payload.assign(bytes.begin() + static_cast<long>(i),
                     bytes.begin() + static_cast<long>(i + n));
    i += n + 4; // skip crc
    out.push_back(std::move(r));
  }
  return out;
}

} // namespace miniwaldb::wal
