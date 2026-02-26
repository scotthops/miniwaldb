#include "wal/wal_writer.h"
#include "storage/file_io.h"
#include <stdexcept>

namespace miniwaldb::wal {

// Framed encoding:
// [frame_len:4 little][type:1][txid:8 little][payload_len:4 little][payload...][crc32:4 little]
// frame_len counts bytes from type..payload (excludes frame_len and crc32).

static void push_u64(std::vector<std::uint8_t>& out, std::uint64_t v) {
  for (int i = 0; i < 8; i++) out.push_back(static_cast<std::uint8_t>((v >> (8*i)) & 0xFF));
}
static void push_u32(std::vector<std::uint8_t>& out, std::uint32_t v) {
  for (int i = 0; i < 4; i++) out.push_back(static_cast<std::uint8_t>((v >> (8*i)) & 0xFF));
}
static std::uint32_t crc32_ieee(const std::vector<std::uint8_t>& bytes,
                                std::size_t offset,
                                std::size_t len) {
  std::uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t i = 0; i < len; ++i) {
    crc ^= bytes[offset + i];
    for (int bit = 0; bit < 8; ++bit) {
      const std::uint32_t mask = static_cast<std::uint32_t>(-(static_cast<std::int32_t>(crc & 1u)));
      crc = (crc >> 1u) ^ (0xEDB88320u & mask);
    }
  }
  return ~crc;
}

WalWriter::WalWriter(std::string path) : path_(std::move(path)) {
  open_or_create_();
}

WalWriter::~WalWriter() = default;

void WalWriter::open_or_create_() {
  // For now we don't keep an fd open; we append with a helper.
  // Later: keep fd_ open and use pwrite/fsync.
}

Lsn WalWriter::append(const WalRecord& rec) {
  std::vector<std::uint8_t> bytes;
  const std::uint32_t payload_len = static_cast<std::uint32_t>(rec.payload.size());
  const std::uint32_t frame_len = static_cast<std::uint32_t>(1 + 8 + 4 + payload_len);

  push_u32(bytes, frame_len);
  const std::size_t frame_start = bytes.size();
  bytes.push_back(static_cast<std::uint8_t>(rec.type));
  push_u64(bytes, rec.txid);
  push_u32(bytes, payload_len);
  bytes.insert(bytes.end(), rec.payload.begin(), rec.payload.end());
  push_u32(bytes, crc32_ieee(bytes, frame_start, frame_len));

  storage::write_file_append(path_, bytes);
  return next_lsn_++;
}

void WalWriter::flush() {
  // placeholder: once we keep an fd open, implement fdatasync/fsync.
}

void WalWriter::flush_on_commit() {
  // placeholder
}

} // namespace miniwaldb::wal
