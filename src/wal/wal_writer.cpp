#include "wal/wal_writer.h"
#include "storage/file_io.h"
#include <stdexcept>

namespace miniwaldb::wal {

// MVP encoding: [type:1][txid:8 little][payload_len:4 little][payload...]
// No CRC yet. We'll add CRC + proper fsync once the flow is working.

static void push_u64(std::vector<std::uint8_t>& out, std::uint64_t v) {
  for (int i = 0; i < 8; i++) out.push_back(static_cast<std::uint8_t>((v >> (8*i)) & 0xFF));
}
static void push_u32(std::vector<std::uint8_t>& out, std::uint32_t v) {
  for (int i = 0; i < 4; i++) out.push_back(static_cast<std::uint8_t>((v >> (8*i)) & 0xFF));
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
  bytes.push_back(static_cast<std::uint8_t>(rec.type));
  push_u64(bytes, rec.txid);
  push_u32(bytes, static_cast<std::uint32_t>(rec.payload.size()));
  bytes.insert(bytes.end(), rec.payload.begin(), rec.payload.end());

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
