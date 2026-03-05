#include "wal/wal_writer.h"
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <unistd.h>

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
static int real_sync(int fd) {
  if (::fdatasync(fd) == 0) return 0;
  if (errno != EINVAL) return -1;
  return ::fsync(fd);
}

WalWriter::WalWriter(std::string path, bool flush_on_commit, SyncHook sync_hook)
    : path_(std::move(path)),
      flush_on_commit_enabled_(flush_on_commit),
      sync_hook_(sync_hook ? std::move(sync_hook) : SyncHook(real_sync)) {
  open_or_create_();
}

WalWriter::~WalWriter() {
  if (fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
  }
}

void WalWriter::open_or_create_() {
  fd_ = ::open(path_.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
  if (fd_ == -1) {
    throw std::runtime_error("failed to open WAL: " + path_ + ": " + std::strerror(errno));
  }
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

  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const auto n = ::write(fd_, bytes.data() + offset, bytes.size() - offset);
    if (n < 0) {
      throw std::runtime_error("failed to write WAL: " + path_ + ": " + std::strerror(errno));
    }
    offset += static_cast<std::size_t>(n);
  }
  return next_lsn_++;
}

void WalWriter::flush() {
  // placeholder: once we keep an fd open, implement fdatasync/fsync.
}

void WalWriter::flush_on_commit() {
  if (fd_ == -1) return;
  if (!flush_on_commit_enabled_) return;
  if (sync_hook_(fd_) != 0) {
    throw std::runtime_error("failed to sync WAL: " + path_ + ": " + std::strerror(errno));
  }
}

} // namespace miniwaldb::wal
