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

WalReadResult WalReader::read_all() {
  const auto bytes = storage::read_file(path_);
  WalReadResult result;

  std::size_t i = 0;
  while (i < bytes.size()) {
    if (bytes.size() - i < 4) {
      result.stop_reason = ReadStopReason::IncompleteTail;
      break;
    }
    const auto frame_len = read_u32(bytes, i);
    if (frame_len < 13) {
      result.stop_reason = ReadStopReason::Corruption;
      break;
    }

    // If the header exists, reject contradictory lengths even if the body is short.
    if (bytes.size() - i >= 13) {
      std::size_t length_pos = i + 9;
      if (read_u32(bytes, length_pos) != frame_len - 13) {
        result.stop_reason = ReadStopReason::Corruption;
        break;
      }
    }
    const auto remaining = bytes.size() - i;
    if (frame_len > remaining || remaining - frame_len < 4) {
      result.stop_reason = ReadStopReason::IncompleteTail;
      break;
    }

    std::size_t crc_pos = i + frame_len;
    if (read_u32(bytes, crc_pos) != crc32_ieee(bytes, i, frame_len)) {
      result.stop_reason = ReadStopReason::Corruption;
      break;
    }
    const auto type = bytes[i];
    if (type < static_cast<std::uint8_t>(RecordType::Begin) ||
        type > static_cast<std::uint8_t>(RecordType::Delete)) {
      result.stop_reason = ReadStopReason::Corruption;
      break;
    }

    WalRecord record;
    record.type = static_cast<RecordType>(bytes[i++]);
    record.txid = read_u64(bytes, i);
    const auto payload_len = read_u32(bytes, i);
    record.payload.assign(bytes.begin() + i, bytes.begin() + i + payload_len);
    i += static_cast<std::size_t>(payload_len) + 4; // Skip payload and CRC.
    result.records.push_back(std::move(record));
    result.valid_bytes = i;
  }
  return result;
}

} // namespace miniwaldb::wal
