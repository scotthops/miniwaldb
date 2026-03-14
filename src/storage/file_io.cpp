#include "storage/file_io.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <stdexcept>
#include <unistd.h>

namespace miniwaldb::storage {

namespace {

void push_u64(std::vector<std::uint8_t>& out, std::uint64_t v) {
  for (int i = 0; i < 8; ++i) out.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xFF));
}

void push_u32(std::vector<std::uint8_t>& out, std::uint32_t v) {
  for (int i = 0; i < 4; ++i) out.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xFF));
}

std::uint64_t read_u64(const std::vector<std::uint8_t>& bytes, std::size_t& i) {
  if (i + 8 > bytes.size()) throw std::runtime_error("truncated snapshot");
  std::uint64_t v = 0;
  for (int k = 0; k < 8; ++k) v |= static_cast<std::uint64_t>(bytes[i++]) << (8 * k);
  return v;
}

std::uint32_t read_u32(const std::vector<std::uint8_t>& bytes, std::size_t& i) {
  if (i + 4 > bytes.size()) throw std::runtime_error("truncated snapshot");
  std::uint32_t v = 0;
  for (int k = 0; k < 4; ++k) v |= static_cast<std::uint32_t>(bytes[i++]) << (8 * k);
  return v;
}

void sync_parent_dir(const std::string& path) {
  const auto parent = std::filesystem::path(path).parent_path();
  const auto dir = parent.empty() ? std::filesystem::path(".") : parent;
  const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd == -1) {
    throw std::runtime_error("failed to open directory for sync: " + dir.string() + ": " + std::strerror(errno));
  }
  if (::fsync(fd) != 0) {
    const std::string err = std::strerror(errno);
    ::close(fd);
    throw std::runtime_error("failed to sync directory: " + dir.string() + ": " + err);
  }
  if (::close(fd) != 0) {
    throw std::runtime_error("failed to close directory: " + dir.string() + ": " + std::strerror(errno));
  }
}

} // namespace

std::vector<std::uint8_t> read_file(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return {};
  in.seekg(0, std::ios::end);
  const auto size = static_cast<std::size_t>(in.tellg());
  in.seekg(0, std::ios::beg);

  std::vector<std::uint8_t> buf(size);
  if (size) in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(size));
  return buf;
}

void write_file_append(const std::string& path, const std::vector<std::uint8_t>& bytes) {
  std::ofstream out(path, std::ios::binary | std::ios::app);
  if (!out) throw std::runtime_error("failed to open for append: " + path);
  if (!bytes.empty()) out.write(reinterpret_cast<const char*>(bytes.data()),
                                static_cast<std::streamsize>(bytes.size()));
}

void write_file(const std::string& path, const std::vector<std::uint8_t>& bytes) {
  const int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd == -1) throw std::runtime_error("failed to open for write: " + path + ": " + std::strerror(errno));

  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const auto n = ::write(fd, bytes.data() + offset, bytes.size() - offset);
    if (n < 0) {
      const std::string err = std::strerror(errno);
      ::close(fd);
      throw std::runtime_error("failed to write file: " + path + ": " + err);
    }
    offset += static_cast<std::size_t>(n);
  }

  if (::fsync(fd) != 0) {
    const std::string err = std::strerror(errno);
    ::close(fd);
    throw std::runtime_error("failed to sync file: " + path + ": " + err);
  }

  if (::close(fd) != 0) {
    throw std::runtime_error("failed to close file: " + path + ": " + std::strerror(errno));
  }
}

void save_snapshot(const std::string& path, const KvSnapshot& kv) {
  // Simple snapshot format:
  // [magic "MWS1":4][entry_count:u32][key:i64][value_len:u32][value bytes]...
  std::vector<std::uint8_t> bytes = {'M', 'W', 'S', '1'};
  push_u32(bytes, static_cast<std::uint32_t>(kv.size()));

  std::vector<std::pair<std::int64_t, std::string>> entries(kv.begin(), kv.end());
  std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
    return a.first < b.first;
  });

  for (const auto& entry : entries) {
    push_u64(bytes, static_cast<std::uint64_t>(entry.first));
    push_u32(bytes, static_cast<std::uint32_t>(entry.second.size()));
    bytes.insert(bytes.end(), entry.second.begin(), entry.second.end());
  }

  const auto temp_path = path + ".tmp";
  write_file(temp_path, bytes);
  if (::rename(temp_path.c_str(), path.c_str()) != 0) {
    const std::string err = std::strerror(errno);
    std::filesystem::remove(temp_path);
    throw std::runtime_error("failed to rename snapshot: " + path + ": " + err);
  }
  sync_parent_dir(path);
}

KvSnapshot load_snapshot(const std::string& path) {
  const auto bytes = read_file(path);
  if (bytes.empty()) return {};
  if (bytes.size() < 8) throw std::runtime_error("invalid snapshot");
  if (!(bytes[0] == 'M' && bytes[1] == 'W' && bytes[2] == 'S' && bytes[3] == '1')) {
    throw std::runtime_error("invalid snapshot magic");
  }

  std::size_t i = 4;
  const auto entry_count = read_u32(bytes, i);
  KvSnapshot kv;

  for (std::uint32_t entry = 0; entry < entry_count; ++entry) {
    const auto key = static_cast<std::int64_t>(read_u64(bytes, i));
    const auto value_len = read_u32(bytes, i);
    if (i + value_len > bytes.size()) throw std::runtime_error("truncated snapshot");
    kv[key] = std::string(bytes.begin() + static_cast<long>(i),
                          bytes.begin() + static_cast<long>(i + value_len));
    i += value_len;
  }

  if (i != bytes.size()) throw std::runtime_error("invalid snapshot trailing bytes");
  return kv;
}

} // namespace miniwaldb::storage
