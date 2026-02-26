#include "storage/file_io.h"
#include <fstream>
#include <stdexcept>

namespace miniwaldb::storage {

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

} // namespace miniwaldb::storage
