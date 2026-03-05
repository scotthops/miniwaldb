#include <catch2/catch_test_macros.hpp>
#include "wal/wal_writer.h"
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <vector>

namespace {

std::string hex_dump(const std::vector<std::uint8_t>& bytes) {
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    if (i != 0) out << ' ';
    out << std::setw(2) << static_cast<int>(bytes[i]);
  }
  return out.str();
}

std::vector<std::uint8_t> read_all_bytes(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return {};
  in.seekg(0, std::ios::end);
  const auto size = static_cast<std::size_t>(in.tellg());
  in.seekg(0, std::ios::beg);
  std::vector<std::uint8_t> out(size);
  if (size > 0) {
    in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(size));
  }
  return out;
}

} // namespace

TEST_CASE("WAL format golden bytes for one deterministic record") {
  const auto wal_path = std::filesystem::temp_directory_path() / "miniwaldb_wal_format_golden.wal";
  std::filesystem::remove(wal_path);

  {
    miniwaldb::wal::WalWriter writer(wal_path.string());
    writer.append(miniwaldb::wal::WalRecord{miniwaldb::wal::RecordType::Begin, 1, {}});
    writer.flush_on_commit();
  }

  const auto actual = read_all_bytes(wal_path);
  const std::vector<std::uint8_t> expected = {
      // Finalize this test:
      // 1) Run tests once and copy "actual hex".
      // 2) Paste the bytes here as 0xNN entries.
      // 3) Remove the temporary forced failure below.
  };

  INFO("expected hex: " << hex_dump(expected));
  INFO("actual hex:   " << hex_dump(actual));

  REQUIRE(false); // temporary forced failure for golden-byte capture
  REQUIRE(actual == expected);
}
