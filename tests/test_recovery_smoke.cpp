#include <catch2/catch_test_macros.hpp>
#include "db/db.h"
#include "wal/wal_writer.h"
#include <filesystem>
#include <fstream>
#include <vector>

namespace {

std::vector<std::uint8_t> encode_set_payload(std::int64_t key, const std::string& value) {
  std::vector<std::uint8_t> payload;
  for (int i = 0; i < 8; ++i) {
    payload.push_back(static_cast<std::uint8_t>((static_cast<std::uint64_t>(key) >> (8 * i)) & 0xFF));
  }
  const auto value_len = static_cast<std::uint32_t>(value.size());
  for (int i = 0; i < 4; ++i) {
    payload.push_back(static_cast<std::uint8_t>((value_len >> (8 * i)) & 0xFF));
  }
  payload.insert(payload.end(), value.begin(), value.end());
  return payload;
}

void corrupt_last_byte(const std::filesystem::path& path) {
  std::fstream f(path, std::ios::binary | std::ios::in | std::ios::out);
  REQUIRE(f.good());
  f.seekg(0, std::ios::end);
  const auto size = static_cast<std::streamoff>(f.tellg());
  REQUIRE(size > 0);
  f.seekg(size - 1);
  char b = 0;
  f.read(&b, 1);
  b = static_cast<char>(b ^ 0xFF);
  f.seekp(size - 1);
  f.write(&b, 1);
  f.flush();
}

} // namespace

TEST_CASE("Recovery replays only committed transactions") {
  const std::string dir = "test_recovery_only_committed";
  std::filesystem::remove_all(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  std::filesystem::create_directories(dir);
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Begin, 1, {}});
    w.append(WalRecord{RecordType::Set, 1, encode_set_payload(1, "v1")});
    w.append(WalRecord{RecordType::Commit, 1, {}});
    w.append(WalRecord{RecordType::Begin, 2, {}});
    w.append(WalRecord{RecordType::Set, 2, encode_set_payload(2, "v2")});
  }

  miniwaldb::Db reopened(dir);
  REQUIRE(reopened.get(1).has_value());
  REQUIRE(reopened.get(1).value() == "v1");
  REQUIRE_FALSE(reopened.get(2).has_value());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Truncated commit does not apply transaction") {
  const std::string dir = "test_recovery_truncated_commit";
  std::filesystem::remove_all(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  std::filesystem::create_directories(dir);
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Begin, 7, {}});
    w.append(WalRecord{RecordType::Set, 7, encode_set_payload(7, "lost")});
    w.append(WalRecord{RecordType::Commit, 7, {}});
  }

  const auto full_size = std::filesystem::file_size(wal_path);
  REQUIRE(full_size > 0);
  std::filesystem::resize_file(wal_path, full_size - 8); // tear commit frame tail

  miniwaldb::Db reopened(dir);
  REQUIRE_FALSE(reopened.get(7).has_value());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Earlier committed survive later corruption") {
  const std::string dir = "test_recovery_late_corruption";
  std::filesystem::remove_all(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  std::filesystem::create_directories(dir);
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Begin, 11, {}});
    w.append(WalRecord{RecordType::Set, 11, encode_set_payload(11, "ok")});
    w.append(WalRecord{RecordType::Commit, 11, {}});
    w.append(WalRecord{RecordType::Begin, 12, {}}); // this record will be corrupted
  }
  corrupt_last_byte(wal_path);

  miniwaldb::Db reopened(dir);
  REQUIRE(reopened.get(11).has_value());
  REQUIRE(reopened.get(11).value() == "ok");

  std::filesystem::remove_all(dir);
}
