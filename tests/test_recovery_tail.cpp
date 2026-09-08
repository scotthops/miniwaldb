#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include "db/db.h"
#include "storage/file_io.h"
#include "wal/wal_reader.h"
#include <algorithm>
#include <filesystem>

namespace {
using miniwaldb::wal::RecordType;
using miniwaldb::wal::ReadStopReason;

struct TestDirectory {
  const std::string path = "test_recovery_tail";
  TestDirectory() {
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
  }
  ~TestDirectory() { std::filesystem::remove_all(path); }
  std::string wal() const { return path + "/wal.log"; }
};

std::vector<std::uint8_t> set_payload(std::int64_t key, char value) {
  std::vector<std::uint8_t> bytes;
  for (int i = 0; i < 8; ++i) {
    bytes.push_back(static_cast<std::uint8_t>(static_cast<std::uint64_t>(key) >> (8 * i)));
  }
  bytes.insert(bytes.end(), {1, 0, 0, 0, static_cast<std::uint8_t>(value)});
  return bytes;
}
} // namespace

TEST_CASE("Recovery repairs incomplete tails before subsequent commits", "[recovery-tail]") {
  TestDirectory dir;
  bool tear_commit = false;
  std::size_t kept_bytes = 0;
  SECTION("partial frame length") { kept_bytes = 2; }
  SECTION("partial header") { kept_bytes = 9; }
  SECTION("partial payload") { kept_bytes = 20; }
  SECTION("partial CRC") { kept_bytes = 32; }
  SECTION("partial commit") { tear_commit = true; kept_bytes = 10; }

  std::size_t boundary = 0;
  {
    miniwaldb::wal::WalWriter writer(dir.wal());
    writer.append({RecordType::Begin, 1, {}});
    writer.append({RecordType::Set, 1, set_payload(1, 'a')});
    writer.append({RecordType::Commit, 1, {}});
    writer.append({RecordType::Begin, 2, {}});
    boundary = std::filesystem::file_size(dir.wal());
    writer.append({RecordType::Set, 2, set_payload(2, 'b')});
    if (tear_commit) boundary = std::filesystem::file_size(dir.wal());
    writer.append({RecordType::Commit, 2, {}});
  }
  std::filesystem::resize_file(dir.wal(), boundary + kept_bytes);
  const auto torn_bytes = miniwaldb::storage::read_file(dir.wal());
  const auto scan = miniwaldb::wal::WalReader(dir.wal()).read_all();
  REQUIRE(scan.stop_reason == ReadStopReason::IncompleteTail);
  REQUIRE(scan.valid_bytes == boundary);
  REQUIRE(scan.records.size() == (tear_commit ? 5 : 4));
  REQUIRE(miniwaldb::storage::read_file(dir.wal()) == torn_bytes); // Reader is read-only.
  const std::vector<std::uint8_t> expected_prefix(torn_bytes.begin(), torn_bytes.begin() + boundary);

  {
    miniwaldb::Db db(dir.path);
    REQUIRE(db.get(1) == "a");
    REQUIRE_FALSE(db.get(2).has_value());
    REQUIRE(miniwaldb::storage::read_file(dir.wal()) == expected_prefix);
  }
  {
    miniwaldb::Db db(dir.path); // Repeated recovery does not change the repaired WAL.
    REQUIRE(db.get(1) == "a");
    REQUIRE_FALSE(db.get(2).has_value());
    REQUIRE(miniwaldb::storage::read_file(dir.wal()) == expected_prefix);
    db.begin();
    db.put(3, "c");
    db.commit();
  }
  const auto with_c = miniwaldb::storage::read_file(dir.wal());
  REQUIRE(with_c.size() == boundary + 76); // Begin 21 + SET 34 + Commit 21.
  REQUIRE(std::equal(expected_prefix.begin(), expected_prefix.end(), with_c.begin()));
  REQUIRE(with_c[boundary] == 13); // C's BEGIN frame length starts exactly here.
  REQUIRE(with_c[boundary + 4] == static_cast<std::uint8_t>(RecordType::Begin));
  const auto final_scan = miniwaldb::wal::WalReader(dir.wal()).read_all();
  REQUIRE(final_scan.stop_reason == ReadStopReason::CleanEof);
  REQUIRE(final_scan.valid_bytes == with_c.size());
  REQUIRE(final_scan.records.size() == scan.records.size() + 3);
  for (int reopen = 0; reopen < 2; ++reopen) {
    miniwaldb::Db db(dir.path);
    REQUIRE(db.get(1) == "a");
    REQUIRE_FALSE(db.get(2).has_value());
    REQUIRE(db.get(3) == "c");
    REQUIRE(miniwaldb::storage::read_file(dir.wal()) == with_c);
  }
}

TEST_CASE("An incomplete first frame repairs to an empty WAL", "[recovery-tail]") {
  TestDirectory dir;
  miniwaldb::storage::write_file(dir.wal(), {13, 0});
  {
    miniwaldb::Db db(dir.path);
    REQUIRE(std::filesystem::file_size(dir.wal()) == 0);
    db.begin();
    db.put(1, "new");
    db.commit();
  }
  miniwaldb::Db reopened(dir.path);
  REQUIRE(reopened.get(1) == "new");
}

TEST_CASE("Empty and complete WALs report clean EOF", "[recovery-tail]") {
  TestDirectory dir;
  auto result = miniwaldb::wal::WalReader(dir.wal()).read_all();
  REQUIRE(result.stop_reason == ReadStopReason::CleanEof);
  REQUIRE(result.valid_bytes == 0);
  REQUIRE(result.records.empty());
  {
    miniwaldb::wal::WalWriter writer(dir.wal());
    writer.append({RecordType::Begin, 1, {}});
  }
  result = miniwaldb::wal::WalReader(dir.wal()).read_all();
  REQUIRE(result.stop_reason == ReadStopReason::CleanEof);
  REQUIRE(result.valid_bytes == 21);
  REQUIRE(result.records.size() == 1); // Incomplete transaction, but complete frame.
}

TEST_CASE("Complete invalid WAL data is corruption and never truncated", "[recovery-tail]") {
  TestDirectory dir;
  {
    miniwaldb::wal::WalWriter writer(dir.wal());
    writer.append({RecordType::Begin, 1, {}});
    writer.append({RecordType::Commit, 1, {}});
  }
  auto bytes = miniwaldb::storage::read_file(dir.wal());
  SECTION("bad CRC") { bytes.back() ^= 0xff; }
  SECTION("impossible frame length") { bytes[21] = 12; }
  SECTION("inconsistent payload length") { bytes[21 + 13] = 1; }
  SECTION("unknown record type with valid CRC") {
    miniwaldb::wal::WalWriter writer(dir.wal());
    writer.append({static_cast<RecordType>(99), 2, {}});
    bytes = miniwaldb::storage::read_file(dir.wal());
  }
  miniwaldb::storage::write_file(dir.wal(), bytes);
  const auto result = miniwaldb::wal::WalReader(dir.wal()).read_all();
  REQUIRE(result.stop_reason == ReadStopReason::Corruption);
  REQUIRE(result.valid_bytes >= 21);
  REQUIRE_THROWS_WITH(miniwaldb::Db{dir.path},
                      "WAL corruption at byte " + std::to_string(result.valid_bytes));
  REQUIRE(miniwaldb::storage::read_file(dir.wal()) == bytes);
}

TEST_CASE("Malformed transaction payload rejects startup before replay or repair", "[recovery-tail]") {
  TestDirectory dir;
  auto payload = set_payload(2, 'b');
  RecordType type = RecordType::Set;
  SECTION("short SET") { payload.resize(7); }
  SECTION("SET length exceeds payload") { payload[8] = 2; }
  SECTION("SET trailing bytes") { payload.push_back(0); }
  SECTION("invalid DELETE") { type = RecordType::Delete; payload.resize(7); }
  SECTION("invalid control payload") { type = RecordType::Begin; payload = {1}; }
  {
    miniwaldb::wal::WalWriter writer(dir.wal());
    writer.append({RecordType::Begin, 1, {}});
    writer.append({RecordType::Set, 1, set_payload(1, 'a')});
    writer.append({type, 1, payload});
    writer.append({RecordType::Commit, 1, {}});
  }
  auto bytes = miniwaldb::storage::read_file(dir.wal());
  bytes.push_back(13); // Even a repairable suffix must remain untouched on validation failure.
  miniwaldb::storage::write_file(dir.wal(), bytes);
  REQUIRE_THROWS_WITH(miniwaldb::Db{dir.path}, "WAL corruption: malformed payload in transaction 1");
  REQUIRE(miniwaldb::storage::read_file(dir.wal()) == bytes);
  REQUIRE_FALSE(std::filesystem::exists(dir.path + "/snapshot.dat"));
  // Construction failed: none of the malformed transaction is published to a usable Db.
}

TEST_CASE("SET and DELETE keys use explicit little endian encoding", "[recovery-tail]") {
  TestDirectory dir;
  {
    miniwaldb::Db db(dir.path);
    db.begin();
    db.put(-2, "negative");
    db.commit();
  }
  const auto result = miniwaldb::wal::WalReader(dir.wal()).read_all();
  const std::vector<std::uint8_t> key_bytes{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
  REQUIRE(std::vector<std::uint8_t>(result.records[1].payload.begin(), result.records[1].payload.begin() + 8) == key_bytes);
  {
    miniwaldb::Db db(dir.path);
    REQUIRE(db.get(-2) == "negative");
    db.begin();
    db.erase(-2);
    db.commit();
  }
  const auto deleted = miniwaldb::wal::WalReader(dir.wal()).read_all();
  REQUIRE(deleted.records[4].payload == key_bytes);
  miniwaldb::Db reopened(dir.path);
  REQUIRE_FALSE(reopened.get(-2).has_value());
}

TEST_CASE("Truncation errors are reported without creating a missing file", "[recovery-tail]") {
  TestDirectory dir;
  REQUIRE_THROWS(miniwaldb::storage::truncate_file(dir.wal(), 0));
  REQUIRE_FALSE(std::filesystem::exists(dir.wal()));
  REQUIRE_THROWS(miniwaldb::storage::truncate_file(dir.path, 0));
}
