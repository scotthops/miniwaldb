#include <catch2/catch_test_macros.hpp>
#include "wal/wal_writer.h"
#include "wal/wal_reader.h"
#include <filesystem>

TEST_CASE("WAL roundtrip writes and reads records") {
  const std::string path = "test_wal_roundtrip.wal";
  std::filesystem::remove(path);

  miniwaldb::wal::WalWriter w(path);
  using miniwaldb::wal::RecordType;
  using miniwaldb::wal::WalRecord;

  w.append(WalRecord{RecordType::Begin, 1, {}});
  w.append(WalRecord{RecordType::Set, 1, {1,2,3}});
  w.append(WalRecord{RecordType::Commit, 1, {}});

  miniwaldb::wal::WalReader r(path);
  auto recs = r.read_all();

  REQUIRE(recs.size() == 3);
  REQUIRE(recs[0].type == RecordType::Begin);
  REQUIRE(recs[1].type == RecordType::Set);
  REQUIRE(recs[1].payload.size() == 3);
  REQUIRE(recs[2].type == RecordType::Commit);

  std::filesystem::remove(path);
}

TEST_CASE("WAL reader stops at truncated mid-record tail") {
  const std::string path = "test_wal_truncated_tail.wal";
  std::filesystem::remove(path);

  miniwaldb::wal::WalWriter w(path);
  using miniwaldb::wal::RecordType;
  using miniwaldb::wal::WalRecord;

  w.append(WalRecord{RecordType::Begin, 7, {}});
  w.append(WalRecord{RecordType::Set, 7, {9,8,7,6}});
  w.append(WalRecord{RecordType::Commit, 7, {}});

  const auto full_size = std::filesystem::file_size(path);
  REQUIRE(full_size > 0);
  // Last record is at least 21 bytes ([len][type+txid+payload_len][crc]).
  // Truncate 8 bytes so the third record is definitely torn mid-record.
  std::filesystem::resize_file(path, full_size - 8);

  miniwaldb::wal::WalReader r(path);
  auto recs = r.read_all();

  REQUIRE(recs.size() == 2);
  REQUIRE(recs[0].type == RecordType::Begin);
  REQUIRE(recs[1].type == RecordType::Set);
  REQUIRE(recs[1].payload == std::vector<std::uint8_t>{9,8,7,6});

  std::filesystem::remove(path);
}

TEST_CASE("WAL flush_on_commit keeps data readable") {
  const std::string path = "test_wal_flush_on_commit.wal";
  std::filesystem::remove(path);

  miniwaldb::wal::WalWriter w(path);
  using miniwaldb::wal::RecordType;
  using miniwaldb::wal::WalRecord;

  w.append(WalRecord{RecordType::Begin, 42, {}});
  w.append(WalRecord{RecordType::Commit, 42, {}});
  w.flush_on_commit();

  miniwaldb::wal::WalReader r(path);
  auto recs = r.read_all();

  REQUIRE(recs.size() == 2);
  REQUIRE(recs[0].type == RecordType::Begin);
  REQUIRE(recs[1].type == RecordType::Commit);

  std::filesystem::remove(path);
}

TEST_CASE("WAL flush_on_commit false does not call sync hook") {
  const std::string path = "test_wal_flush_hook_disabled.wal";
  std::filesystem::remove(path);

  int sync_calls = 0;
  miniwaldb::wal::WalWriter w(
      path,
      false,
      [&sync_calls](int) {
        ++sync_calls;
        return 0;
      });
  using miniwaldb::wal::RecordType;
  using miniwaldb::wal::WalRecord;

  w.append(WalRecord{RecordType::Commit, 99, {}});
  w.flush_on_commit();

  REQUIRE(sync_calls == 0);
  std::filesystem::remove(path);
}

TEST_CASE("WAL flush_on_commit true calls sync hook once") {
  const std::string path = "test_wal_flush_hook_enabled.wal";
  std::filesystem::remove(path);

  int sync_calls = 0;
  miniwaldb::wal::WalWriter w(
      path,
      true,
      [&sync_calls](int) {
        ++sync_calls;
        return 0;
      });
  using miniwaldb::wal::RecordType;
  using miniwaldb::wal::WalRecord;

  w.append(WalRecord{RecordType::Commit, 100, {}});
  w.flush_on_commit();

  REQUIRE(sync_calls == 1);
  std::filesystem::remove(path);
}
