#include <catch2/catch_test_macros.hpp>
#include "db/db.h"
#include "storage/file_io.h"
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

std::vector<std::uint8_t> encode_delete_payload(std::int64_t key) {
  std::vector<std::uint8_t> payload;
  for (int i = 0; i < 8; ++i) {
    payload.push_back(static_cast<std::uint8_t>((static_cast<std::uint64_t>(key) >> (8 * i)) & 0xFF));
  }
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

TEST_CASE("Db constructor loads snapshot state on startup") {
  const std::string dir = "test_db_constructor_loads_snapshot";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  miniwaldb::storage::KvSnapshot snapshot;
  snapshot.emplace(201, "snap");
  snapshot.emplace(202, "state");
  miniwaldb::storage::save_snapshot((std::filesystem::path(dir) / "snapshot.dat").string(), snapshot);

  miniwaldb::Db db(dir);
  REQUIRE(db.get(201).has_value());
  REQUIRE(db.get(201).value() == "snap");
  REQUIRE(db.get(202).has_value());
  REQUIRE(db.get(202).value() == "state");

  std::filesystem::remove_all(dir);
}

TEST_CASE("Checkpoint saves snapshot state for later reopen") {
  const std::string dir = "test_db_checkpoint_saves_snapshot";
  std::filesystem::remove_all(dir);

  {
    miniwaldb::Db db(dir);
    db.begin();
    db.put(401, "snapshotted");
    db.put(402, "state");
    db.commit();
    db.checkpoint();
  }

  std::filesystem::remove((std::filesystem::path(dir) / "wal.log").string());

  miniwaldb::Db reopened(dir);
  REQUIRE(reopened.get(401).has_value());
  REQUIRE(reopened.get(401).value() == "snapshotted");
  REQUIRE(reopened.get(402).has_value());
  REQUIRE(reopened.get(402).value() == "state");

  std::filesystem::remove_all(dir);
}

TEST_CASE("Checkpoint during transaction is rejected") {
  const std::string dir = "test_db_checkpoint_during_transaction";
  std::filesystem::remove_all(dir);

  miniwaldb::Db db(dir);
  db.begin();
  db.put(403, "pending");

  REQUIRE_THROWS(db.checkpoint());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Db constructor replays WAL on top of snapshot state") {
  const std::string dir = "test_db_constructor_snapshot_then_wal";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  miniwaldb::storage::KvSnapshot snapshot;
  snapshot.emplace(301, "from_snapshot");
  snapshot.emplace(302, "old");
  miniwaldb::storage::save_snapshot((std::filesystem::path(dir) / "snapshot.dat").string(), snapshot);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Begin, 1, {}});
    w.append(WalRecord{RecordType::Set, 1, encode_set_payload(302, "new")});
    w.append(WalRecord{RecordType::Delete, 1, encode_delete_payload(301)});
    w.append(WalRecord{RecordType::Commit, 1, {}});
  }

  miniwaldb::Db db(dir);
  REQUIRE_FALSE(db.get(301).has_value());
  REQUIRE(db.get(302).has_value());
  REQUIRE(db.get(302).value() == "new");

  std::filesystem::remove_all(dir);
}

TEST_CASE("Db constructor recovers committed data on reopen") {
  const std::string dir = "test_db_constructor_recovers_committed";
  std::filesystem::remove_all(dir);

  {
    miniwaldb::Db db(dir);
    db.begin();
    db.put(101, "persisted");
    db.commit();
  }

  miniwaldb::Db reopened(dir);
  REQUIRE(reopened.get(101).has_value());
  REQUIRE(reopened.get(101).value() == "persisted");

  std::filesystem::remove_all(dir);
}

TEST_CASE("Db constructor ignores uncommitted transaction on reopen") {
  const std::string dir = "test_db_constructor_ignores_uncommitted";
  std::filesystem::remove_all(dir);

  {
    miniwaldb::Db db(dir);
    db.begin();
    db.put(102, "not_committed");
  }

  miniwaldb::Db reopened(dir);
  REQUIRE_FALSE(reopened.get(102).has_value());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Db constructor recovery is stable across repeated reopen") {
  const std::string dir = "test_db_constructor_recovery_stable";
  std::filesystem::remove_all(dir);

  {
    miniwaldb::Db db(dir);
    db.begin();
    db.put(103, "stable");
    db.commit();
  }

  miniwaldb::Db first_reopen(dir);
  REQUIRE(first_reopen.get(103).has_value());
  REQUIRE(first_reopen.get(103).value() == "stable");

  miniwaldb::Db second_reopen(dir);
  REQUIRE(second_reopen.get(103).has_value());
  REQUIRE(second_reopen.get(103).value() == "stable");

  std::filesystem::remove_all(dir);
}

TEST_CASE("Committed delete removes existing key after recovery") {
  const std::string dir = "test_recovery_committed_delete";
  std::filesystem::remove_all(dir);

  {
    miniwaldb::Db db(dir);
    db.begin();
    db.put(1, "present");
    db.commit();

    db.begin();
    db.erase(1);
    db.commit();
  }

  miniwaldb::Db reopened(dir);
  REQUIRE_FALSE(reopened.get(1).has_value());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Uncommitted delete does not affect recovered state") {
  const std::string dir = "test_recovery_uncommitted_delete";
  std::filesystem::remove_all(dir);

  {
    miniwaldb::Db db(dir);
    db.begin();
    db.put(2, "present");
    db.commit();

    db.begin();
    db.erase(2);
  }

  miniwaldb::Db reopened(dir);
  REQUIRE(reopened.get(2).has_value());
  REQUIRE(reopened.get(2).value() == "present");

  std::filesystem::remove_all(dir);
}

TEST_CASE("Put then delete in same committed transaction leaves key absent") {
  const std::string dir = "test_recovery_put_then_delete_same_txn";
  std::filesystem::remove_all(dir);

  {
    miniwaldb::Db db(dir);
    db.begin();
    db.put(3, "temp");
    db.erase(3);
    db.commit();
  }

  miniwaldb::Db reopened(dir);
  REQUIRE_FALSE(reopened.get(3).has_value());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Delete then put in same committed transaction leaves latest value present") {
  const std::string dir = "test_recovery_delete_then_put_same_txn";
  std::filesystem::remove_all(dir);

  {
    miniwaldb::Db db(dir);
    db.begin();
    db.put(4, "old");
    db.commit();

    db.begin();
    db.erase(4);
    db.put(4, "new");
    db.commit();
  }

  miniwaldb::Db reopened(dir);
  REQUIRE(reopened.get(4).has_value());
  REQUIRE(reopened.get(4).value() == "new");

  std::filesystem::remove_all(dir);
}

TEST_CASE("Commit without matching begin is ignored safely") {
  const std::string dir = "test_recovery_commit_without_begin";
  std::filesystem::remove_all(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  std::filesystem::create_directories(dir);
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Commit, 99, {}});
  }

  miniwaldb::Db reopened(dir);
  REQUIRE_FALSE(reopened.get(99).has_value());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Operations without matching begin are ignored safely") {
  const std::string dir = "test_recovery_set_without_begin";
  std::filesystem::remove_all(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  std::filesystem::create_directories(dir);
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Set, 55, encode_set_payload(55, "orphan")});
    w.append(WalRecord{RecordType::Commit, 55, {}});
  }

  miniwaldb::Db reopened(dir);
  REQUIRE_FALSE(reopened.get(55).has_value());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Abort discards pending operations for transaction") {
  const std::string dir = "test_recovery_abort_discards_pending";
  std::filesystem::remove_all(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  std::filesystem::create_directories(dir);
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Begin, 21, {}});
    w.append(WalRecord{RecordType::Set, 21, encode_set_payload(21, "gone")});
    w.append(WalRecord{RecordType::Abort, 21, {}});
  }

  miniwaldb::Db reopened(dir);
  REQUIRE_FALSE(reopened.get(21).has_value());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Two committed transactions replay in WAL order") {
  const std::string dir = "test_recovery_committed_order";
  std::filesystem::remove_all(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  std::filesystem::create_directories(dir);
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Begin, 1, {}});
    w.append(WalRecord{RecordType::Set, 1, encode_set_payload(5, "first")});
    w.append(WalRecord{RecordType::Commit, 1, {}});
    w.append(WalRecord{RecordType::Begin, 2, {}});
    w.append(WalRecord{RecordType::Set, 2, encode_set_payload(5, "second")});
    w.append(WalRecord{RecordType::Commit, 2, {}});
  }

  miniwaldb::Db reopened(dir);
  REQUIRE(reopened.get(5).has_value());
  REQUIRE(reopened.get(5).value() == "second");

  std::filesystem::remove_all(dir);
}

TEST_CASE("Running recovery twice does not duplicate effects") {
  const std::string dir = "test_recovery_idempotent_reopen";
  std::filesystem::remove_all(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  std::filesystem::create_directories(dir);
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Begin, 1, {}});
    w.append(WalRecord{RecordType::Set, 1, encode_set_payload(8, "stable")});
    w.append(WalRecord{RecordType::Commit, 1, {}});
  }

  miniwaldb::Db first_open(dir);
  REQUIRE(first_open.get(8).has_value());
  REQUIRE(first_open.get(8).value() == "stable");

  miniwaldb::Db second_open(dir);
  REQUIRE(second_open.get(8).has_value());
  REQUIRE(second_open.get(8).value() == "stable");

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

TEST_CASE("Committed transaction survives truncated tail") {
  const std::string dir = "test_recovery_committed_before_truncated_tail";
  std::filesystem::remove_all(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  std::filesystem::create_directories(dir);
  {
    miniwaldb::wal::WalWriter w(wal_path);
    using miniwaldb::wal::RecordType;
    using miniwaldb::wal::WalRecord;
    w.append(WalRecord{RecordType::Begin, 31, {}});
    w.append(WalRecord{RecordType::Set, 31, encode_set_payload(31, "kept")});
    w.append(WalRecord{RecordType::Commit, 31, {}});
    w.append(WalRecord{RecordType::Begin, 32, {}});
    w.append(WalRecord{RecordType::Set, 32, encode_set_payload(32, "lost")});
  }

  const auto full_size = std::filesystem::file_size(wal_path);
  REQUIRE(full_size > 0);
  std::filesystem::resize_file(wal_path, full_size - 6);

  miniwaldb::Db reopened(dir);
  REQUIRE(reopened.get(31).has_value());
  REQUIRE(reopened.get(31).value() == "kept");
  REQUIRE_FALSE(reopened.get(32).has_value());

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

TEST_CASE("Missing WAL recovers as empty database") {
  const std::string dir = "test_recovery_missing_wal";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  miniwaldb::Db reopened(dir);
  REQUIRE_FALSE(reopened.get(1).has_value());

  std::filesystem::remove_all(dir);
}

TEST_CASE("Empty WAL recovers as empty database") {
  const std::string dir = "test_recovery_empty_wal";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  const auto wal_path = (std::filesystem::path(dir) / "wal.log").string();
  {
    std::ofstream out(wal_path, std::ios::binary);
    REQUIRE(out.good());
  }

  miniwaldb::Db reopened(dir);
  REQUIRE_FALSE(reopened.get(1).has_value());

  std::filesystem::remove_all(dir);
}
