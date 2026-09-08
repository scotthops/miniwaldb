#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include "db/db.h"
#include "storage/file_io.h"
#include "wal/wal_reader.h"
#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <unistd.h>

namespace {
// These tests run sequentially; scope cleanup also runs when an assertion fails.
struct TestDirectory {
  const std::string path = "test_persistence_boundary";
  TestDirectory() {
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
  }
  ~TestDirectory() { std::filesystem::remove_all(path); }
  std::string wal_path() const { return path + "/wal.log"; }
};

void require_unusable(miniwaldb::Db& db) {
  const char* message = "database persistence error: destroy and reopen before continuing";
  REQUIRE_THROWS_WITH(db.begin(), message);
  REQUIRE_THROWS_WITH(db.put(7, "later"), message);
  REQUIRE_THROWS_WITH(db.erase(7), message);
  REQUIRE_THROWS_WITH(db.get(7), message);
  REQUIRE_THROWS_WITH(db.commit(), message);
  REQUIRE_THROWS_WITH(db.abort(), message);
  REQUIRE_THROWS_WITH(db.checkpoint(), message);
}
} // namespace

TEST_CASE("Db syncs complete transactions including after checkpoint", "[persistence]") {
  TestDirectory dir;
  int sync_calls = 0;
  {
    miniwaldb::Db db(dir.path, [&](int fd) {
      const auto records = miniwaldb::wal::WalReader(dir.wal_path()).read_all().records;
      REQUIRE(records.size() == 3);
      REQUIRE(records[0].type == miniwaldb::wal::RecordType::Begin);
      REQUIRE(records[1].type == miniwaldb::wal::RecordType::Set);
      REQUIRE(records[2].type == miniwaldb::wal::RecordType::Commit);
      ++sync_calls;
      return ::fdatasync(fd);
    });
    db.begin();
    db.put(7, "first");
    REQUIRE(sync_calls == 0);
    db.commit();
    REQUIRE(sync_calls == 1);
    REQUIRE(db.get(7) == "first");
    db.checkpoint();
    db.begin();
    db.put(7, "second");
    db.commit();
    REQUIRE(sync_calls == 2);
    REQUIRE(db.get(7) == "second");
  }
  miniwaldb::Db reopened(dir.path);
  REQUIRE(reopened.get(7) == "second");
}

TEST_CASE("Sync failure stops Db but a complete commit can still recover", "[persistence]") {
  TestDirectory dir;
  {
    miniwaldb::Db db(dir.path, [](int) { errno = EIO; return -1; });
    db.begin();
    db.put(7, "uncertain");
    REQUIRE_THROWS(db.commit());
    const auto before = miniwaldb::storage::read_file(dir.wal_path());
    require_unusable(db);
    REQUIRE(miniwaldb::storage::read_file(dir.wal_path()) == before);
  }
  // This injected error left a complete COMMIT readable. Failure is not rollback.
  miniwaldb::Db reopened(dir.path);
  REQUIRE(reopened.get(7) == "uncertain");
  reopened.begin();
  reopened.put(7, "after reopen");
  reopened.commit();
  REQUIRE(reopened.get(7) == "after reopen");
}

TEST_CASE("Every database WAL append failure stops continued use", "[persistence]") {
  using miniwaldb::wal::RecordType;
  TestDirectory dir;
  RecordType fail_type = RecordType::Begin;
  SECTION("begin") { fail_type = RecordType::Begin; }
  SECTION("put") { fail_type = RecordType::Set; }
  SECTION("erase") { fail_type = RecordType::Delete; }
  SECTION("commit") { fail_type = RecordType::Commit; }
  SECTION("abort") { fail_type = RecordType::Abort; }
  {
    miniwaldb::Db db(dir.path, {}, [&](int fd, const void* data, std::size_t size) -> ssize_t {
      const auto* bytes = static_cast<const std::uint8_t*>(data);
      if (bytes[4] == static_cast<std::uint8_t>(fail_type)) {
        errno = ENOSPC;
        return -1; // No bytes written in this fixture, so reopen can safely continue.
      }
      return ::write(fd, data, size);
    });
    if (fail_type == RecordType::Begin) {
      REQUIRE_THROWS(db.begin());
    } else {
      db.begin();
      if (fail_type == RecordType::Set) REQUIRE_THROWS(db.put(7, "pending"));
      else if (fail_type == RecordType::Delete) REQUIRE_THROWS(db.erase(7));
      else {
        db.put(7, "pending");
        if (fail_type == RecordType::Commit) REQUIRE_THROWS(db.commit());
        else REQUIRE_THROWS(db.abort());
      }
    }
    const auto before = miniwaldb::storage::read_file(dir.wal_path());
    require_unusable(db);
    REQUIRE(miniwaldb::storage::read_file(dir.wal_path()) == before);
  }
  miniwaldb::Db reopened(dir.path);
  REQUIRE_FALSE(reopened.get(7).has_value());
  reopened.begin();
  reopened.put(7, "after reopen");
  reopened.commit();
}

TEST_CASE("Partial commit write followed by failure stops Db", "[persistence]") {
  TestDirectory dir;
  bool commit_started = false;
  {
    miniwaldb::Db db(dir.path, {}, [&](int fd, const void* data, std::size_t size) -> ssize_t {
      if (commit_started) { errno = EIO; return -1; }
      const auto* bytes = static_cast<const std::uint8_t*>(data);
      if (bytes[4] == static_cast<std::uint8_t>(miniwaldb::wal::RecordType::Commit)) {
        commit_started = true;
        return ::write(fd, data, 5);
      }
      return ::write(fd, data, size);
    });
    db.begin();
    db.put(7, "pending");
    const auto before = std::filesystem::file_size(dir.wal_path());
    REQUIRE_THROWS(db.commit());
    REQUIRE(std::filesystem::file_size(dir.wal_path()) == before + 5);
    require_unusable(db);
  }
  miniwaldb::Db reopened(dir.path);
  REQUIRE_FALSE(reopened.get(7).has_value());
  // Startup has now removed the partial commit frame.
}

TEST_CASE("WAL retries interrupted and short writes", "[persistence]") {
  TestDirectory dir;
  int calls = 0;
  miniwaldb::wal::WalWriter writer(dir.wal_path(), true, {},
      [&](int fd, const void* data, std::size_t size) -> ssize_t {
        if (++calls == 1) { errno = EINTR; return -1; }
        return ::write(fd, data, std::min(size, std::size_t{3}));
      });
  writer.append({miniwaldb::wal::RecordType::Begin, 42, {}});
  writer.flush_on_commit();
  const auto records = miniwaldb::wal::WalReader(dir.wal_path()).read_all().records;
  REQUIRE(calls == 8); // One interruption, then seven three-byte writes.
  REQUIRE(records.size() == 1);
  REQUIRE(records[0].txid == 42);
  REQUIRE(records[0].type == miniwaldb::wal::RecordType::Begin);
}

TEST_CASE("Zero progress WAL write reports failure and stops Db", "[persistence]") {
  TestDirectory dir;
  miniwaldb::Db db(dir.path, {}, [](int, const void*, std::size_t) -> ssize_t { return 0; });
  REQUIRE_THROWS(db.begin());
  require_unusable(db);
}

TEST_CASE("Explicit WAL flush syncs even when commit syncing is disabled", "[persistence]") {
  TestDirectory dir;
  int calls = 0;
  miniwaldb::wal::WalWriter writer(dir.wal_path(), false, [&](int fd) {
    if (++calls == 1) { errno = EINTR; return -1; }
    return ::fdatasync(fd);
  });
  writer.flush_on_commit();
  REQUIRE(calls == 0);
  writer.flush();
  REQUIRE(calls == 2);
}

TEST_CASE("File reads distinguish missing files from access errors", "[persistence]") {
  TestDirectory dir;
  REQUIRE(miniwaldb::storage::read_file(dir.path + "/missing").empty());
  miniwaldb::storage::write_file(dir.path + "/regular", {1, 2, 3});
  REQUIRE_THROWS(miniwaldb::storage::read_file(dir.path + "/regular/child"));
  REQUIRE_THROWS(miniwaldb::storage::read_file(dir.path));
  SECTION("snapshot path is a directory") {
    std::filesystem::create_directory(dir.path + "/snapshot.dat");
    REQUIRE_THROWS(miniwaldb::Db{dir.path});
  }
  SECTION("WAL path is a directory") {
    std::filesystem::create_directory(dir.wal_path());
    REQUIRE_THROWS(miniwaldb::Db{dir.path});
  }
}

TEST_CASE("Checkpoint persistence failure also stops Db", "[persistence]") {
  TestDirectory dir;
  miniwaldb::Db db(dir.path);
  db.begin();
  db.put(7, "committed");
  db.commit();
  std::filesystem::create_directory(dir.path + "/snapshot.dat.tmp");
  REQUIRE_THROWS(db.checkpoint());
  require_unusable(db);
}
