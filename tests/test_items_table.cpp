#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include "table/items_table.h"
#include "storage/file_io.h"
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <unistd.h>

namespace {
using miniwaldb::Db;
using miniwaldb::ItemRow;
using miniwaldb::ItemsTable;

struct TestDirectory {
  std::string path;
  TestDirectory() {
    auto pattern = (std::filesystem::temp_directory_path() / "miniwaldb-items-XXXXXX").string();
    if (!::mkdtemp(pattern.data())) throw std::runtime_error("cannot create test directory");
    path = std::move(pattern);
  }
  ~TestDirectory() { std::filesystem::remove_all(path); }
};
} // namespace

TEST_CASE("Items enforce distinct insert update and delete rules", "[items]") {
  TestDirectory dir;
  Db db(dir.path);
  ItemsTable items(db);
  REQUIRE_FALSE(items.lookup(1).has_value());
  db.begin();
  items.insert(1, ""); // Empty text is a present row, not a missing value.
  REQUIRE(items.lookup(1) == ItemRow{1, ""});
  REQUIRE_THROWS_WITH(items.insert(1, "duplicate"), "item already exists");
  REQUIRE(items.lookup(1) == ItemRow{1, ""});
  items.update(1, "updated");
  REQUIRE(items.lookup(1) == ItemRow{1, "updated"});
  REQUIRE_THROWS_WITH(items.update(2, "missing"), "item does not exist");
  REQUIRE_THROWS_WITH(items.erase(2), "item does not exist");
  REQUIRE_FALSE(items.lookup(2).has_value());
  items.erase(1);
  REQUIRE_FALSE(items.lookup(1).has_value());
  REQUIRE_THROWS_WITH(items.erase(1), "item does not exist");
  items.insert(1, "reinserted");
  db.commit(); // Constraint failures did not poison the transaction.
  REQUIRE(items.lookup(1) == ItemRow{1, "reinserted"});
}

TEST_CASE("Items mutations require the existing Db transaction", "[items]") {
  TestDirectory dir;
  Db db(dir.path);
  ItemsTable items(db);
  REQUIRE_THROWS_WITH(items.insert(1, "outside"), "not in transaction");
  db.begin();
  items.insert(1, "original");
  db.commit();
  const auto wal_path = dir.path + "/wal.log";
  const auto before = miniwaldb::storage::read_file(wal_path);
  REQUIRE_THROWS_WITH(items.update(1, "outside"), "not in transaction");
  REQUIRE_THROWS_WITH(items.erase(1), "not in transaction");
  REQUIRE(items.lookup(1) == ItemRow{1, "original"});
  REQUIRE(miniwaldb::storage::read_file(wal_path) == before);
}

TEST_CASE("Items scans selections and projections are ordered and typed", "[items]") {
  TestDirectory dir;
  Db db(dir.path);
  ItemsTable items(db);
  REQUIRE(items.scan().empty());
  REQUIRE(items.select_value("missing").empty());
  REQUIRE(miniwaldb::project_ids(items.scan()).empty());
  REQUIRE(miniwaldb::project_values(items.scan()).empty());
  db.begin();
  items.insert(8, "shared");
  items.insert(-2, "shared");
  items.insert(3, "Shared");
  items.insert(1, "");
  const std::vector<ItemRow> expected{{-2, "shared"}, {1, ""}, {3, "Shared"}, {8, "shared"}};
  REQUIRE(items.scan() == expected); // Reads uncommitted rows through the working map.
  const auto matches = items.select_value("shared");
  REQUIRE(matches == std::vector<ItemRow>{{-2, "shared"}, {8, "shared"}});
  REQUIRE(items.select_value("") == std::vector<ItemRow>{{1, ""}});
  REQUIRE(items.select_value("missing").empty());
  REQUIRE(miniwaldb::project_ids(matches) == std::vector<std::int64_t>{-2, 8});
  REQUIRE(miniwaldb::project_values(matches) == std::vector<std::string>{"shared", "shared"});
  REQUIRE(miniwaldb::project_ids(items.scan()) == std::vector<std::int64_t>{-2, 1, 3, 8});
  REQUIRE(miniwaldb::project_values(items.scan()) == std::vector<std::string>{"shared", "", "Shared", "shared"});
  auto detached = items.scan();
  detached[0].value = "local change";
  REQUIRE(items.lookup(-2) == ItemRow{-2, "shared"}); // Results cannot mutate storage.
  db.commit();
  REQUIRE(items.scan() == expected);
}

TEST_CASE("Aborting relational mutations restores all table queries", "[items]") {
  TestDirectory dir;
  Db db(dir.path);
  ItemsTable items(db);
  db.begin();
  items.insert(1, "original");
  items.insert(2, "keep");
  db.commit();
  const auto committed = items.scan();
  db.begin();
  items.update(1, "changed");
  items.erase(2);
  items.insert(3, "new");
  REQUIRE(items.scan() == std::vector<ItemRow>{{1, "changed"}, {3, "new"}});
  db.abort();
  REQUIRE(items.scan() == committed);
  REQUIRE(items.select_value("changed").empty());
  REQUIRE_FALSE(items.lookup(3).has_value());
  REQUIRE(items.lookup(2) == ItemRow{2, "keep"});
}

TEST_CASE("Relational commits use Db sync and survive repeated recovery", "[items]") {
  TestDirectory dir;
  int sync_calls = 0;
  {
    Db db(dir.path, [&](int fd) { ++sync_calls; return ::fsync(fd); });
    ItemsTable items(db);
    db.begin();
    items.insert(-2, "original");
    items.insert(7, "remove");
    db.commit();
    db.begin();
    items.update(-2, "persisted");
    items.erase(7);
    items.insert(9, "new");
    db.commit();
    REQUIRE(sync_calls == 2);
    REQUIRE(db.get(-2) == "persisted"); // The table and engine share the same state.
    REQUIRE_FALSE(std::filesystem::exists(dir.path + "/snapshot.dat"));
  }
  const auto wal_path = dir.path + "/wal.log";
  const auto wal_bytes = miniwaldb::storage::read_file(wal_path);
  REQUIRE_FALSE(wal_bytes.empty());
  for (int reopen = 0; reopen < 3; ++reopen) {
    Db db(dir.path);
    ItemsTable items(db);
    REQUIRE(items.scan() == std::vector<ItemRow>{{-2, "persisted"}, {9, "new"}});
    REQUIRE_FALSE(items.lookup(7).has_value());
    REQUIRE(miniwaldb::storage::read_file(wal_path) == wal_bytes);
  }
}

TEST_CASE("Table operations preserve Db fail-stop behavior", "[items]") {
  TestDirectory dir;
  Db db(dir.path, [](int) { errno = EIO; return -1; });
  ItemsTable items(db);
  db.begin();
  items.insert(1, "uncertain");
  REQUIRE_THROWS(db.commit());
  const char* message = "database persistence error: destroy and reopen before continuing";
  REQUIRE_THROWS_WITH(items.lookup(1), message);
  REQUIRE_THROWS_WITH(items.scan(), message);
  REQUIRE_THROWS_WITH(items.select_value("uncertain"), message);
  REQUIRE_THROWS_WITH(items.insert(2, "later"), message);
  REQUIRE_THROWS_WITH(items.update(1, "later"), message);
  REQUIRE_THROWS_WITH(items.erase(1), message);
}
