#include <catch2/catch_test_macros.hpp>
#include "db/db.h"
#include <filesystem>

TEST_CASE("Recovery replays only committed transactions") {
  const std::string dir = "test_recovery_db";
  std::filesystem::remove_all(dir);

  {
    miniwaldb::Db db(dir);
    db.begin();
    db.put(1, "txn1");
    db.commit();

    db.begin();
    db.put(2, "txn2");
    // no commit: should be ignored on reopen
  }

  miniwaldb::Db reopened(dir);
  REQUIRE(reopened.get(1).has_value());
  REQUIRE(reopened.get(1).value() == "txn1");
  REQUIRE_FALSE(reopened.get(2).has_value());

  std::filesystem::remove_all(dir);
}
