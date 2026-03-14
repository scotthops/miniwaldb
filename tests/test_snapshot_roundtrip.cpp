#include <catch2/catch_test_macros.hpp>
#include "storage/file_io.h"
#include <filesystem>

TEST_CASE("Snapshot save/load roundtrip preserves key-value state") {
  const auto path = std::filesystem::temp_directory_path() / "miniwaldb_snapshot_roundtrip.snap";
  std::filesystem::remove(path);

  miniwaldb::storage::KvSnapshot expected;
  expected.emplace(1, "one");
  expected.emplace(-2, "two");
  expected.emplace(42, "");

  miniwaldb::storage::save_snapshot(path.string(), expected);
  const auto actual = miniwaldb::storage::load_snapshot(path.string());

  REQUIRE(actual == expected);

  std::filesystem::remove(path);
}

TEST_CASE("Loading missing snapshot returns empty state") {
  const auto path = std::filesystem::temp_directory_path() / "miniwaldb_snapshot_missing.snap";
  std::filesystem::remove(path);

  const auto actual = miniwaldb::storage::load_snapshot(path.string());

  REQUIRE(actual.empty());
}
