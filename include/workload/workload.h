#pragma once
#include "wal/wal_writer.h"
#include <cstddef>
#include <string>

namespace miniwaldb::workload {
struct Options {
  std::size_t producers{2};
  std::size_t keys_per_producer{100}; // Each key generates Insert then Lookup.
  std::size_t capacity{64};
};
struct Result {
  std::size_t attempted{};
  std::size_t accepted{};
  std::size_t processed{}; // Successfully completed requests.
  std::size_t failed{};    // Requests whose execution failed, not startup failures.
  std::size_t unprocessed{};
  std::size_t max_depth{};
  bool verified{false};
  std::string error;
};
// Requires an empty database. All started threads are joined before returning.
// The sync hook is passed only to the consumer's Db for deterministic failure tests.
Result run(const std::string& directory, const Options& options = {}, wal::SyncHook sync_hook = {});
}
