#include <catch2/catch_test_macros.hpp>
#include "workload/request_queue.h"
#include "workload/workload.h"
#include "table/items_table.h"
#include "storage/file_io.h"
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <stdexcept>
#include <thread>

namespace {
using namespace miniwaldb::workload;
Request request(std::size_t id) { return {id, Operation::Insert, static_cast<std::int64_t>(id), "value"}; }
struct TestDirectory {
  std::string path;
  TestDirectory() {
    auto pattern = (std::filesystem::temp_directory_path() / "miniwaldb-workload-test-XXXXXX").string();
    if (!::mkdtemp(pattern.data())) throw std::runtime_error("cannot create test directory");
    path = std::move(pattern);
  }
  ~TestDirectory() { std::filesystem::remove_all(path); }
};
}

TEST_CASE("Request queue is bounded FIFO and drains after close", "[workload][queue]") {
  REQUIRE_THROWS(RequestQueue{0});
  RequestQueue queue(3);
  REQUIRE(queue.push(request(10)));
  REQUIRE(queue.push(request(11)));
  REQUIRE(queue.push(request(12)));
  REQUIRE(queue.max_depth() == 3);
  queue.close();
  queue.close(); // Closing is idempotent.
  REQUIRE_FALSE(queue.push(request(13)));
  Request item;
  for (std::size_t id = 10; id <= 12; ++id) {
    REQUIRE(queue.pop(item));
    REQUIRE(item.id == id);
  }
  REQUIRE_FALSE(queue.pop(item));
}

TEST_CASE("Full capacity-one queue wakes producer on space or close", "[workload][queue]") {
  bool closing = false;
  SECTION("pop frees space") { closing = false; }
  SECTION("close rejects waiting producer") { closing = true; }
  std::promise<void> waiting;
  auto entered_wait = waiting.get_future();
  RequestQueue queue(1, [&](WaitReason reason) {
    if (reason == WaitReason::Space) waiting.set_value();
  });
  REQUIRE(queue.push(request(1)));
  std::promise<bool> pushed;
  auto push_result = pushed.get_future();
  std::thread producer([&] { pushed.set_value(queue.push(request(2))); });
  entered_wait.wait(); // Observer runs under the mutex before predicate wait releases it.
  const bool was_blocked = push_result.wait_for(std::chrono::seconds(0)) == std::future_status::timeout;
  Request item;
  bool popped = false;
  if (closing) queue.close();
  else popped = queue.pop(item);
  producer.join(); // No Catch assertions while a joinable thread remains.
  REQUIRE(was_blocked);
  REQUIRE(push_result.get() == !closing);
  REQUIRE(queue.max_depth() == 1);
  if (!closing) {
    REQUIRE(popped);
    REQUIRE(item.id == 1);
  }
  queue.close();
  REQUIRE(queue.pop(item));
  REQUIRE(item.id == (closing ? 1 : 2));
  REQUIRE_FALSE(queue.pop(item));
}

TEST_CASE("Empty queue wakes consumer on work or close", "[workload][queue]") {
  bool closing = false;
  SECTION("push supplies work") { closing = false; }
  SECTION("close ends consumption") { closing = true; }
  std::promise<void> waiting;
  auto entered_wait = waiting.get_future();
  RequestQueue queue(1, [&](WaitReason reason) {
    if (reason == WaitReason::Work) waiting.set_value();
  });
  Request item;
  std::promise<bool> popped;
  auto pop_result = popped.get_future();
  std::thread consumer([&] { popped.set_value(queue.pop(item)); });
  entered_wait.wait();
  const bool was_blocked = pop_result.wait_for(std::chrono::seconds(0)) == std::future_status::timeout;
  bool pushed = false;
  if (closing) queue.close();
  else pushed = queue.push(request(7));
  consumer.join();
  REQUIRE(was_blocked);
  REQUIRE(pop_result.get() == !closing);
  if (!closing) {
    REQUIRE(pushed);
    REQUIRE(item.id == 7);
  }
  queue.close();
}

TEST_CASE("Multiple producers complete every request and persist expected rows", "[workload]") {
  TestDirectory dir;
  Options options{4, 12, 1};
  SECTION("capacity one") { options.capacity = 1; }
  SECTION("larger queue") { options.capacity = 8; }
  const auto result = run(dir.path, options);
  REQUIRE(result.error.empty());
  REQUIRE(result.verified); // Checks all accepted IDs against exactly-once completion.
  REQUIRE(result.attempted == 96);
  REQUIRE(result.accepted == 96);
  REQUIRE(result.processed == 96);
  REQUIRE(result.failed == 0);
  REQUIRE(result.unprocessed == 0);
  REQUIRE(result.max_depth >= 1);
  REQUIRE(result.max_depth <= options.capacity);
  // run() has joined all threads; this reopen cannot race a worker.
  miniwaldb::Db db(dir.path);
  const auto rows = miniwaldb::ItemsTable(db).scan();
  REQUIRE(rows.size() == 48);
  for (std::size_t key = 0; key < rows.size(); ++key) {
    REQUIRE(rows[key].id == static_cast<std::int64_t>(key));
    REQUIRE(rows[key].value == "value-" + std::to_string(key));
  }
}

TEST_CASE("Consumer sync failure closes the queue and joins active producers", "[workload]") {
  TestDirectory dir;
  int sync_calls = 0; // Only the consumer writes; read after run has joined it.
  const auto result = run(dir.path, Options{4, 100, 1}, [&](int) {
    ++sync_calls;
    errno = EIO;
    return -1;
  });
  REQUIRE(sync_calls == 1);
  REQUIRE_FALSE(result.verified);
  REQUIRE_FALSE(result.error.empty());
  REQUIRE(result.processed == 0);
  REQUIRE(result.failed == 1);
  REQUIRE(result.accepted >= 1);
  REQUIRE(result.attempted >= result.accepted);
  REQUIRE(result.attempted < 800);
  REQUIRE(result.accepted == result.processed + result.failed + result.unprocessed);
  REQUIRE(result.max_depth == 1);
  // A failed sync is not rollback; no assertion that the inserted row is absent.
}

TEST_CASE("Consumer startup failure also wakes and joins producers", "[workload]") {
  TestDirectory dir;
  miniwaldb::storage::write_file(dir.path + "/wal.log", {1, 0, 0, 0});
  const auto result = run(dir.path, Options{2, 100, 1});
  REQUIRE_FALSE(result.verified);
  REQUIRE(result.error.find("WAL corruption") != std::string::npos);
  REQUIRE(result.failed == 0); // No request began execution.
  REQUIRE(result.processed == 0);
  REQUIRE(result.unprocessed == result.accepted);
}

TEST_CASE("Workload validates options and handles an empty workload", "[workload]") {
  TestDirectory dir;
  REQUIRE_THROWS(run(dir.path, Options{0, 1, 1}));
  REQUIRE_THROWS(run(dir.path, Options{2, 1, 0}));
  const auto result = run(dir.path, Options{2, 0, 1});
  REQUIRE(result.verified);
  REQUIRE(result.accepted == 0);
  REQUIRE(result.processed == 0);
  REQUIRE(result.max_depth == 0);
}
