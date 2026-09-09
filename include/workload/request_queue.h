#pragma once
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>

namespace miniwaldb::workload {
enum class Operation { Insert, Lookup };
struct Request {
  std::size_t id{};
  Operation operation{Operation::Insert};
  std::int64_t key{};
  std::string value;
};

enum class WaitReason { Space, Work };
// Test observer only: called under the mutex immediately before waiting.
// Must not throw, block, or call back into the queue.
using WaitObserver = std::function<void(WaitReason)>;

class RequestQueue {
public:
  explicit RequestQueue(std::size_t capacity, WaitObserver observer = {});
  bool push(Request request);
  bool pop(Request& request);
  void close();
  std::size_t max_depth() const;
private:
  const std::size_t capacity_;
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::deque<Request> requests_;
  bool closed_{false};
  std::size_t max_depth_{0};
  WaitObserver observer_;
};
}
