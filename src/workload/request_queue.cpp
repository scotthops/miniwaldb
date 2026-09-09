#include "workload/request_queue.h"
#include <algorithm>
#include <stdexcept>
#include <utility>

namespace miniwaldb::workload {
RequestQueue::RequestQueue(std::size_t capacity, WaitObserver observer)
    : capacity_(capacity), observer_(std::move(observer)) {
  if (capacity == 0) throw std::invalid_argument("queue capacity must be positive");
}

bool RequestQueue::push(Request request) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!closed_ && requests_.size() == capacity_ && observer_) observer_(WaitReason::Space);
  not_full_.wait(lock, [&] { return closed_ || requests_.size() < capacity_; });
  if (closed_) return false;
  requests_.push_back(std::move(request));
  max_depth_ = std::max(max_depth_, requests_.size());
  lock.unlock();
  not_empty_.notify_one();
  return true;
}

bool RequestQueue::pop(Request& request) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!closed_ && requests_.empty() && observer_) observer_(WaitReason::Work);
  not_empty_.wait(lock, [&] { return closed_ || !requests_.empty(); });
  if (requests_.empty()) return false; // Only closed + empty ends normal consumption.
  request = std::move(requests_.front());
  requests_.pop_front();
  lock.unlock();
  not_full_.notify_one();
  return true; // No queue lock is held while the caller handles this request.
}

void RequestQueue::close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true; // Keep accepted work available for draining.
  }
  not_empty_.notify_all();
  not_full_.notify_all();
}

std::size_t RequestQueue::max_depth() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return max_depth_;
}
}
