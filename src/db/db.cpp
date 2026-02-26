#include "db/db.h"
#include <filesystem>
#include <stdexcept>

namespace miniwaldb {

Db::Db(std::string dir) : dir_(std::move(dir)) {
  ensure_dir_();
}

void Db::ensure_dir_() {
  std::filesystem::create_directories(dir_);
}

void Db::begin() {
  if (in_tx_) throw std::runtime_error("already in transaction");
  in_tx_ = true;
}

void Db::commit() {
  if (!in_tx_) throw std::runtime_error("not in transaction");
  // WAL + fsync will go here soon.
  in_tx_ = false;
}

void Db::abort() {
  if (!in_tx_) throw std::runtime_error("not in transaction");
  // rollback will be added once WAL is real
  in_tx_ = false;
}

void Db::put(std::int64_t key, std::string value) {
  kv_[key] = std::move(value);
}

std::optional<std::string> Db::get(std::int64_t key) const {
  auto it = kv_.find(key);
  if (it == kv_.end()) return std::nullopt;
  return it->second;
}

} // namespace miniwaldb
