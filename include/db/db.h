#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

namespace miniwaldb {

class Db {
public:
  explicit Db(std::string dir);

  void begin();
  void commit();
  void abort();

  void put(std::int64_t key, std::string value);
  std::optional<std::string> get(std::int64_t key) const;

private:
  std::string dir_;
  std::unordered_map<std::int64_t, std::string> kv_;

  bool in_tx_{false};

  void ensure_dir_();
};

} // namespace miniwaldb
