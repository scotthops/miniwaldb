#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <memory>
#include "wal/wal_writer.h"

namespace miniwaldb {

class Db {
public:
  explicit Db(std::string dir);

  void begin();
  void commit();
  void abort();

  void put(std::int64_t key, std::string value);
  void erase(std::int64_t key);
  std::optional<std::string> get(std::int64_t key) const;

private:
  std::string dir_;
  std::unordered_map<std::int64_t, std::string> kv_;
  std::string snapshot_path_;
  std::string wal_path_;
  std::unique_ptr<wal::WalWriter> wal_writer_;

  bool in_tx_{false};
  wal::TxId next_txid_{1};
  wal::TxId current_txid_{0};

  void ensure_dir_();
  void load_snapshot_();
  void recover_from_wal_();
};

} // namespace miniwaldb
