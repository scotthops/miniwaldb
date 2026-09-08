#include "db/db.h"
#include "storage/file_io.h"
#include "wal/wal_reader.h"
#include <filesystem>
#include <stdexcept>
#include <utility>
#include <vector>

namespace miniwaldb {

Db::Db(std::string dir, wal::SyncHook sync_hook, wal::WriteHook write_hook)
    : dir_(std::move(dir)), sync_hook_(std::move(sync_hook)), write_hook_(std::move(write_hook)) {
  ensure_dir_();
  snapshot_path_ = (std::filesystem::path(dir_) / "snapshot.dat").string();
  wal_path_ = (std::filesystem::path(dir_) / "wal.log").string();
  load_snapshot_();
  recover_from_wal_();
  open_wal_writer_();
}

void Db::check_usable_() const {
  if (persistence_error_) {
    throw std::runtime_error("database persistence error: destroy and reopen before continuing");
  }
}

void Db::open_wal_writer_() {
  wal_writer_ = std::make_unique<wal::WalWriter>(wal_path_, true, sync_hook_, write_hook_);
}

void Db::append_wal_(const wal::WalRecord& record) {
  try {
    wal_writer_->append(record);
  } catch (...) {
    persistence_error_ = true;
    throw;
  }
}

void Db::ensure_dir_() {
  std::filesystem::create_directories(dir_);
}

void Db::load_snapshot_() {
  kv_ = storage::load_snapshot(snapshot_path_);
}

void Db::begin() {
  check_usable_();
  if (in_tx_) throw std::runtime_error("already in transaction");
  // Prepare the copy before logging BEGIN; copying can fail without starting a transaction.
  auto working = kv_;
  current_txid_ = next_txid_++;
  append_wal_(wal::WalRecord{wal::RecordType::Begin, current_txid_, {}});
  working_kv_.swap(working);
  in_tx_ = true;
}

void Db::commit() {
  check_usable_();
  if (!in_tx_) throw std::runtime_error("not in transaction");
  append_wal_(wal::WalRecord{wal::RecordType::Commit, current_txid_, {}});
  try {
    wal_writer_->flush_on_commit();
  } catch (...) {
    persistence_error_ = true;
    throw;
  }
  kv_.swap(working_kv_);
  working_kv_.clear(); // After the swap, this holds the previous committed state.
  current_txid_ = 0;
  in_tx_ = false;
}

void Db::abort() {
  check_usable_();
  if (!in_tx_) throw std::runtime_error("not in transaction");
  append_wal_(wal::WalRecord{wal::RecordType::Abort, current_txid_, {}});
  working_kv_.clear();
  current_txid_ = 0;
  in_tx_ = false;
}

void Db::checkpoint() {
  check_usable_();
  if (in_tx_) throw std::runtime_error("cannot checkpoint during transaction");
  try {
    storage::save_snapshot(snapshot_path_, kv_);
    wal_writer_.reset();
    storage::write_file(wal_path_, {});
    open_wal_writer_();
  } catch (...) {
    persistence_error_ = true;
    throw;
  }
}

void Db::put(std::int64_t key, std::string value) {
  check_usable_();
  if (!in_tx_) throw std::runtime_error("not in transaction");
  std::vector<std::uint8_t> payload;
  for (int i = 0; i < 8; ++i) {
    payload.push_back(static_cast<std::uint8_t>((static_cast<std::uint64_t>(key) >> (8 * i)) & 0xFF));
  }

  const auto value_len = static_cast<std::uint32_t>(value.size());
  for (int i = 0; i < 4; ++i) {
    payload.push_back(static_cast<std::uint8_t>((value_len >> (8 * i)) & 0xFF));
  }
  payload.insert(payload.end(), value.begin(), value.end());
  append_wal_(wal::WalRecord{wal::RecordType::Set, current_txid_, std::move(payload)});
  try {
    working_kv_[key] = std::move(value);
  } catch (...) {
    // The WAL already contains this operation; memory must not silently diverge.
    persistence_error_ = true;
    throw;
  }
}

std::optional<std::string> Db::get(std::int64_t key) const {
  check_usable_();
  const auto& state = in_tx_ ? working_kv_ : kv_;
  auto it = state.find(key);
  if (it == state.end()) return std::nullopt;
  return it->second;
}

void Db::recover_from_wal_() {
  wal::WalReader reader(wal_path_);
  const auto result = reader.read_all();
  if (result.stop_reason == wal::ReadStopReason::Corruption) {
    throw std::runtime_error("WAL corruption at byte " + std::to_string(result.valid_bytes));
  }

  // Validate all payloads before replay: never silently apply part of a transaction.
  // Reject malformed complete records even in unfinished or orphan transactions.
  for (const auto& rec : result.records) {
    bool valid = true;
    if (rec.type == wal::RecordType::Set) {
      valid = rec.payload.size() >= 12;
      if (valid) {
        std::uint32_t length = 0;
        for (int i = 0; i < 4; ++i) {
          length |= static_cast<std::uint32_t>(rec.payload[8 + i]) << (8 * i);
        }
        valid = length == rec.payload.size() - 12;
      }
    } else if (rec.type == wal::RecordType::Delete) {
      valid = rec.payload.size() == 8;
    } else {
      valid = rec.payload.empty();
    }
    if (!valid) throw std::runtime_error("WAL corruption: malformed payload in transaction " + std::to_string(rec.txid));
  }

  struct PendingOp {
    enum class Type {
      Set,
      Delete
    };

    Type type{};
    std::int64_t key{};
    std::string value;
  };

  using PendingOps = std::vector<PendingOp>;
  std::unordered_map<wal::TxId, PendingOps> pending;

  for (const auto& rec : result.records) {
    if (rec.type == wal::RecordType::Begin) {
      pending[rec.txid];
      if (next_txid_ <= rec.txid) next_txid_ = rec.txid + 1;
      continue;
    }

    if (rec.type == wal::RecordType::Set) {
      auto pending_it = pending.find(rec.txid);
      if (pending_it == pending.end()) continue;
      std::size_t i = 0;
      std::int64_t key = 0;
      for (int k = 0; k < 8; ++k) {
        key |= static_cast<std::int64_t>(static_cast<std::uint64_t>(rec.payload[i++]) << (8 * k));
      }
      std::uint32_t value_len = 0;
      for (int k = 0; k < 4; ++k) {
        value_len |= static_cast<std::uint32_t>(rec.payload[i++]) << (8 * k);
      }
      pending_it->second.push_back(PendingOp{
          PendingOp::Type::Set,
          key,
          std::string(rec.payload.begin() + static_cast<long>(i),
                      rec.payload.begin() + static_cast<long>(i + value_len))});
      continue;
    }

    if (rec.type == wal::RecordType::Delete) {
      auto pending_it = pending.find(rec.txid);
      if (pending_it == pending.end()) continue;
      std::size_t i = 0;
      std::int64_t key = 0;
      for (int k = 0; k < 8; ++k) {
        key |= static_cast<std::int64_t>(static_cast<std::uint64_t>(rec.payload[i++]) << (8 * k));
      }
      pending_it->second.push_back(PendingOp{PendingOp::Type::Delete, key, {}});
      continue;
    }

    if (rec.type == wal::RecordType::Commit) {
      auto it = pending.find(rec.txid);
      if (it != pending.end()) {
        for (const auto& op : it->second) {
          if (op.type == PendingOp::Type::Set) {
            kv_[op.key] = op.value;
          } else {
            kv_.erase(op.key);
          }
        }
        pending.erase(it);
      }
      if (next_txid_ <= rec.txid) next_txid_ = rec.txid + 1;
      continue;
    }

    if (rec.type == wal::RecordType::Abort) {
      pending.erase(rec.txid);
      if (next_txid_ <= rec.txid) next_txid_ = rec.txid + 1;
    }
  }
  // Construction opens the append writer only after replay and repair succeed.
  if (result.stop_reason == wal::ReadStopReason::IncompleteTail) {
    storage::truncate_file(wal_path_, result.valid_bytes);
  }
}

void Db::erase(std::int64_t key) {
  check_usable_();
  if (!in_tx_) throw std::runtime_error("not in transaction");
  std::vector<std::uint8_t> payload;
  for (int i = 0; i < 8; ++i) {
    payload.push_back(static_cast<std::uint8_t>((static_cast<std::uint64_t>(key) >> (8 * i)) & 0xFF));
  }
  append_wal_(wal::WalRecord{wal::RecordType::Delete, current_txid_, std::move(payload)});
  working_kv_.erase(key);
}

} // namespace miniwaldb
