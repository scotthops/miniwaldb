#include "db/db.h"
#include "wal/wal_reader.h"
#include <filesystem>
#include <stdexcept>
#include <utility>
#include <vector>

namespace miniwaldb {

Db::Db(std::string dir) : dir_(std::move(dir)) {
  ensure_dir_();
  wal_path_ = (std::filesystem::path(dir_) / "wal.log").string();
  recover_from_wal_();
  wal_writer_ = std::make_unique<wal::WalWriter>(wal_path_);
}

void Db::ensure_dir_() {
  std::filesystem::create_directories(dir_);
}

void Db::begin() {
  if (in_tx_) throw std::runtime_error("already in transaction");
  current_txid_ = next_txid_++;
  wal_writer_->append(wal::WalRecord{wal::RecordType::Begin, current_txid_, {}});
  in_tx_ = true;
}

void Db::commit() {
  if (!in_tx_) throw std::runtime_error("not in transaction");
  wal_writer_->append(wal::WalRecord{wal::RecordType::Commit, current_txid_, {}});
  wal_writer_->flush_on_commit();
  current_txid_ = 0;
  in_tx_ = false;
}

void Db::abort() {
  if (!in_tx_) throw std::runtime_error("not in transaction");
  wal_writer_->append(wal::WalRecord{wal::RecordType::Abort, current_txid_, {}});
  current_txid_ = 0;
  in_tx_ = false;
}

void Db::put(std::int64_t key, std::string value) {
  if (in_tx_) {
    std::vector<std::uint8_t> payload;
    const auto* key_bytes = reinterpret_cast<const std::uint8_t*>(&key);
    payload.insert(payload.end(), key_bytes, key_bytes + sizeof(key));

    const auto value_len = static_cast<std::uint32_t>(value.size());
    for (int i = 0; i < 4; ++i) {
      payload.push_back(static_cast<std::uint8_t>((value_len >> (8 * i)) & 0xFF));
    }
    payload.insert(payload.end(), value.begin(), value.end());
    wal_writer_->append(wal::WalRecord{wal::RecordType::Set, current_txid_, std::move(payload)});
  }
  kv_[key] = std::move(value);
}

std::optional<std::string> Db::get(std::int64_t key) const {
  auto it = kv_.find(key);
  if (it == kv_.end()) return std::nullopt;
  return it->second;
}

void Db::recover_from_wal_() {
  wal::WalReader reader(wal_path_);
  const auto records = reader.read_all();

  using PendingOps = std::vector<std::pair<std::int64_t, std::string>>;
  std::unordered_map<wal::TxId, PendingOps> pending;

  for (const auto& rec : records) {
    if (rec.type == wal::RecordType::Begin) {
      pending[rec.txid];
      if (next_txid_ <= rec.txid) next_txid_ = rec.txid + 1;
      continue;
    }

    if (rec.type == wal::RecordType::Set) {
      auto pending_it = pending.find(rec.txid);
      if (pending_it == pending.end()) continue;
      if (rec.payload.size() < 12) continue;
      std::size_t i = 0;
      std::int64_t key = 0;
      for (int k = 0; k < 8; ++k) {
        key |= static_cast<std::int64_t>(static_cast<std::uint64_t>(rec.payload[i++]) << (8 * k));
      }
      std::uint32_t value_len = 0;
      for (int k = 0; k < 4; ++k) {
        value_len |= static_cast<std::uint32_t>(rec.payload[i++]) << (8 * k);
      }
      if (i + value_len > rec.payload.size()) continue;
      pending_it->second.emplace_back(
          key,
          std::string(rec.payload.begin() + static_cast<long>(i),
                      rec.payload.begin() + static_cast<long>(i + value_len)));
      continue;
    }

    if (rec.type == wal::RecordType::Commit) {
      auto it = pending.find(rec.txid);
      if (it != pending.end()) {
        for (const auto& op : it->second) {
          kv_[op.first] = op.second;
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
}

} // namespace miniwaldb
