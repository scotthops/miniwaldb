#pragma once
#include "db/db.h"
#include <vector>

namespace miniwaldb {

// Fixed logical schema: items(id INTEGER PRIMARY KEY, value TEXT).
struct ItemRow {
  std::int64_t id;
  std::string value;

  bool operator==(const ItemRow& other) const {
    return id == other.id && value == other.value;
  }
};

// Non-owning table view: Db must outlive this object. Transactions belong to Db.
class ItemsTable {
public:
  explicit ItemsTable(Db& db) : db_(db) {}

  // Mutations require an active Db transaction. Duplicate insert and missing
  // update/delete are errors. Constraint errors leave the transaction usable.
  void insert(std::int64_t id, std::string value);
  void update(std::int64_t id, std::string value);
  void erase(std::int64_t id);
  std::optional<ItemRow> lookup(std::int64_t id) const;

  // Both-column results, ordered by ascending primary key.
  std::vector<ItemRow> scan() const;
  std::vector<ItemRow> select_value(const std::string& value) const;

private:
  Db& db_;
};

// Project any row result (including a selection). Preserve input order and
// duplicates, like SQL SELECT without DISTINCT; these functions do no I/O.
std::vector<std::int64_t> project_ids(const std::vector<ItemRow>& rows);
std::vector<std::string> project_values(const std::vector<ItemRow>& rows);

} // namespace miniwaldb
