#include "table/items_table.h"
#include <algorithm>
#include <stdexcept>
#include <utility>

namespace miniwaldb {

void ItemsTable::insert(std::int64_t id, std::string value) {
  if (db_.get(id)) throw std::runtime_error("item already exists");
  db_.put(id, std::move(value));
}

void ItemsTable::update(std::int64_t id, std::string value) {
  if (!db_.get(id)) throw std::runtime_error("item does not exist");
  db_.put(id, std::move(value));
}

void ItemsTable::erase(std::int64_t id) {
  if (!db_.get(id)) throw std::runtime_error("item does not exist");
  db_.erase(id);
}

std::optional<ItemRow> ItemsTable::lookup(std::int64_t id) const {
  auto value = db_.get(id);
  if (!value) return std::nullopt;
  return ItemRow{id, std::move(*value)};
}

std::vector<ItemRow> ItemsTable::scan() const {
  std::vector<ItemRow> rows;
  for (auto& [id, value] : db_.entries()) {
    rows.push_back(ItemRow{id, std::move(value)});
  }
  std::sort(rows.begin(), rows.end(), [](const ItemRow& a, const ItemRow& b) {
    return a.id < b.id;
  });
  return rows;
}

std::vector<ItemRow> ItemsTable::select_value(const std::string& value) const {
  std::vector<ItemRow> matches;
  for (const auto& row : scan()) {
    if (row.value == value) matches.push_back(row);
  }
  return matches;
}

std::vector<std::int64_t> project_ids(const std::vector<ItemRow>& rows) {
  std::vector<std::int64_t> ids;
  for (const auto& row : rows) ids.push_back(row.id);
  return ids;
}

std::vector<std::string> project_values(const std::vector<ItemRow>& rows) {
  std::vector<std::string> values;
  for (const auto& row : rows) values.push_back(row.value);
  return values;
}

} // namespace miniwaldb
