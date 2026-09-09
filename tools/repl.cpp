#include "repl.h"
#include "table/items_table.h"
#include <charconv>
#include <istream>
#include <ostream>
#include <sstream>
#include <stdexcept>

namespace miniwaldb {
namespace {
void no_more_arguments(std::istringstream& input) {
  std::string extra;
  if (input >> extra) throw std::runtime_error("unexpected arguments");
}

std::int64_t read_id(std::istringstream& input) {
  std::string token;
  if (!(input >> token)) throw std::runtime_error("missing id");
  std::int64_t id = 0;
  const auto result = std::from_chars(token.data(), token.data() + token.size(), id);
  if (result.ec != std::errc{} || result.ptr != token.data() + token.size()) {
    throw std::runtime_error("invalid id: expected a signed 64-bit decimal integer");
  }
  return id;
}

std::string read_value(std::istringstream& input) {
  std::string value;
  std::getline(input >> std::ws, value);
  if (value.empty()) throw std::runtime_error("missing value: enter non-whitespace text");
  return value;
}

void print_row(const ItemRow& row, std::ostream& output) {
  output << row.id << " | " << row.value << '\n';
}

void print_rows(const std::vector<ItemRow>& rows, std::ostream& output) {
  if (rows.empty()) output << "(no rows)\n";
  for (const auto& row : rows) print_row(row, output);
}

void print_help(std::ostream& output) {
  output << "begin | commit | abort | checkpoint\n"
            "insert ID TEXT | update ID TEXT | delete ID | get ID\n"
            "scan | select-value TEXT | ids | values\n"
            "help | quit | exit\n"
            "Mutations require begin, then commit or abort. TEXT is the rest of the line.\n"
            "TEXT must be nonempty; no quoting or escapes. Exit never commits pending work.\n";
}
} // namespace

int run_repl(Db& db, std::istream& input, std::ostream& output, std::ostream& errors) {
  ItemsTable items(db);
  output << "miniwaldb: items(id, value). Type help for commands.\n";
  std::string line;
  while (true) {
    output << "> " << std::flush;
    if (!std::getline(input, line)) {
      if (input.eof()) return 0;
      errors << "fatal input error\n";
      return 1;
    }
    std::istringstream command_input(line);
    std::string command;
    if (!(command_input >> command)) continue;
    try {
      if (command == "insert" || command == "update") {
        const auto id = read_id(command_input);
        auto value = read_value(command_input);
        if (command == "insert") items.insert(id, std::move(value));
        else items.update(id, std::move(value));
        output << "ok\n";
      } else if (command == "get" || command == "delete") {
        const auto id = read_id(command_input);
        no_more_arguments(command_input);
        if (command == "delete") {
          items.erase(id);
          output << "ok\n";
        } else {
          const auto row = items.lookup(id);
          if (row) print_row(*row, output);
          else output << "not found\n";
        }
      } else if (command == "select-value") {
        print_rows(items.select_value(read_value(command_input)), output);
      } else if (command == "begin" || command == "commit" || command == "abort" ||
                 command == "checkpoint") {
        no_more_arguments(command_input);
        if (command == "begin") db.begin();
        else if (command == "commit") db.commit();
        else if (command == "abort") db.abort();
        else db.checkpoint();
        output << "ok\n";
      } else if (command == "scan" || command == "ids" || command == "values") {
        no_more_arguments(command_input);
        const auto rows = items.scan();
        if (command == "scan") print_rows(rows, output);
        else {
          if (rows.empty()) output << "(no rows)\n";
          if (command == "ids") {
            for (auto id : project_ids(rows)) output << id << '\n';
          } else {
            for (const auto& value : project_values(rows)) output << value << '\n';
          }
        }
      } else if (command == "help") {
        no_more_arguments(command_input);
        print_help(output);
      } else if (command == "quit" || command == "exit") {
        no_more_arguments(command_input);
        return 0;
      } else {
        throw std::runtime_error("unknown command: " + command + " (type help)");
      }
    } catch (const std::exception& e) {
      if (db.has_persistence_error()) {
        errors << "fatal database error: " << e.what() << "; close and reopen before continuing\n";
        return 1;
      }
      errors << "error: " << e.what() << '\n';
    }
  }
}

int run_shell(const std::string& directory, std::istream& input,
              std::ostream& output, std::ostream& errors) {
  try {
    Db db(directory);
    return run_repl(db, input, output, errors);
  } catch (const std::exception& e) {
    errors << "fatal database startup/session error: " << e.what() << '\n';
    return 1;
  }
}
} // namespace miniwaldb
