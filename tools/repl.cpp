#include "repl.h"
#include "table/items_table.h"
#include <charconv>
#include <istream>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

namespace miniwaldb {
namespace {
// The protocol carries UTF-8 text. Escape JSON punctuation and control bytes;
// leave UTF-8 multibyte sequences intact. This is output encoding, not WAL encoding.
std::string json_string(const std::string& value) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result = "\"";
  for (unsigned char byte : value) {
    if (byte == '"' || byte == '\\') {
      result += '\\';
      result += static_cast<char>(byte);
    } else if (byte < 0x20) {
      result += "\\u00";
      result += hex[byte >> 4];
      result += hex[byte & 0x0f];
    } else {
      result += static_cast<char>(byte);
    }
  }
  result += '"';
  return result;
}

void print_json_row(const ItemRow& row, std::ostream& output) {
  output << "{\"id\":" << row.id << ",\"value\":" << json_string(row.value) << '}';
}

void print_json_rows(const std::vector<ItemRow>& rows, std::ostream& output) {
  output << "{\"status\":\"ok\",\"rows\":[";
  for (std::size_t i = 0; i < rows.size(); ++i) {
    if (i) output << ',';
    print_json_row(rows[i], output);
  }
  output << "]}";
}

void report_error(std::ostream& output, std::ostream& errors,
                  const std::string& message, bool fatal, ShellMode mode) {
  errors << message << '\n';
  if (mode == ShellMode::Machine) {
    output << "{\"status\":\"" << (fatal ? "fatal" : "error")
           << "\",\"message\":" << json_string(message) << "}\n" << std::flush;
  }
}

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

int run_repl(Db& db, std::istream& input, std::ostream& output, std::ostream& errors,
             ShellMode mode) {
  ItemsTable items(db);
  const bool machine = mode == ShellMode::Machine;
  if (machine) {
    output << "{\"status\":\"ready\",\"protocol\":1,\"pid\":" << ::getpid()
           << "}\n" << std::flush;
  } else {
    output << "miniwaldb: items(id, value). Type help for commands.\n";
  }
  std::string line;
  while (true) {
    if (!machine) output << "> " << std::flush;
    if (!output) {
      errors << "fatal output error\n";
      return 1;
    }
    if (!std::getline(input, line)) {
      if (input.eof()) return 0;
      report_error(output, errors, "fatal input error", true, mode);
      return 1;
    }
    // Windows StreamWriter.WriteLine commonly sends CRLF through WSL pipes.
    // Only machine mode treats the final CR as part of the line terminator.
    if (machine && !line.empty() && line.back() == '\r') line.pop_back();
    std::istringstream command_input(line);
    std::string command;
    if (!(command_input >> command)) {
      if (machine) report_error(output, errors, "error: empty command", false, mode);
      continue;
    }
    try {
      // Buffer just this response, so serialization errors cannot emit half a line.
      // Both modes execute the same command against the same borrowed Db.
      std::ostringstream response;
      bool exiting = false;
      if (command == "insert" || command == "update") {
        const auto id = read_id(command_input);
        auto value = read_value(command_input);
        if (command == "insert") items.insert(id, std::move(value));
        else items.update(id, std::move(value));
        response << (machine ? "{\"status\":\"ok\"}" : "ok\n");
      } else if (command == "get" || command == "delete") {
        const auto id = read_id(command_input);
        no_more_arguments(command_input);
        if (command == "delete") {
          items.erase(id);
          response << (machine ? "{\"status\":\"ok\"}" : "ok\n");
        } else {
          const auto row = items.lookup(id);
          if (machine) {
            response << "{\"status\":\"ok\",\"row\":";
            if (row) print_json_row(*row, response);
            else response << "null";
            response << '}';
          } else if (row) print_row(*row, response);
          else response << "not found\n";
        }
      } else if (command == "select-value") {
        const auto rows = items.select_value(read_value(command_input));
        if (machine) print_json_rows(rows, response);
        else print_rows(rows, response);
      } else if (command == "begin" || command == "commit" || command == "abort" ||
                 command == "checkpoint") {
        no_more_arguments(command_input);
        if (command == "begin") db.begin();
        else if (command == "commit") db.commit();
        else if (command == "abort") db.abort();
        else db.checkpoint();
        // In particular, commit() has returned (including WAL sync) before success.
        response << (machine ? "{\"status\":\"ok\"}" : "ok\n");
      } else if (command == "scan" || command == "ids" || command == "values") {
        no_more_arguments(command_input);
        const auto rows = items.scan();
        if (machine) {
          if (command == "scan") print_json_rows(rows, response);
          else {
            response << "{\"status\":\"ok\",\"" << command << "\":[";
            for (std::size_t i = 0; i < rows.size(); ++i) {
              if (i) response << ',';
              if (command == "ids") response << rows[i].id;
              else response << json_string(rows[i].value);
            }
            response << "]}";
          }
        } else if (command == "scan") print_rows(rows, response);
        else {
          if (rows.empty()) response << "(no rows)\n";
          if (command == "ids") {
            for (auto id : project_ids(rows)) response << id << '\n';
          } else {
            for (const auto& value : project_values(rows)) response << value << '\n';
          }
        }
      } else if (command == "help") {
        no_more_arguments(command_input);
        if (machine) {
          std::ostringstream text;
          print_help(text);
          response << "{\"status\":\"ok\",\"text\":" << json_string(text.str()) << '}';
        } else print_help(response);
      } else if (command == "quit" || command == "exit") {
        no_more_arguments(command_input);
        exiting = true;
        if (machine) response << "{\"status\":\"bye\"}";
      } else {
        throw std::runtime_error("unknown command: " + command + " (type help)");
      }
      output << response.str();
      if (machine) output << '\n' << std::flush;
      if (!output) {
        errors << "fatal output error\n";
        return 1;
      }
      if (exiting) return 0;
    } catch (const std::exception& e) {
      if (db.has_persistence_error()) {
        report_error(output, errors,
                     std::string("fatal database error: ") + e.what() +
                         "; close and reopen before continuing", true, mode);
        return 1;
      }
      report_error(output, errors, std::string("error: ") + e.what(), false, mode);
    }
  }
}

int run_shell(const std::string& directory, std::istream& input,
              std::ostream& output, std::ostream& errors, ShellMode mode) {
  try {
    Db db(directory);
    return run_repl(db, input, output, errors, mode);
  } catch (const std::exception& e) {
    report_error(output, errors,
                 std::string("fatal database startup/session error: ") + e.what(), true, mode);
    return 1;
  }
}
} // namespace miniwaldb
