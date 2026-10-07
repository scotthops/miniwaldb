#pragma once
#include "db/db.h"
#include <iosfwd>
#include <string>

namespace miniwaldb {
enum class ShellMode { Human, Machine };

// Returns 0 for quit/exit/EOF, 1 for a fatal database or input failure.
// Machine mode sends readiness and one flushed JSON line per input command.
int run_repl(Db& db, std::istream& input, std::ostream& output, std::ostream& errors,
             ShellMode mode = ShellMode::Human);
// Opens the database and reports startup errors before running the loop.
int run_shell(const std::string& directory, std::istream& input,
              std::ostream& output, std::ostream& errors,
              ShellMode mode = ShellMode::Human);
}
