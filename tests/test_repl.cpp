#include <catch2/catch_test_macros.hpp>
#include "repl.h"
#include "storage/file_io.h"
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <stdexcept>

namespace {
struct TestDirectory {
  std::string path;
  TestDirectory() {
    auto pattern = (std::filesystem::temp_directory_path() / "miniwaldb-repl-XXXXXX").string();
    if (!::mkdtemp(pattern.data())) throw std::runtime_error("cannot create test directory");
    path = std::move(pattern);
  }
  ~TestDirectory() { std::filesystem::remove_all(path); }
};
}

TEST_CASE("REPL demonstrates relational commands and recovery", "[repl]") {
  TestDirectory dir;
  std::istringstream input(
      "begin\ninsert 9 hello world\ninsert 1 apple\ninsert 4 blue\ncommit\n"
      "get 9\nscan\nselect-value hello world\nids\nvalues\n"
      "begin\nupdate 4 dark blue\ndelete 1\ncommit\ncheckpoint\nexit\n");
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_shell(dir.path, input, output, errors) == 0);
  REQUIRE(errors.str().empty());
  REQUIRE(output.str().find("> 9 | hello world\n") != std::string::npos);
  REQUIRE(output.str().find("> 1 | apple\n4 | blue\n9 | hello world\n") != std::string::npos);
  REQUIRE(output.str().find("> 1\n4\n9\n") != std::string::npos);
  REQUIRE(output.str().find("> apple\nblue\nhello world\n") != std::string::npos);
  std::istringstream restarted("get 1\nget 4\nscan\nselect-value dark blue\nselect-value absent\n");
  std::ostringstream reopened_output, reopened_errors;
  REQUIRE(miniwaldb::run_shell(dir.path, restarted, reopened_output, reopened_errors) == 0);
  REQUIRE(reopened_errors.str().empty());
  REQUIRE(reopened_output.str().find("> not found\n") != std::string::npos);
  REQUIRE(reopened_output.str().find("> 4 | dark blue\n9 | hello world\n") != std::string::npos);
  REQUIRE(reopened_output.str().find("> 4 | dark blue\n> (no rows)\n") != std::string::npos);
}

TEST_CASE("REPL rejects malformed input before performing operations", "[repl]") {
  TestDirectory dir;
  miniwaldb::Db db(dir.path);
  db.begin();
  db.put(4, "original");
  db.commit();
  db.begin();
  const auto before = miniwaldb::storage::read_file(dir.path + "/wal.log");
  const std::vector<std::string> bad_commands{
    "insert", "insert abc value", "insert 4x text", "insert 1.5 text",
    "insert 9223372036854775808 text", "insert -9223372036854775809 text",
    "insert +2 text", "insert 8   ", "update 4", "delete", "delete banana",
    "get", "get 4 extra", "delete 4 extra", "begin extra", "commit extra",
    "abort extra", "checkpoint extra", "scan extra", "ids extra", "values extra",
    "help extra", "quit extra", "exit extra", "select-value", "put 4 bypass", "unknown"
  };
  for (const auto& command : bad_commands) {
    INFO(command);
    std::istringstream input(command + "\nget 4\n");
    std::ostringstream output, errors;
    REQUIRE(miniwaldb::run_repl(db, input, output, errors) == 0);
    REQUIRE(errors.str().find("error:") != std::string::npos);
    REQUIRE(output.str().find("4 | original") != std::string::npos);
    REQUIRE(miniwaldb::storage::read_file(dir.path + "/wal.log") == before);
  }
  db.abort();
}

TEST_CASE("REPL reports constraint and transaction errors and continues", "[repl]") {
  TestDirectory dir;
  std::istringstream input(
      "insert 1 outside\ncommit\nabort\nbegin\nbegin\ninsert 1 first\n"
      "insert 1 duplicate\nupdate 2 missing\ndelete 2\ncheckpoint\n"
      "update 1 accepted\ncommit\nget 1\n");
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_shell(dir.path, input, output, errors) == 0);
  REQUIRE(output.str().find("1 | accepted") != std::string::npos);
  for (const auto* message : {"not in transaction", "already in transaction", "item already exists",
                             "item does not exist", "cannot checkpoint during transaction"}) {
    REQUIRE(errors.str().find(message) != std::string::npos);
  }
}

TEST_CASE("REPL preserves text spaces and supports signed key boundaries", "[repl]") {
  TestDirectory dir;
  std::istringstream input(
      "  \nhelp\nbegin\ninsert -9223372036854775808   dark  blue  \n"
      "insert 9223372036854775807 max\ncommit\n"
      "select-value dark  blue  \nselect-value dark blue\nget 9223372036854775807\n");
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_shell(dir.path, input, output, errors) == 0);
  REQUIRE(errors.str().empty());
  REQUIRE(output.str().find("insert ID TEXT") != std::string::npos);
  REQUIRE(output.str().find("> -9223372036854775808 | dark  blue  \n> (no rows)\n") != std::string::npos);
  REQUIRE(output.str().find("9223372036854775807 | max") != std::string::npos);
}

TEST_CASE("EOF quit and exit never commit pending shell changes", "[repl]") {
  TestDirectory dir;
  std::string ending;
  SECTION("EOF") { ending = ""; }
  SECTION("quit") { ending = "quit\ncommit\n"; }
  SECTION("exit") { ending = "exit\ncommit\n"; }
  std::istringstream input("begin\ninsert 1 committed\ncommit\nbegin\nupdate 1 pending\ninsert 2 pending\n" + ending);
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_shell(dir.path, input, output, errors) == 0);
  REQUIRE(errors.str().empty());
  miniwaldb::Db reopened(dir.path);
  REQUIRE(reopened.get(1) == "committed");
  REQUIRE_FALSE(reopened.get(2).has_value());
}

TEST_CASE("REPL exits after a persistence failure without running more commands", "[repl]") {
  TestDirectory dir;
  miniwaldb::Db db(dir.path, [](int) { errno = EIO; return -1; });
  std::istringstream input("begin\ninsert 1 uncertain\ncommit\nget 1\n");
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_repl(db, input, output, errors) == 1);
  REQUIRE(errors.str().find("fatal database error:") != std::string::npos);
  REQUIRE(db.has_persistence_error());
  std::string remaining;
  std::getline(input, remaining);
  REQUIRE(remaining == "get 1");
}

TEST_CASE("Shell reports corruption at startup without entering the REPL", "[repl]") {
  TestDirectory dir;
  miniwaldb::storage::write_file(dir.path + "/wal.log", {1, 0, 0, 0}); // Impossible frame length.
  const auto before = miniwaldb::storage::read_file(dir.path + "/wal.log");
  std::istringstream input("begin\n");
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_shell(dir.path, input, output, errors) == 1);
  REQUIRE(errors.str().find("WAL corruption") != std::string::npos);
  REQUIRE(output.str().empty());
  REQUIRE(miniwaldb::storage::read_file(dir.path + "/wal.log") == before);
}
