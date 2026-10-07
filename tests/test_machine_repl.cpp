#include <catch2/catch_test_macros.hpp>
#include "repl.h"
#include "storage/file_io.h"
#include "wal/wal_reader.h"
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

namespace {
struct TestDirectory {
  std::string path;
  TestDirectory() {
    auto pattern = (std::filesystem::temp_directory_path() / "miniwaldb-machine-XXXXXX").string();
    if (!::mkdtemp(pattern.data())) throw std::runtime_error("cannot create test directory");
    path = std::move(pattern);
  }
  ~TestDirectory() { std::filesystem::remove_all(path); }
};
constexpr auto machine = miniwaldb::ShellMode::Machine;
}

TEST_CASE("Machine mode shares transactions and structured query results", "[machine]") {
  TestDirectory dir;
  std::istringstream input(
      "begin\ninsert 1 10\ninsert 2 0\ncommit\nbegin\nupdate 1 7\nget 1\n"
      "abort\nget 1\nget 99\nscan\nselect-value 0\nids\nvalues\nhelp\nquit\n");
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_shell(dir.path, input, output, errors, machine) == 0);
  REQUIRE(errors.str().empty());
  const auto text = output.str();
  REQUIRE(text.find("{\"status\":\"ready\",\"protocol\":1,\"pid\":") == 0);
  REQUIRE(text.find("{\"status\":\"ok\",\"row\":{\"id\":1,\"value\":\"7\"}}\n") != std::string::npos);
  REQUIRE(text.find("{\"status\":\"ok\",\"row\":{\"id\":1,\"value\":\"10\"}}\n") != std::string::npos);
  REQUIRE(text.find("{\"status\":\"ok\",\"row\":null}\n") != std::string::npos);
  REQUIRE(text.find("{\"status\":\"ok\",\"rows\":[{\"id\":1,\"value\":\"10\"},{\"id\":2,\"value\":\"0\"}]}\n") != std::string::npos);
  REQUIRE(text.find("{\"status\":\"ok\",\"rows\":[{\"id\":2,\"value\":\"0\"}]}\n") != std::string::npos);
  REQUIRE(text.find("{\"status\":\"ok\",\"ids\":[1,2]}\n") != std::string::npos);
  REQUIRE(text.find("{\"status\":\"ok\",\"values\":[\"10\",\"0\"]}\n") != std::string::npos);
  REQUIRE(text.find("{\"status\":\"ok\",\"text\":\"begin | commit | abort | checkpoint\\u000a") != std::string::npos);
  REQUIRE(text.find("> ") == std::string::npos);
  REQUIRE(text.ends_with("{\"status\":\"bye\"}\n"));
  miniwaldb::Db reopened(dir.path);
  REQUIRE(reopened.get(1) == "10");
  REQUIRE(reopened.get(2) == "0");
}

TEST_CASE("Machine JSON escapes stored control characters and preserves UTF-8", "[machine]") {
  TestDirectory dir;
  miniwaldb::Db db(dir.path);
  std::string value = "\"\\\n\r\t\b\f";
  value.push_back('\0');
  value += " café";
  db.begin();
  db.put(1, value);
  db.commit();
  std::istringstream input("get 1\n");
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_repl(db, input, output, errors, machine) == 0);
  REQUIRE(errors.str().empty());
  REQUIRE(output.str().find(
      "{\"status\":\"ok\",\"row\":{\"id\":1,\"value\":\"\\\"\\\\\\u000a\\u000d\\u0009\\u0008\\u000c\\u0000 café\"}}\n") != std::string::npos);
}

TEST_CASE("Machine command errors preserve the session and active transaction", "[machine]") {
  TestDirectory dir;
  std::istringstream input("\nbegin\ninsert 1 10\ninsert 1 duplicate\nget 1 extra\n"
                           "unknown\"\\\nget 1\nabort\nget 1\nexit\n");
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_shell(dir.path, input, output, errors, machine) == 0);
  REQUIRE(output.str().find("{\"status\":\"error\",\"message\":\"error: empty command\"}\n") != std::string::npos);
  REQUIRE(output.str().find("{\"status\":\"error\",\"message\":\"error: item already exists\"}\n") != std::string::npos);
  REQUIRE(output.str().find("unknown\\\"\\\\ (type help)") != std::string::npos);
  REQUIRE(output.str().find("{\"status\":\"ok\",\"row\":{\"id\":1,\"value\":\"10\"}}\n") != std::string::npos);
  REQUIRE(output.str().find("{\"status\":\"ok\",\"row\":null}\n") != std::string::npos);
  REQUIRE(errors.str().find("error: unexpected arguments") != std::string::npos);
}

TEST_CASE("Machine commit acknowledgment follows synchronization", "[machine][persistence]") {
  TestDirectory dir;
  std::ostringstream output, errors;
  bool synchronized = false;
  miniwaldb::Db db(dir.path, [&](int fd) {
    // BEGIN and both INSERTs are acknowledged; COMMIT is not yet acknowledged.
    const auto text = output.str();
    const std::string ok = "{\"status\":\"ok\"}\n";
    const auto first = text.find(ok);
    REQUIRE(first != std::string::npos);
    const auto second = text.find(ok, first + ok.size());
    REQUIRE(second != std::string::npos);
    const auto third = text.find(ok, second + ok.size());
    REQUIRE(third != std::string::npos);
    REQUIRE(text.find(ok, third + ok.size()) == std::string::npos);
    const auto records = miniwaldb::wal::WalReader(dir.path + "/wal.log").read_all().records;
    REQUIRE(records.back().type == miniwaldb::wal::RecordType::Commit);
    const int result = ::fdatasync(fd);
    synchronized = result == 0;
    return result;
  });
  std::istringstream input("begin\ninsert 1 10\ninsert 2 0\ncommit\n");
  REQUIRE(miniwaldb::run_repl(db, input, output, errors, machine) == 0);
  REQUIRE(synchronized);
  REQUIRE(errors.str().empty());
  REQUIRE(output.str().ends_with("{\"status\":\"ok\"}\n"));
}

TEST_CASE("Machine persistence failure sends fatal instead of commit success", "[machine][persistence]") {
  TestDirectory dir;
  {
    miniwaldb::Db db(dir.path, [](int) { errno = EIO; return -1; });
    std::istringstream input("begin\ninsert 1 uncertain\ncommit\nget 1\n");
    std::ostringstream output, errors;
    REQUIRE(miniwaldb::run_repl(db, input, output, errors, machine) == 1);
    REQUIRE(db.has_persistence_error());
    REQUIRE(output.str().find("{\"status\":\"fatal\",\"message\":\"fatal database error:") != std::string::npos);
    REQUIRE_FALSE(output.str().ends_with("{\"status\":\"ok\"}\n"));
    REQUIRE(errors.str().find("fatal database error:") != std::string::npos);
    std::string remaining;
    std::getline(input, remaining);
    REQUIRE(remaining == "get 1");
  }
  // The complete COMMIT remains: the observed error was not a rollback guarantee.
  miniwaldb::Db reopened(dir.path);
  REQUIRE(reopened.get(1) == "uncertain");
}

TEST_CASE("Machine startup corruption sends fatal without readiness or modification", "[machine]") {
  TestDirectory dir;
  const std::vector<std::uint8_t> corrupt{1, 0, 0, 0};
  miniwaldb::storage::write_file(dir.path + "/wal.log", corrupt);
  std::istringstream input("begin\n");
  std::ostringstream output, errors;
  REQUIRE(miniwaldb::run_shell(dir.path, input, output, errors, machine) == 1);
  REQUIRE(output.str().find("{\"status\":\"fatal\"") == 0);
  REQUIRE(output.str().find("ready") == std::string::npos);
  REQUIRE(errors.str().find("WAL corruption") != std::string::npos);
  REQUIRE(miniwaldb::storage::read_file(dir.path + "/wal.log") == corrupt);
}
