#include "workload/workload.h"
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {
std::size_t positive_number(const char* argument) {
  const std::string token(argument);
  std::size_t value = 0;
  const auto parsed = std::from_chars(token.data(), token.data() + token.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != token.data() + token.size() || value == 0) {
    throw std::invalid_argument("arguments must be positive integers");
  }
  return value;
}
}
int main(int argc, char** argv) {
  try {
    if (argc > 3) throw std::invalid_argument("usage: miniwaldb_workload [keys-per-producer] [capacity]");
    miniwaldb::workload::Options options;
    if (argc > 1) options.keys_per_producer = positive_number(argv[1]);
    if (argc > 2) options.capacity = positive_number(argv[2]);
    auto directory = (std::filesystem::temp_directory_path() / "miniwaldb-workload-XXXXXX").string();
    if (!::mkdtemp(directory.data())) throw std::runtime_error("cannot create workload directory");
    std::cout << "database: " << directory << '\n';
    const auto start = std::chrono::steady_clock::now();
    const auto result = miniwaldb::workload::run(directory, options);
    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    std::cout << "producers: " << options.producers << "\nrequests attempted: " << result.attempted
              << "\nrequests accepted: " << result.accepted << "\nrequests processed: " << result.processed
              << "\nfailed: " << result.failed << "\nunprocessed accepted: " << result.unprocessed
              << "\nmax queue depth: " << result.max_depth << "\nelapsed (including verification): "
              << std::fixed << std::setprecision(3) << elapsed << " s\nthroughput: "
              << (elapsed > 0 ? result.processed / elapsed : 0) << " requests/s\nverification: "
              << (result.verified ? "passed" : "failed") << '\n';
    if (!result.error.empty()) std::cerr << "error: " << result.error << '\n';
    return result.verified ? 0 : 1;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}
