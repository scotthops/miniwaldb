#include "flash/workload.h"
#include <iomanip>
#include <iostream>

namespace {
void print_result(const char* name, const miniwaldb::flash::WorkloadResult& result) {
  const auto& s = result.statistics;
  std::cout << '\n' << name << ": " << (result.completed ? "completed" : "INCOMPLETE")
            << "\n  attempted / successful / target: " << s.logical_writes << " / "
            << s.successful_logical_writes << " / " << result.config.target_writes
            << "\n  physical programs: " << s.physical_programs
            << "\n  host programs / GC copies: " << s.physical_programs - s.gc_copy_programs
            << " / " << s.gc_copy_programs << "\n  block erases / GC runs: "
            << s.block_erases << " / " << s.gc_runs << "\n  page-program write amplification: ";
  if (result.write_amplification) std::cout << std::fixed << std::setprecision(3) << *result.write_amplification;
  else std::cout << "n/a (no successful writes)";
  std::cout << "\n  blocks used: " << s.blocks_used << '/' << result.config.blocks
            << "\n  valid / invalid / free: " << s.valid_pages << " / " << s.invalid_pages << " / " << s.free_pages
            << "\n  read verification: " << (result.verified ? "passed" : "FAILED") << '\n';
  if (!result.error.empty()) std::cout << "  error: " << result.error << '\n';
}
}
int main() {
  try {
    miniwaldb::flash::WorkloadConfig config;
    std::cout << "Geometry: " << config.blocks << " blocks x " << config.pages_per_block
              << " pages\nWorking set: " << config.working_set << " logical pages\nTarget successful writes: "
              << config.target_writes << " (includes common ordered population)\nRandom seed: " << config.seed << '\n';
    const auto sequential = miniwaldb::flash::run_flash_workload(config);
    config.pattern = miniwaldb::flash::Pattern::Random;
    const auto random = miniwaldb::flash::run_flash_workload(config);
    print_result("Sequential", sequential);
    print_result("Random", random);
    if (!sequential.completed || !random.completed || !sequential.verified || !random.verified) {
      std::cout << "\nComparison incomplete: no equal-completed-workload conclusion.\n";
      return 1;
    }
    std::cout << "\nBoth completed the same successful-write target. These are simulator-specific page counts, not SSD timing.\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}
