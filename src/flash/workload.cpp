#include "flash/workload.h"
#include <limits>
#include <random>
#include <stdexcept>

namespace miniwaldb::flash {
std::vector<LogicalPage> generate_accesses(const WorkloadConfig& config) {
  if (config.working_set == 0 || config.working_set > std::numeric_limits<std::uint32_t>::max()) {
    throw std::invalid_argument("working set must be between 1 and UINT32_MAX");
  }
  if (config.pattern != Pattern::Sequential && config.pattern != Pattern::Random) {
    throw std::invalid_argument("unknown flash workload pattern");
  }
  std::mt19937 generator(config.seed);
  std::uniform_int_distribution<std::uint32_t> choose(0, static_cast<std::uint32_t>(config.working_set - 1));
  std::vector<LogicalPage> accesses;
  accesses.reserve(config.target_writes);
  for (std::size_t index = 0; index < config.target_writes; ++index) {
    if (index < config.working_set || config.pattern == Pattern::Sequential) {
      accesses.push_back(index % config.working_set);
    } else {
      accesses.push_back(choose(generator));
    }
  }
  return accesses;
}
std::string workload_value(LogicalPage logical, std::size_t request_index) {
  return "lpn=" + std::to_string(logical) + " version=" + std::to_string(request_index);
}
std::optional<double> page_write_amplification(const Statistics& statistics) {
  if (statistics.successful_logical_writes == 0) return std::nullopt;
  return static_cast<double>(statistics.physical_programs) / statistics.successful_logical_writes;
}
WorkloadResult run_flash_workload(const WorkloadConfig& config) {
  const auto accesses = generate_accesses(config);
  NandSimulator simulator(config.blocks, config.pages_per_block);
  WorkloadResult result;
  result.config = config;
  std::map<LogicalPage, std::string> expected;
  for (std::size_t index = 0; index < accesses.size(); ++index) {
    const auto logical = accesses[index];
    const auto value = workload_value(logical, index);
    try {
      simulator.write(logical, value);
    } catch (const std::exception& e) {
      result.error = e.what();
      break; // No retries: do not hide out-of-space or compare unequal successes silently.
    }
    expected[logical] = value;
  }
  result.statistics = simulator.statistics();
  result.completed = result.error.empty() && result.statistics.successful_logical_writes == config.target_writes;
  result.write_amplification = page_write_amplification(result.statistics);
  result.verified = simulator.valid() && result.statistics.valid_pages == expected.size();
  for (const auto& entry : expected) {
    const auto actual = simulator.read(entry.first);
    if (!actual || *actual != entry.second) result.verified = false;
    if (actual) result.final_values.emplace(entry.first, *actual);
  }
  if (!result.verified && result.error.empty()) result.error = "final logical state verification failed";
  return result;
}
}
