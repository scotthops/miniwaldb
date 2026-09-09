#pragma once
#include "flash/nand.h"

namespace miniwaldb::flash {
enum class Pattern { Sequential, Random };
struct WorkloadConfig {
  Pattern pattern{Pattern::Sequential};
  std::size_t blocks{8};
  std::size_t pages_per_block{8};
  std::size_t working_set{32};
  std::size_t target_writes{1000}; // Includes common initial population.
  std::uint32_t seed{12345};
};
struct WorkloadResult {
  WorkloadConfig config;
  Statistics statistics;
  std::optional<double> write_amplification; // Undefined when successful writes = 0.
  std::map<LogicalPage, std::string> final_values;
  bool completed{false};
  bool verified{false};
  std::string error;
};
std::vector<LogicalPage> generate_accesses(const WorkloadConfig& config);
std::string workload_value(LogicalPage logical, std::size_t request_index);
std::optional<double> page_write_amplification(const Statistics& statistics);
WorkloadResult run_flash_workload(const WorkloadConfig& config);
}
