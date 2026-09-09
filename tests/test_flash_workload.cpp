#include <catch2/catch_test_macros.hpp>
#include "flash/workload.h"
#include <algorithm>

using namespace miniwaldb::flash;
namespace {
void same_statistics(const Statistics& a, const Statistics& b) {
  REQUIRE(a.logical_writes == b.logical_writes);
  REQUIRE(a.successful_logical_writes == b.successful_logical_writes);
  REQUIRE(a.physical_programs == b.physical_programs);
  REQUIRE(a.gc_copy_programs == b.gc_copy_programs);
  REQUIRE(a.block_erases == b.block_erases);
  REQUIRE(a.gc_runs == b.gc_runs);
  REQUIRE(a.valid_pages == b.valid_pages);
  REQUIRE(a.invalid_pages == b.invalid_pages);
  REQUIRE(a.free_pages == b.free_pages);
  REQUIRE(a.blocks_used == b.blocks_used);
}
}

TEST_CASE("Flash access patterns share setup and reproduce seeded request sequences", "[flash-workload]") {
  WorkloadConfig config;
  config.working_set = 4;
  config.target_writes = 12;
  const auto sequential = generate_accesses(config);
  REQUIRE(sequential == std::vector<LogicalPage>{0,1,2,3,0,1,2,3,0,1,2,3});
  REQUIRE(generate_accesses(config) == sequential);
  config.pattern = Pattern::Random;
  const auto random = generate_accesses(config);
  REQUIRE(random.size() == sequential.size());
  REQUIRE(std::equal(random.begin(), random.begin() + 4, sequential.begin()));
  REQUIRE(generate_accesses(config) == random);
  for (auto key : random) REQUIRE(key < config.working_set);
  config.seed = 54321;
  REQUIRE(generate_accesses(config) != random);
}

TEST_CASE("Sequential and seeded random flash runs reproduce complete results", "[flash-workload]") {
  WorkloadConfig config;
  SECTION("sequential") { config.pattern = Pattern::Sequential; }
  SECTION("random") { config.pattern = Pattern::Random; }
  const auto a = run_flash_workload(config);
  const auto b = run_flash_workload(config);
  REQUIRE(a.completed);
  REQUIRE(a.verified);
  REQUIRE(a.error.empty());
  REQUIRE(b.completed);
  REQUIRE(b.verified);
  same_statistics(a.statistics, b.statistics);
  REQUIRE(a.final_values == b.final_values);
  REQUIRE(a.write_amplification == b.write_amplification);
  REQUIRE(a.statistics.logical_writes == config.target_writes);
  REQUIRE(a.statistics.successful_logical_writes == config.target_writes);
  REQUIRE(a.write_amplification.value() >= 1.0);
  REQUIRE(a.write_amplification.value() == static_cast<double>(a.statistics.physical_programs) / config.target_writes);
}

TEST_CASE("Workload metrics and final values match physical simulator state", "[flash-workload]") {
  WorkloadConfig config;
  config.target_writes = 150;
  config.pattern = Pattern::Random;
  const auto result = run_flash_workload(config);
  NandSimulator simulator(config.blocks, config.pages_per_block);
  std::map<LogicalPage, std::string> expected;
  const auto accesses = generate_accesses(config);
  for (std::size_t index = 0; index < accesses.size(); ++index) {
    const auto value = workload_value(accesses[index], index);
    simulator.write(accesses[index], value);
    expected[accesses[index]] = value;
  }
  REQUIRE(result.completed);
  REQUIRE(result.final_values == expected);
  same_statistics(result.statistics, simulator.statistics());
  std::size_t used = 0, valid = 0, invalid = 0, free = 0;
  for (const auto& block : simulator.flash().blocks()) {
    bool programmed = false;
    for (const auto& page : block.pages) {
      if (page.state == PageState::Free) ++free;
      else {
        programmed = true;
        if (page.state == PageState::Valid) ++valid;
        else ++invalid;
      }
    }
    if (programmed) ++used;
  }
  REQUIRE(result.statistics.blocks_used == used);
  REQUIRE(result.statistics.valid_pages == valid);
  REQUIRE(result.statistics.invalid_pages == invalid);
  REQUIRE(result.statistics.free_pages == free);
  REQUIRE(valid + invalid + free == config.blocks * config.pages_per_block);
  const auto before_reads = simulator.statistics();
  for (const auto& entry : expected) REQUIRE(simulator.read(entry.first) == entry.second);
  same_statistics(before_reads, simulator.statistics());
  REQUIRE(page_write_amplification(before_reads) == page_write_amplification(simulator.statistics()));
}

TEST_CASE("GC copies increase page amplification beyond successful host writes", "[flash-workload]") {
  NandSimulator simulator(3, 4);
  simulator.write(5, "A"); simulator.write(6, "keep");
  simulator.write(5, "B"); simulator.write(5, "C");
  for (LogicalPage key = 7; key <= 10; ++key) simulator.write(key, "value");
  simulator.write(11, "collect");
  const auto s = simulator.statistics();
  REQUIRE(s.successful_logical_writes == 9);
  REQUIRE(s.gc_copy_programs == 2);
  REQUIRE(s.physical_programs == 11);
  REQUIRE(s.physical_programs == s.successful_logical_writes + s.gc_copy_programs);
  REQUIRE(s.block_erases == simulator.flash().statistics().block_erases);
  REQUIRE(s.block_erases == 1);
  REQUIRE(page_write_amplification(s).value() == 11.0 / 9.0);
}

TEST_CASE("Out of space reports incomplete workloads and excludes rejected writes from WA", "[flash-workload]") {
  WorkloadConfig config;
  config.blocks = 2;
  config.pages_per_block = 3;
  config.working_set = 5;
  config.target_writes = 20;
  const auto result = run_flash_workload(config);
  REQUIRE_FALSE(result.completed);
  REQUIRE(result.verified); // Earlier successful writes remain readable.
  REQUIRE(result.error.find("out of space") != std::string::npos);
  REQUIRE(result.statistics.logical_writes == 7);
  REQUIRE(result.statistics.successful_logical_writes == 6);
  REQUIRE(result.statistics.physical_programs == 6);
  REQUIRE(result.write_amplification.value() == 1.0);
  REQUIRE(result.final_values.at(0) == workload_value(0, 5));
  REQUIRE(result.final_values.at(1) == workload_value(1, 1));
}

TEST_CASE("Zero successful writes leave WA undefined and invalid configurations fail", "[flash-workload]") {
  WorkloadConfig config;
  config.target_writes = 0;
  const auto empty = run_flash_workload(config);
  REQUIRE(empty.completed);
  REQUIRE(empty.verified);
  REQUIRE_FALSE(empty.write_amplification.has_value());
  REQUIRE(empty.statistics.successful_logical_writes == 0);
  REQUIRE(empty.statistics.blocks_used == 0);
  REQUIRE(empty.final_values.empty());
  config.working_set = 0;
  REQUIRE_THROWS(run_flash_workload(config));
  config.working_set = 4;
  config.blocks = 0;
  REQUIRE_THROWS(run_flash_workload(config));
}
