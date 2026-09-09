#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include "flash/nand.h"
#include <sstream>

using namespace miniwaldb::flash;
namespace {
void check_totals(const NandSimulator& sim, std::size_t total) {
  const auto s = sim.statistics();
  REQUIRE(s.free_pages + s.valid_pages + s.invalid_pages == total);
  REQUIRE(sim.valid());
}
}

TEST_CASE("Physical NAND programs only Free pages and erases whole blocks", "[flash]") {
  NandFlash flash(2, 3);
  REQUIRE(flash.statistics().free_pages == 6);
  REQUIRE(flash.page({0, 0}).state == PageState::Free);
  flash.program({0, 0}, 5, "A");
  flash.program({0, 1}, 6, "B");
  REQUIRE(flash.page({0, 0}).state == PageState::Valid);
  REQUIRE(flash.page({0, 0}).logical == 5);
  REQUIRE(flash.page({0, 0}).value == "A");
  REQUIRE_THROWS(flash.program({0, 0}, 5, "overwrite"));
  flash.invalidate({0, 0});
  REQUIRE(flash.page({0, 0}).state == PageState::Invalid);
  REQUIRE(flash.page({0, 0}).value == "A");
  REQUIRE_THROWS(flash.program({0, 0}, 5, "reuse"));
  REQUIRE(flash.statistics().physical_programs == 2);
  flash.erase_block(0);
  for (std::size_t p = 0; p < 3; ++p) {
    REQUIRE(flash.page({0, p}).state == PageState::Free);
    REQUIRE_FALSE(flash.page({0, p}).logical.has_value());
    REQUIRE(flash.page({0, p}).value.empty());
  }
  REQUIRE(flash.statistics().block_erases == 1);
  REQUIRE(flash.statistics().free_pages == 6);
  flash.program({0, 0}, 9, "reused after erase");
  REQUIRE(flash.statistics().physical_programs == 3);
}

TEST_CASE("Logical writes move mappings and preserve stale physical data", "[flash]") {
  NandSimulator sim(3, 4);
  REQUIRE_FALSE(sim.read(5).has_value());
  REQUIRE_FALSE(sim.location(5).has_value());
  sim.write(5, "A");
  const auto a = sim.location(5).value();
  REQUIRE(a == Location{0, 0});
  sim.write(5, "B");
  const auto b = sim.location(5).value();
  sim.write(5, "C");
  const auto c = sim.location(5).value();
  REQUIRE_FALSE(a == b);
  REQUIRE_FALSE(b == c);
  REQUIRE(sim.flash().page(a).state == PageState::Invalid);
  REQUIRE(sim.flash().page(b).state == PageState::Invalid);
  REQUIRE(sim.flash().page(a).value == "A");
  REQUIRE(sim.read(5) == "C");
  REQUIRE(sim.statistics().logical_writes == 3);
  REQUIRE(sim.statistics().physical_programs == 3);
  REQUIRE(sim.statistics().invalid_pages == 2);
  REQUIRE(sim.statistics().blocks_used == 1);
  const auto before = sim.statistics();
  REQUIRE(sim.read(5) == "C");
  REQUIRE_FALSE(sim.read(999).has_value());
  REQUIRE(sim.statistics().physical_programs == before.physical_programs);
  REQUIRE(sim.statistics().block_erases == before.block_erases);
  REQUIRE(sim.statistics().logical_writes == before.logical_writes);
  check_totals(sim, 12);
}

TEST_CASE("GC relocates live pages outside its victim before whole-block erase", "[flash]") {
  NandSimulator sim(3, 4);
  sim.write(5, "A");
  sim.write(6, "keep");
  sim.write(5, "B");
  sim.write(5, "C");
  const auto old5 = sim.location(5).value();
  const auto old6 = sim.location(6).value();
  REQUIRE(sim.collect() == 0);
  REQUIRE(sim.location(5)->block != 0);
  REQUIRE(sim.location(6)->block != 0);
  REQUIRE_FALSE(sim.location(5).value() == old5);
  REQUIRE_FALSE(sim.location(6).value() == old6);
  REQUIRE(sim.read(5) == "C");
  REQUIRE(sim.read(6) == "keep");
  for (const auto& page : sim.flash().blocks()[0].pages) REQUIRE(page.state == PageState::Free);
  const auto s = sim.statistics();
  REQUIRE(s.logical_writes == 4);
  REQUIRE(s.physical_programs == 6); // Four host programs plus two live copies.
  REQUIRE(s.block_erases == 1);
  REQUIRE(s.gc_runs == 1);
  REQUIRE(s.valid_pages == 2);
  REQUIRE(s.invalid_pages == 0);
  REQUIRE(s.free_pages == 10);
  check_totals(sim, 12);
}

TEST_CASE("Automatic GC permits repeated updates and preserves every latest value", "[flash]") {
  NandSimulator sim(3, 4);
  for (LogicalPage key = 0; key < 4; ++key) sim.write(key, "initial");
  for (int version = 0; version < 20; ++version) {
    for (LogicalPage key = 0; key < 4; ++key) sim.write(key, std::to_string(version));
    for (LogicalPage key = 0; key < 4; ++key) REQUIRE(sim.read(key) == std::to_string(version));
    check_totals(sim, 12);
  }
  REQUIRE(sim.statistics().logical_writes == 84);
  REQUIRE(sim.statistics().gc_runs > 0);
  REQUIRE(sim.statistics().block_erases == sim.statistics().gc_runs);
}

TEST_CASE("Automatic GC counts copies in the deterministic demo geometry", "[flash]") {
  NandSimulator sim(3, 4);
  sim.write(5, "A"); sim.write(6, "keep"); sim.write(5, "B"); sim.write(5, "C");
  for (LogicalPage key = 7; key <= 10; ++key) sim.write(key, "value");
  REQUIRE(sim.statistics().free_pages == 4);
  sim.write(11, "new");
  REQUIRE(sim.statistics().logical_writes == 9);
  REQUIRE(sim.statistics().physical_programs == 11);
  REQUIRE(sim.statistics().gc_runs == 1);
  REQUIRE(sim.read(5) == "C");
  REQUIRE(sim.read(6) == "keep");
  for (LogicalPage key = 7; key <= 10; ++key) REQUIRE(sim.read(key) == "value");
  REQUIRE(sim.read(11) == "new");
  check_totals(sim, 12);
}

TEST_CASE("Unsafe GC and full-flash writes fail without losing logical data", "[flash]") {
  NandSimulator sim(2, 3);
  for (LogicalPage key = 0; key < 5; ++key) sim.write(key, "original");
  sim.write(0, "updated"); // Last free page; victim has two live pages but no relocation space.
  const auto before = sim.statistics();
  std::ostringstream dump_before;
  sim.dump(dump_before);
  REQUIRE_THROWS_WITH(sim.collect(), "out of space: no reclaimable block with safe relocation space");
  std::ostringstream dump_after;
  sim.dump(dump_after);
  REQUIRE(dump_before.str() == dump_after.str());
  REQUIRE_THROWS(sim.write(1, "must not appear"));
  REQUIRE(sim.read(0) == "updated");
  for (LogicalPage key = 1; key < 5; ++key) REQUIRE(sim.read(key) == "original");
  REQUIRE(sim.statistics().logical_writes == before.logical_writes + 1);
  REQUIRE(sim.statistics().physical_programs == before.physical_programs);
  REQUIRE(sim.statistics().gc_runs == 0);
  REQUIRE(sim.statistics().block_erases == 0);
  check_totals(sim, 6);
}

TEST_CASE("Geometry and physical indexes are checked", "[flash]") {
  REQUIRE_THROWS(NandFlash(0, 4));
  REQUIRE_THROWS(NandFlash(4, 0));
  REQUIRE_THROWS(NandSimulator(1, 4));
  NandFlash flash(2, 2);
  REQUIRE_THROWS(flash.page({2, 0}));
  REQUIRE_THROWS(flash.program({0, 2}, 0, "bad"));
  REQUIRE_THROWS(flash.invalidate({0, 0}));
  REQUIRE_THROWS(flash.erase_block(2));
  NandSimulator defaults;
  REQUIRE(defaults.statistics().free_pages == 64);
  NandSimulator tiny(2, 1);
  tiny.write(0, "");
  tiny.write(0, "new");
  tiny.write(0, "newest");
  REQUIRE(tiny.read(0) == "newest");
  check_totals(tiny, 2);
}
