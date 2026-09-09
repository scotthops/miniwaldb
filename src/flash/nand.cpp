#include "flash/nand.h"
#include <iomanip>
#include <limits>
#include <stdexcept>
#include <utility>

namespace miniwaldb::flash {
NandFlash::NandFlash(std::size_t blocks, std::size_t pages_per_block)
    : pages_per_block_(pages_per_block) {
  if (blocks == 0 || pages_per_block == 0 || blocks > std::numeric_limits<std::size_t>::max() / pages_per_block) {
    throw std::invalid_argument("flash geometry must be positive and fit in size_t");
  }
  blocks_.resize(blocks);
  for (auto& block : blocks_) block.pages.resize(pages_per_block);
}
const Page& NandFlash::page(Location location) const {
  return blocks_.at(location.block).pages.at(location.page);
}
void NandFlash::program(Location location, LogicalPage logical, const std::string& value) {
  auto& target = blocks_.at(location.block).pages.at(location.page);
  if (target.state != PageState::Free) throw std::runtime_error("only Free pages may be programmed");
  Page programmed{PageState::Valid, logical, value}; // Copy before changing physical state.
  target = std::move(programmed);
  ++programs_;
}
void NandFlash::invalidate(Location location) {
  auto& target = blocks_.at(location.block).pages.at(location.page);
  if (target.state != PageState::Valid) throw std::runtime_error("only Valid pages may be invalidated");
  target.state = PageState::Invalid; // Retain stale data until whole-block erase.
}
void NandFlash::erase_block(std::size_t block) {
  for (auto& page : blocks_.at(block).pages) page = Page{};
  ++erases_;
}
Statistics NandFlash::statistics() const {
  Statistics result;
  result.physical_programs = programs_;
  result.block_erases = erases_;
  for (const auto& block : blocks_) {
    bool used = false;
    for (const auto& page : block.pages) {
      if (page.state == PageState::Free) ++result.free_pages;
      else {
        used = true;
        if (page.state == PageState::Valid) ++result.valid_pages;
        else ++result.invalid_pages;
      }
    }
    if (used) ++result.blocks_used;
  }
  return result;
}
NandSimulator::NandSimulator(std::size_t blocks, std::size_t pages_per_block)
    : flash_(blocks, pages_per_block) {
  if (blocks < 2) throw std::invalid_argument("simulator requires at least two blocks for relocation");
}
std::optional<Location> NandSimulator::free_page(std::optional<std::size_t> excluded_block) const {
  for (std::size_t b = 0; b < flash_.blocks().size(); ++b) {
    if (excluded_block && b == *excluded_block) continue;
    for (std::size_t p = 0; p < flash_.pages_per_block(); ++p) {
      if (flash_.page({b, p}).state == PageState::Free) return Location{b, p};
    }
  }
  return std::nullopt;
}
std::optional<Location> NandSimulator::location(LogicalPage logical) const {
  const auto found = mapping_.find(logical);
  if (found == mapping_.end()) return std::nullopt;
  return found->second;
}
std::optional<std::string> NandSimulator::read(LogicalPage logical) const {
  const auto address = location(logical);
  if (!address) return std::nullopt;
  const auto& page = flash_.page(*address);
  if (page.state != PageState::Valid || page.logical != logical) throw std::logic_error("invalid L2P mapping");
  return page.value;
}
void NandSimulator::write(LogicalPage logical, const std::string& value) {
  ++logical_writes_;
  const auto before = statistics();
  if (before.free_pages == 0 ||
      (before.free_pages <= flash_.pages_per_block() && before.invalid_pages > 0)) {
    collect(); // Early collection preserves relocation headroom; it can still fail safely.
  }
  const auto destination = free_page();
  if (!destination) throw std::runtime_error("out of space: no free physical page");
  // Collection may have moved the old version: find its location after allocation/GC.
  const auto old = location(logical);
  flash_.program(*destination, logical, value);
  try {
    mapping_.insert_or_assign(logical, *destination);
  } catch (...) {
    flash_.invalidate(*destination); // Failed map allocation must not leave an orphan Valid page.
    throw;
  }
  if (old) flash_.invalidate(*old);
}
std::size_t NandSimulator::collect() {
  const auto free_total = flash_.statistics().free_pages;
  std::optional<std::size_t> victim;
  std::size_t best_invalid = 0;
  for (std::size_t b = 0; b < flash_.blocks().size(); ++b) {
    std::size_t live = 0, invalid = 0, free_inside = 0;
    for (const auto& page : flash_.blocks()[b].pages) {
      if (page.state == PageState::Valid) ++live;
      else if (page.state == PageState::Invalid) ++invalid;
      else ++free_inside;
    }
    if (invalid > best_invalid && live <= free_total - free_inside) {
      victim = b;
      best_invalid = invalid;
    }
  }
  if (!victim) throw std::runtime_error("out of space: no reclaimable block with safe relocation space");
  // Feasibility was checked before changing anything. Destinations exclude the victim.
  for (std::size_t p = 0; p < flash_.pages_per_block(); ++p) {
    const Location source{*victim, p};
    const auto& page = flash_.page(source);
    if (page.state != PageState::Valid) continue;
    const auto destination = free_page(*victim);
    if (!destination) throw std::logic_error("GC relocation space invariant failed");
    flash_.program(*destination, *page.logical, page.value);
    mapping_.at(*page.logical) = *destination;
    flash_.invalidate(source);
  }
  flash_.erase_block(*victim);
  ++gc_runs_;
  return *victim;
}
Statistics NandSimulator::statistics() const {
  auto result = flash_.statistics();
  result.logical_writes = logical_writes_;
  result.gc_runs = gc_runs_;
  return result;
}
bool NandSimulator::valid() const {
  std::size_t live = 0;
  for (std::size_t b = 0; b < flash_.blocks().size(); ++b) {
    for (std::size_t p = 0; p < flash_.pages_per_block(); ++p) {
      const auto& page = flash_.page({b, p});
      if (page.state == PageState::Free && (page.logical || !page.value.empty())) return false;
      if (page.state != PageState::Free && !page.logical) return false;
      if (page.state == PageState::Valid) {
        ++live;
        const auto address = location(*page.logical);
        if (!address || !(*address == Location{b, p})) return false;
      }
    }
  }
  return live == mapping_.size();
}
void NandSimulator::dump(std::ostream& output) const {
  for (std::size_t b = 0; b < flash_.blocks().size(); ++b) {
    output << "Block " << b << ":\n";
    for (std::size_t p = 0; p < flash_.pages_per_block(); ++p) {
      const auto& page = flash_.page({b, p});
      output << "  P" << p << ' ';
      if (page.state == PageState::Free) output << "FREE\n";
      else output << (page.state == PageState::Valid ? "VALID" : "INVALID")
                  << " LPN=" << *page.logical << ' ' << std::quoted(page.value) << '\n';
    }
  }
  output << "Mapping:\n";
  for (const auto& entry : mapping_) {
    output << "  " << entry.first << " -> B" << entry.second.block << ":P" << entry.second.page << '\n';
  }
  const auto s = statistics();
  output << "logical writes requested: " << s.logical_writes << "\nphysical programs: " << s.physical_programs
         << "\nblock erases: " << s.block_erases << "\nGC runs: " << s.gc_runs
         << "\nvalid/invalid/free: " << s.valid_pages << '/' << s.invalid_pages << '/' << s.free_pages
         << "\nblocks used: " << s.blocks_used << '\n';
}
}
