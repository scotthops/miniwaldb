#pragma once
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

namespace miniwaldb::flash {
using LogicalPage = std::uint64_t;
enum class PageState { Free, Valid, Invalid };
struct Location {
  std::size_t block;
  std::size_t page;
  bool operator==(const Location& other) const { return block == other.block && page == other.page; }
};
struct Page {
  PageState state{PageState::Free};
  std::optional<LogicalPage> logical;
  std::string value;
};
struct Block { std::vector<Page> pages; };
struct Statistics {
  std::size_t logical_writes{}; // Requests, including rejected out-of-space writes.
  std::size_t physical_programs{};
  std::size_t block_erases{};
  std::size_t gc_runs{}; // Successfully completed collections.
  std::size_t valid_pages{};
  std::size_t invalid_pages{};
  std::size_t free_pages{};
  std::size_t blocks_used{}; // Blocks with at least one non-Free page.
};

class NandFlash {
public:
  NandFlash(std::size_t blocks = 8, std::size_t pages_per_block = 8);
  const Page& page(Location location) const;
  const std::vector<Block>& blocks() const { return blocks_; }
  std::size_t pages_per_block() const { return pages_per_block_; }
  void program(Location location, LogicalPage logical, const std::string& value);
  void invalidate(Location location);
  void erase_block(std::size_t block);
  Statistics statistics() const;
private:
  std::vector<Block> blocks_;
  std::size_t pages_per_block_;
  std::size_t programs_{0};
  std::size_t erases_{0};
};

class NandSimulator {
public:
  NandSimulator(std::size_t blocks = 8, std::size_t pages_per_block = 8);
  void write(LogicalPage logical, const std::string& value);
  std::optional<std::string> read(LogicalPage logical) const;
  // Collects one feasible victim; throws before changing pages if none exists.
  std::size_t collect(); // Returns the erased block index.
  std::optional<Location> location(LogicalPage logical) const;
  const NandFlash& flash() const { return flash_; }
  Statistics statistics() const;
  bool valid() const; // Small invariant check for tests/debugging.
  void dump(std::ostream& output) const;
private:
  NandFlash flash_;
  std::map<LogicalPage, Location> mapping_; // Ordered for deterministic inspection.
  std::size_t logical_writes_{0};
  std::size_t gc_runs_{0};
  std::optional<Location> free_page(std::optional<std::size_t> excluded_block = {}) const;
};
}
