#include "flash/nand.h"
#include <iostream>
#include <stdexcept>

int main() {
  try {
    miniwaldb::flash::NandSimulator flash(3, 4);
    flash.write(5, "A");
    flash.write(6, "keep");
    flash.write(5, "B");
    flash.write(5, "C");
    for (std::uint64_t key = 7; key <= 10; ++key) flash.write(key, "value-" + std::to_string(key));
    std::cout << "Before GC: page 5 has two stale versions; four free pages remain.\n";
    flash.dump(std::cout);
    flash.write(11, "trigger GC");
    std::cout << "\nAfter automatic GC and write 11: live pages moved out of block 0 before erase.\n";
    flash.dump(std::cout);
    if (!flash.valid() || flash.read(5) != "C" || flash.read(6) != "keep" || flash.read(11) != "trigger GC") {
      throw std::runtime_error("demo verification failed");
    }
    for (std::uint64_t key = 7; key <= 10; ++key) {
      if (flash.read(key) != "value-" + std::to_string(key)) throw std::runtime_error("demo read mismatch");
    }
    std::cout << "verification: passed\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}
