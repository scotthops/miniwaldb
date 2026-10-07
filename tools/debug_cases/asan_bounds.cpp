// Intentional bug: opt-in teaching target, never a CTest test.
#include <memory>
#include <iostream>

int main() {
  const int count = 4;
  auto pages = std::make_unique<int[]>(count);
  volatile int index = count; // Valid indexes are 0..count-1.
  pages[index] = 42; // BUG: one int beyond the heap allocation.
  std::cout << pages[index] << '\n';
}
