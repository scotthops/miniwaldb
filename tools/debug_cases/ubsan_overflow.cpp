// Intentional bug: opt-in teaching target, never a CTest test.
#include <limits>
#include <iostream>

int main() {
  volatile int completed = std::numeric_limits<int>::max();
  int next = completed + 1; // BUG: result is not representable as signed int.
  std::cout << next << '\n';
}
