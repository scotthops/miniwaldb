// Intentional logic bug: the assertion stops execution before an invalid read.
#include <cassert>

int read_page(const int* pages, int count, int index) {
  assert(index >= 0 && index < count);
  return pages[index];
}

int pick_last_page() {
  int pages[4] = {10, 20, 30, 40};
  int count = 4;
  int index = count; // BUG: last index should be count - 1.
  return read_page(pages, count, index);
}

int main() {
  return pick_last_page();
}
