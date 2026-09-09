#include "repl.h"
#include <iostream>

int main() {
  return miniwaldb::run_shell("./dbdata", std::cin, std::cout, std::cerr);
}
