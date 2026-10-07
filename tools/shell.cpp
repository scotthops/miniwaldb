#include "repl.h"
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
  std::string directory = "./dbdata";
  auto mode = miniwaldb::ShellMode::Human;
  for (int i = 1; i < argc; ++i) {
    const std::string argument = argv[i];
    if (argument == "--machine") {
      mode = miniwaldb::ShellMode::Machine;
    } else if (argument == "--dir" && i + 1 < argc && argv[i + 1][0] != '\0' &&
               std::string(argv[i + 1]).rfind("--", 0) != 0) {
      directory = argv[++i];
    } else {
      std::cerr << "usage: miniwaldb_shell [--machine] [--dir DIRECTORY]\n";
      return 2;
    }
  }
  return miniwaldb::run_shell(directory, std::cin, std::cout, std::cerr, mode);
}
