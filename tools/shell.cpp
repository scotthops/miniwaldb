#include "db/db.h"
#include <iostream>
#include <sstream>

int main() {
  miniwaldb::Db db("./dbdata");

  std::cout << "miniwaldb shell (commands: begin, commit, abort, put k v, get k, quit)\n";
  std::string line;
  while (true) {
    std::cout << "> ";
    if (!std::getline(std::cin, line)) break;
    std::istringstream iss(line);
    std::string cmd;
    iss >> cmd;
    if (cmd == "quit" || cmd == "exit") break;

    try {
      if (cmd == "begin") db.begin();
      else if (cmd == "commit") db.commit();
      else if (cmd == "abort") db.abort();
      else if (cmd == "put") {
        long long k; std::string v;
        iss >> k;
        std::getline(iss, v);
        if (!v.empty() && v[0] == ' ') v.erase(0,1);
        db.put(k, v);
      } else if (cmd == "get") {
        long long k; iss >> k;
        auto val = db.get(k);
        std::cout << (val ? *val : "<null>") << "\n";
      } else {
        std::cout << "unknown command\n";
      }
    } catch (const std::exception& e) {
      std::cout << "error: " << e.what() << "\n";
    }
  }
  return 0;
}
