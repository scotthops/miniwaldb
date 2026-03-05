#!/usr/bin/env bash
set -euo pipefail
cmake -S . -B build -G Ninja -DMINIWALDB_BUILD_TESTS=ON
cmake --build build
cd build
ctest -V
