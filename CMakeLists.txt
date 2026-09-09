cmake_minimum_required(VERSION 3.16)
project(miniwaldb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MINIWALDB_BUILD_TESTS "Build tests" ON)

# Library target (your database code)
add_library(miniwaldb
    src/wal/wal_writer.cpp
    src/wal/wal_reader.cpp
    src/storage/file_io.cpp
    src/db/db.cpp
    src/table/items_table.cpp
)

target_include_directories(miniwaldb PUBLIC include)
target_compile_features(miniwaldb PUBLIC cxx_std_20)

# Reasonable warnings
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(miniwaldb PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Tools: a tiny REPL/shell
add_library(miniwaldb_repl tools/repl.cpp)
target_include_directories(miniwaldb_repl PUBLIC tools)
target_link_libraries(miniwaldb_repl PUBLIC miniwaldb)

add_executable(miniwaldb_shell tools/shell.cpp)
target_link_libraries(miniwaldb_shell PRIVATE miniwaldb_repl)

# Standalone producer-consumer harness; Db remains single-threaded.
find_package(Threads REQUIRED)
add_library(miniwaldb_workload_support
    src/workload/request_queue.cpp
    src/workload/workload.cpp
)
target_link_libraries(miniwaldb_workload_support PUBLIC miniwaldb Threads::Threads)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(miniwaldb_workload_support PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_executable(miniwaldb_workload tools/workload.cpp)
target_link_libraries(miniwaldb_workload PRIVATE miniwaldb_workload_support)

# In-memory educational flash model; independent of Db and its persistence.
add_library(miniwaldb_flash src/flash/nand.cpp)
target_include_directories(miniwaldb_flash PUBLIC include)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(miniwaldb_flash PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_executable(miniwaldb_flash_demo tools/flash_demo.cpp)
target_link_libraries(miniwaldb_flash_demo PRIVATE miniwaldb_flash)

# Tests (Catch2 via FetchContent)
if (MINIWALDB_BUILD_TESTS)
  include(CTest)
  enable_testing()

  include(FetchContent)
  FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG v3.5.4
  )
  FetchContent_MakeAvailable(Catch2)

  add_executable(miniwaldb_tests
      tests/test_nand.cpp
      tests/test_workload.cpp
      tests/test_repl.cpp
      tests/test_items_table.cpp
      tests/test_recovery_tail.cpp
      tests/test_persistence_boundary.cpp
      tests/test_wal_roundtrip.cpp
      tests/test_recovery_smoke.cpp
      tests/test_snapshot_roundtrip.cpp
      tests/test_wal_format_golden.cpp
  )
  target_link_libraries(miniwaldb_tests PRIVATE miniwaldb_repl miniwaldb_workload_support miniwaldb_flash Catch2::Catch2WithMain)

  add_test(NAME miniwaldb_tests COMMAND miniwaldb_tests)
  set_tests_properties(miniwaldb_tests PROPERTIES TIMEOUT 60)
endif()
