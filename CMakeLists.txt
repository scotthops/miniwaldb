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
)

target_include_directories(miniwaldb PUBLIC include)
target_compile_features(miniwaldb PUBLIC cxx_std_20)

# Reasonable warnings
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(miniwaldb PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Tools: a tiny REPL/shell
add_executable(miniwaldb_shell tools/shell.cpp)
target_link_libraries(miniwaldb_shell PRIVATE miniwaldb)

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
      tests/test_wal_roundtrip.cpp
      tests/test_recovery_smoke.cpp
  )
  target_link_libraries(miniwaldb_tests PRIVATE miniwaldb Catch2::Catch2WithMain)

  add_test(NAME miniwaldb_tests COMMAND miniwaldb_tests)
endif()
