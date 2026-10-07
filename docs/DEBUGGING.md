# Debugging and host-side CI

Commands below run from the repository root on Linux/WSL with GCC or Clang, CMake,
and GDB installed. Use Debug builds for source lines, local variables and assertions.
Catch2 v3.5.4 is fetched during the first test configuration; that needs network access.

## Normal build/test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DMINIWALDB_BUILD_TESTS=ON -DMINIWALDB_ENABLE_SANITIZERS=OFF -DMINIWALDB_BUILD_DEBUG_CASES=OFF
cmake --build build --parallel 2
(cd build && ctest --output-on-failure)
```

Run CTest inside the build directory. This works with the project's CMake 3.16
minimum; running it from the repository root finds no tests and is not validation.

## Sanitizer build/test

```sh
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DMINIWALDB_BUILD_TESTS=ON -DMINIWALDB_ENABLE_SANITIZERS=ON -DMINIWALDB_BUILD_DEBUG_CASES=OFF
cmake --build build-asan --parallel 2
(cd build-asan && ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 ctest --output-on-failure)
```

If Catch2 already exists locally and network access is unavailable, append
`-DFETCHCONTENT_SOURCE_DIR_CATCH2="$PWD/build/_deps/catch2-src"` to the sanitizer
configure command. The source is reused; the instrumented build products remain
separate. This override was used during local verification, not in CI.

`MINIWALDB_ENABLE_SANITIZERS` centrally adds `-fsanitize=address,undefined` at
compile and link time, plus `-fno-omit-frame-pointer` and
`-fno-sanitize-recover=all` at compile time. Project libraries, executables and
Catch2 receive instrumentation. Unsupported compiler families produce a CMake error.
UBSan findings stop execution instead of merely printing a warning and passing CI.

LeakSanitizer's exit-time inspection can fail under ptrace or a restricted sandbox.
During verification here, every assertion passed inside the sandbox but leak checking
ended with `LeakSanitizer has encountered a fatal error`. Running the same command
outside the sandbox passed with leak detection enabled. Do not call the failing exit
a passing test or disable CI leak checking to hide it. Run sanitizer tests outside GDB.

## Deliberate ASan bounds case

Build these separate manual targets only when requested:

```sh
cmake -S . -B build-debug-asan -DCMAKE_BUILD_TYPE=Debug -DMINIWALDB_BUILD_TESTS=OFF -DMINIWALDB_ENABLE_SANITIZERS=ON -DMINIWALDB_BUILD_DEBUG_CASES=ON
cmake --build build-debug-asan --target miniwaldb_debug_asan miniwaldb_debug_ubsan --parallel 2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 ./build-debug-asan/miniwaldb_debug_asan
```

Expected: nonzero exit with an ASan report. Observed excerpt:

```text
ERROR: AddressSanitizer: heap-buffer-overflow
WRITE of size 4
... tools/debug_cases/asan_bounds.cpp:9
0 bytes to the right of 16-byte region
```

The program allocates four ints and writes `pages[4]`; valid indexes are 0–3.
ASan reports the write and allocation site. The fix would be to validate the index
and use `count - 1` when writing the last element. `unique_ptr` handles ownership,
but does not bounds-check subscripting. The example deliberately retains the bug.

## Deliberate UBSan case

Use the same configure/build commands above, then:

```sh
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 ./build-debug-asan/miniwaldb_debug_ubsan
```

Expected: nonzero exit. Observed diagnostic at `ubsan_overflow.cpp:7`:

```text
runtime error: signed integer overflow: 2147483647 + 1 cannot be represented in type 'int'
```

The counter already equals `INT_MAX`; adding one is undefined signed overflow.
Check the limit before adding, or choose a sufficiently wide type and define what
happens at its limit. Signed overflow is not a portable wraparound operation.
Both sanitizer examples use volatile inputs to keep the operation visible; volatile
is not the fix and has no thread-synchronization meaning here.

## GDB walkthrough

```sh
cmake -S . -B build-debug-cases -DCMAKE_BUILD_TYPE=Debug -DMINIWALDB_BUILD_TESTS=OFF -DMINIWALDB_ENABLE_SANITIZERS=OFF -DMINIWALDB_BUILD_DEBUG_CASES=ON
cmake --build build-debug-cases --target miniwaldb_debug_gdb --parallel 2
gdb -q ./build-debug-cases/miniwaldb_debug_gdb
```

At the GDB prompt, use this sequence against `tools/debug_cases/gdb_index.cpp`:

```text
set pagination off
break pick_last_page
run
until 12
next
print count
print index
print pages[3]
step
next
print index
bt
continue
bt
quit
```

- `break`/`run` stop in `pick_last_page`. `until 12` reaches the incorrect assignment;
  `next` executes it. Inspecting variables now shows `count = 4`, `index = 4`, and
  `pages[3] = 40`. Do not inspect `index` before its initialization.
- `step` enters `read_page`. The following `next` advances past the function entry
  on the verified GCC/GDB build. Inspect parameters at the assertion line; some
  debuggers display unreliable parameter values in the function prologue.
- `bt` shows `read_page -> pick_last_page -> main`, identifying where the bad index
  came from. `continue` triggers the assertion at line 5 and stops on SIGABRT.
  The second backtrace includes libc assertion/abort frames above those three calls.
- The bug originates at line 12: `index = count`, instead of `count - 1`. The assertion
  catches it before an out-of-bounds read. Keep assertions enabled; this is why the
  walkthrough uses Debug, not Release/NDEBUG.

Line stepping can vary with compiler debug information; use `list` to locate the
assignment and assertion if source lines change. GDB requires ptrace permission;
this walkthrough was verified outside the restricted sandbox. Missing libc source
files in the SIGABRT frame do not prevent inspection of the application's frames.

## Real issue found while enabling sanitizers

The initial instrumented suite reported `allocation-size-too-big` in
`storage::read_file`, reached by `File reads distinguish missing files from access
errors`. A directory opened as a stream and produced an enormous apparent size.
The original broad `REQUIRE_THROWS` accepted an allocation exception in normal builds,
masking the wrong failure path.

The retained fix rejects non-regular persistence files before seeking/allocating.
The regression now requires `not a regular persistence file: ...`. Reproduce the
fixed regression with:

```sh
(cd build-asan && ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 ./miniwaldb_tests 'File reads distinguish missing files from access errors')
```

The production bug is fixed, not retained as an intentional failure. The two tiny
manual examples provide reproducible failing programs without damaging real paths.

## CI and verification record

`.github/workflows/host-tests.yml` runs on pushes and pull requests on Ubuntu 24.04.
Its OFF/ON sanitizer matrix checks out with `actions/checkout@v4`, configures a Debug
build with tests enabled and debug cases disabled, builds with two jobs, then runs
CTest inside `build`. Sanitizer failures and test/build failures fail the job. Leak
checking and fatal UBSan diagnostics are enabled. The workflow has read-only repository
permissions, no deployment, caching, services, or deliberate failing test executions.

The debug targets are behind an OFF-by-default option, marked EXCLUDE_FROM_ALL, and
never registered with CTest. Even opting in does not build them without explicit
`--target` requests. They are not linked into production executables.

Local verification used GCC 9.4, GDB 9.2 and CMake 3.16.3: normal and combined ASan/UBSan
builds passed all 88 Catch2 cases / 1,286 assertions. ASan and UBSan examples exited
nonzero with the diagnostics above; GDB reproduced the assertion and variable values.
The YAML was parsed and its triggers, matrix and commands checked against local
configuration. This does not establish a hosted GitHub Actions run: commit/push the
workflow and inspect its first run before claiming a demonstrated CI pass.

## What the tools do not prove

GDB observes control flow, variables and call stacks; it does not automatically
identify every bug. ASan instruments memory accesses for bugs such as heap overflows
and use-after-free. UBSan checks selected undefined behavior, including signed overflow.
They check executed paths, not every possible input. Passing runs do not prove memory
safety, race freedom, transaction correctness or power-loss durability. ASan/UBSan
are not ThreadSanitizer, and these exercises do not emulate real hardware failures.
