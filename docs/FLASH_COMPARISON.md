# Sequential and random flash-write comparison

Run `./build/miniwaldb_flash_compare`. There is no timing benchmark or database
integration. `run_flash_workload(WorkloadConfig)` and `generate_accesses` in
`src/flash/workload.cpp` can also be used directly in tests or small experiments.
The existing allocator and GC policy are unchanged.

## Controlled workload

Both runs start with fresh 8-block × 8-page flash and use a 32-page logical working
set, targeting 1,000 successful writes. The first 32 requests populate pages 0–31
in order in both runs. These setup writes count toward all reported totals.

Sequential then cycles through 0–31 until it reaches the target. Random instead
chooses each subsequent page uniformly from 0–31, with replacement, using
`std::mt19937` seeded with 12345 and `uniform_int_distribution<uint32_t>`.
The seed, geometry, working set and target are printed. Generation is reproducible
with the same standard-library implementation; C++ distributions need not map engine
outputs identically across different standard libraries. Tests compare repeat runs
and request sequences in the current toolchain, not portable golden random output.

Values use `lpn=KEY version=REQUEST_INDEX` in both runs. Strings can have different
character lengths, but each always occupies exactly one abstract simulated page;
no byte-size or timing model exists. Only the post-population access pattern differs.
There are no retries. On failure, the runner stops, reports the error and successful
prefix, and still verifies the values from successful writes. An incomplete run is
not presented as an equal-successful-count comparison. Invalid configurations throw.
Zero-target workloads complete with no writes and undefined WA.

## Metrics

- **Successful logical write:** one host `write` that finished programming, mapping
  replacement and old-page invalidation. Counted separately from attempts; rejected
  writes increase attempts only.
- **Physical program:** one physical page program, caused by a host write or a live
  GC copy. It is counted at the physical operation, not inferred from logical writes.
- **GC-copy programs:** counted at each successful relocation program. Direct host
  programs are `physical_programs - gc_copy_programs`.
- **Page-program write amplification:** `physical_programs / successful_logical_writes`.
  If the denominator is zero, report n/a. Rejected attempts never enter the denominator.
- **Erase activity:** block erases. In translator runs, these occur during reclamation;
  completed GC runs are also reported. Each completed collection erases one block.
- **Block usage:** blocks containing at least one non-Free page at the end. This is
  a snapshot of occupancy, not cumulative blocks touched, wear or erase distribution.
- **Page usage:** current Valid, Invalid and Free counts; their sum equals capacity.

Physical programs count both direct writes and preservation of still-live pages
before victim erasure. This is why the numerator can exceed successful logical writes.
Reads used for final verification do not change these counters. A partial run's WA
is a diagnostic ratio for that run, not evidence of a fair completed comparison.

## Actual default results

Measured with the repository's current Linux/WSL build and seed 12345:

| Metric | Sequential | Random |
|---|---:|---:|
| Attempted / successful writes | 1000 / 1000 | 1000 / 1000 |
| Physical programs | 1000 | 1451 |
| Direct host programs | 1000 | 1000 |
| GC-copy programs | 0 | 451 |
| Block erases / completed GC runs | 118 / 118 | 176 / 176 |
| Page-program WA | 1.000 | 1.451 |
| Blocks used | 7 / 8 | 8 / 8 |
| Valid / Invalid / Free | 32 / 24 / 8 | 32 / 21 / 11 |
| Latest-value verification | passed | passed |

Worked arithmetic: random WA = 1451 / 1000 = 1.451. Its 451 relocation programs
are additional physical work beyond the 1,000 completed host writes.

For this configuration, sequential cycling leaves reclaimable victims without live
pages (zero relocation copies). Random updates spread invalidations across logical
pages and leave some live pages in chosen victims, requiring copies. More physical
programming also consumes free pages faster and leads to more erasures here.

Random ends with more used blocks *and* more free pages: block usage counts whether
any page in a block is programmed, not how full each block is. These metrics describe
different aspects of placement.

These measurements are not universal workload rankings. First-free allocation, basic
feasible-victim selection, early GC and geometry determine the result. No test asserts
that random always has greater WA. Real SSDs add firmware placement, wear leveling,
overprovisioning, channels, byte sizes and other behavior absent from this model.
No latency, bandwidth or endurance conclusion follows directly from these counts.

## Regression evidence

`tests/test_flash_workload.cpp` checks generated order and seed reproducibility,
equal target completion, exact WA calculation, zero denominator, GC-copy accounting,
physical page occupancy, erase counts, unchanged counters after reads, latest values,
and safe incomplete-run reporting. A deliberately full 2×3 configuration produces
7 attempts, 6 successful writes and 6 physical programs: WA remains 1.0, rather than
the incorrect 6/7 obtained by dividing by attempts.
