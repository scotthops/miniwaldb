# Educational NAND-flash simulator

This is an in-memory, single-threaded model independent of Db, WAL, snapshots and
real files. Run `./build/miniwaldb_flash_demo` for a deterministic demonstration.
No database storage path uses simulated flash.

## Components and geometry

`NandFlash` owns `vector<Block>`, each containing `vector<Page>`, and counts physical
page programs and block erases. `NandSimulator` owns the flash, an ordered L2P map,
logical write-request count, allocation and garbage collection. An ordered map makes
inspection deterministic; it is not a modeled hardware structure. Defaults are eight
blocks of eight pages. Physical geometry must be positive; the translator requires
at least two blocks so relocation can go elsewhere. One page holds one string,
including an empty string; there is no byte capacity or partial-page programming.

Free means unprogrammed since the last block erase. Valid contains a current logical
value. Invalid retains stale data and its logical ID. Both Valid and Invalid pages
reject programming. Only whole-block erase resets pages to Free and clears IDs/data.
The low-level flash can erase live pages; the translator is responsible for moving
them first. The translator exposes only const access to its physical model, so callers
cannot erase or invalidate its live mappings behind its back.

## Writes and mapping

The L2P map stores `logical page number -> {block, page}`. Read checks that the target
is Valid and has the matching logical ID; missing logical pages return nullopt.
Allocation scans blocks and pages from index zero for the first Free page.

A write increments logical requests, obtains a Free destination, programs it, updates
the mapping, then invalidates the previous physical page. The old location is looked
up after any automatic GC because collection may have relocated it. This models
out-of-place updates, not crash-consistent metadata. Logical deletion is omitted.

## Garbage collection and space limits

Before a write, if no free pages remain, or free pages are at most one block's worth
and invalid pages exist, attempt one collection. Explicit `collect()` is also available.
This early threshold preserves relocation opportunities; it is not a guaranteed
reserved block or sophisticated overprovisioning policy. Filling almost all space
with live logical data can still make further updates impossible.

A candidate must have invalid pages and enough free pages **outside** itself to hold
all its live pages. Among feasible candidates, choose the greatest invalid-page count;
lowest block index wins ties. Check feasibility before copying. Relocate each Valid
page to the first Free page outside the victim, update its mapping, and invalidate
its old copy. After all live data is preserved, erase the entire victim block.

If no feasible victim exists, throw a clear out-of-space error without changing pages
or mappings. This may reject a write even with some free pages remaining under the
early-GC policy. Failed writes still count as host requests; rejected collection does
not increment GC runs. No fallback overwrites or unsafe erases occur.

String allocation failures are not simulated flash crashes. Programming prepares its
string before changing a page. A failed new-map allocation invalidates the newly
programmed orphan; completed GC relocations remain coherent if a later copy throws.
There is no rollback guarantee for counters or physical placement on allocation errors.

## Invariants and counters

At public operation boundaries:

- Each mapped logical page points to exactly one Valid physical page.
- Each Valid page has one matching L2P entry; no two live pages represent the same LPN.
- Only Free pages can be programmed; Invalid pages become Free only through block erase.
- Logical updates use a different physical page.
- GC destinations exclude the victim; mappings follow copies before erase.
- Valid + Invalid + Free equals total physical pages.

`valid()` checks page metadata and mapping consistency for tests/debugging. Counter
statistics include logical write requests (even rejected writes), physical programs
(host writes plus live GC copies), block erases, successfully completed GC runs,
successful logical writes, GC-copy programs, current Valid/Invalid/Free counts and
blocks containing any programmed page. Reads
and dumps do not increment counters. Page-state counts are derived by scanning rather
than maintained through extra mutable counters. The separate comparison runner
reports physical programs divided by successful logical writes; see
[flash comparisons](FLASH_COMPARISON.md).

## Demo trace

The demo uses three blocks of four pages. Write 5=A to B0:P0, 6=keep to B0:P1,
5=B to B0:P2, then 5=C to B0:P3. P0 and P2 become Invalid; the mapping for 5 follows
P3. Keys 7 through 10 fill block 1. Four free pages remain in block 2.

Writing 11 triggers early GC. Block 0 has two invalid and two live pages. The live
6 and 5 values move to B2:P0 and B2:P1, respectively. Their mappings move with them;
block 0 is erased. Write 11 then uses newly free B0:P0. Read 5 still returns C.
The resulting counters are nine logical requests, eleven physical programs, one
erase and one GC run. The two extra programs are live-page relocation copies.

## Scope

This illustrates page-program/block-erase constraints, L2P translation and basic GC.
It does not simulate NAND bit transitions, ECC, bad blocks, endurance, wear leveling,
channels, dies, timing, persistence, concurrent access or a full SSD controller.
Tests cover physical rules, out-of-place updates, live relocation/counters, repeated
automatic collection, safe space failure, geometry/index errors and mapping invariants.
The separate comparison runner exercises these policies without changing them.
