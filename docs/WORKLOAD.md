# Bounded producer-consumer workload

`miniwaldb_workload` is a standalone concurrency exercise. Db is still single-owner
and has no internal thread synchronization.

```text
producer threads -> bounded FIFO RequestQueue -> one consumer -> ItemsTable -> Db/WAL
```

`Request` contains an ID, Insert/Lookup operation, integer key, and text value.
Producer p generates keys `p * keys_per_producer + index`, with value `value-KEY`.
For each key it submits Insert (ID `2 * key`) then Lookup (ID `2 * key + 1`). Each
producer preserves that order, and the FIFO preserves it through consumption.
Producers can interleave freely because their keys and request IDs are disjoint.
Each insert is a complete begin/insert/commit transaction inside the consumer.

## Queue rules

- The same mutex protects FIFO contents, closed state, and maximum occupancy.
- Capacity is fixed and positive. Size never exceeds it.
- Push waits on `not_full` with predicate `closed || size < capacity`.
- Pop waits on `not_empty` with predicate `closed || !empty`.
- Predicate waits release the mutex while waiting and recheck after wakeup,
  including spurious wakeups. There is no polling loop.
- Successful push wakes a consumer; successful pop wakes a producer.
- Close marks the queue closed under the mutex and wakes both kinds of waiters.
  It is idempotent. Future pushes return false; already accepted entries remain.
- On normal shutdown pop returns false only for closed + empty. Closed + nonempty
  continues draining. Consumer failure is an explicit exception: it stops processing
  and reports undrained accepted work.
- Pop releases the lock before returning. Table operations, WAL writes and syncs
  therefore never run under the queue mutex.
- The optional wait observer is a test seam called under the mutex immediately
  before waiting. It must not throw, wait, or reenter the queue. Tests use it to
  signal promises, proving full/empty blocking without sleeps.

## Ownership and shutdown

The consumer constructs and destroys its Db inside its thread. Producers only
construct Requests and push them. Main accesses the database for verification only
after joining every worker, so Db needs no mutex.

Normal shutdown: start consumer, start producers, join producers, close queue,
consumer drains, consumer exits on closed + empty, join consumer, verify/report.
Joining producers before closing ensures no finite workload is rejected merely
because main finished launching threads.

Consumer failure: catch inside the consumer, record the exception, close queue,
wake producers, stop consumption. Producers whose pushes are rejected exit; main
joins them, closes again harmlessly, and joins the consumer. Remaining queue entries
are counted as unprocessed. A failed mutation is not assumed rolled back: persistence
outcomes may be uncertain under the existing Db contract. No automatic retry occurs.

Producer exceptions and thread-launch failures also close the queue and join all
started threads. The consumer drains accepted work unless it independently fails.
No thread is detached. A failure in initial consumer creation starts no other workers.

## Accounting and verification

Each producer owns its attempt counter and disjoint elements of an accepted-ID byte
array. The consumer owns completion flags, processed/failed counters, and its error.
Main reads these only after joining; joins establish visibility. Byte arrays are
used rather than vector<bool>, whose packed bits could share writable storage.

The consumer rejects duplicate completion IDs. A successful run requires every
planned ID to be accepted and the accepted/completed arrays to match exactly. Then
main reopens the database and compares all sorted rows against the deterministic
key/value sequence. This confirms persistence as well as execution.

`processed` means successfully completed, including lookups. `failed` counts a
request that started execution and threw; a startup error is reported separately
and does not count as a failed request. `unprocessed = accepted - processed - failed`.
Attempted counts actual submission attempts, including a rejected push, not all
planned requests after early shutdown. Failure counts may vary with scheduling.

The executable reports attempted, accepted, processed, failed, unprocessed, maximum
depth, elapsed time, throughput and verification status. Timing includes startup,
thread execution, joining and verification; it is an illustrative end-to-end rate,
not a database benchmark. Tests never assert performance thresholds.

## Deliberate limits

Two producers by default, one consumer, Insert/Lookup only, no networking, concurrent
writers, thread pool or scheduler. Full-map transactions remain unchanged. Queue
storage is bounded, but verification arrays scale with total planned requests; the
harness as a whole is not constant-memory. The runner requires an empty database
and does not clear existing user data. The executable creates a fresh separate
temporary directory and prints its path, leaving it available for inspection.

Catch2 tests cover FIFO, capacity one, full/empty blocking and resumption, close
waking both waiter types, draining, rejected submissions, multiple producers,
reopened state, consumer sync/startup failures, and empty workloads. The CTest timeout
is a deadlock guard, not a timing assertion or synchronization mechanism.
