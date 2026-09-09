#include "workload/workload.h"
#include "workload/request_queue.h"
#include "table/items_table.h"
#include <algorithm>
#include <exception>
#include <limits>
#include <stdexcept>
#include <thread>
#include <vector>

namespace miniwaldb::workload {
Result run(const std::string& directory, const Options& options, wal::SyncHook sync_hook) {
  if (options.producers == 0 || options.capacity == 0) {
    throw std::invalid_argument("producer count and capacity must be positive");
  }
  const auto limit = std::min<std::uintmax_t>(std::numeric_limits<std::size_t>::max() / 2,
                                           std::numeric_limits<std::int64_t>::max());
  if (options.keys_per_producer > limit / options.producers) {
    throw std::invalid_argument("workload is too large");
  }
  const auto keys = options.producers * options.keys_per_producer;
  const auto total = keys * 2;
  RequestQueue queue(options.capacity);
  Result result;
  // Disjoint byte elements are written by their producer; never use vector<bool> here.
  std::vector<unsigned char> accepted(total, 0);
  std::vector<unsigned char> completed(total, 0); // Consumer alone writes this array.
  struct ProducerResult { std::size_t attempted{}; std::exception_ptr error; };
  std::vector<ProducerResult> producer_results(options.producers);
  std::exception_ptr consumer_error;
  std::exception_ptr launch_error;
  std::vector<std::thread> producers;
  producers.reserve(options.producers); // Allocate before starting any threads.

  std::thread consumer([&] {
    bool executing = false;
    try {
      Db db(directory, std::move(sync_hook));
      ItemsTable items(db);
      if (!items.scan().empty()) throw std::runtime_error("workload requires an empty database");
      Request request;
      while (queue.pop(request)) {
        executing = true;
        if (request.id >= total || completed[request.id]) {
          throw std::runtime_error("invalid or duplicate request ID");
        }
        if (request.operation == Operation::Insert) {
          db.begin();
          items.insert(request.key, request.value);
          db.commit();
        } else {
          const auto row = items.lookup(request.key);
          if (!row || row->value != request.value) throw std::runtime_error("lookup verification failed");
        }
        completed[request.id] = 1;
        ++result.processed;
        executing = false;
      }
    } catch (...) {
      if (executing) ++result.failed;
      consumer_error = std::current_exception();
      queue.close(); // Wake producers even if the consumer cannot drain the queue.
    }
  });

  try {
    for (std::size_t producer = 0; producer < options.producers; ++producer) {
      producers.emplace_back([&, producer] {
        try {
          for (std::size_t index = 0; index < options.keys_per_producer; ++index) {
            const auto key = producer * options.keys_per_producer + index;
            const auto value = "value-" + std::to_string(key);
            for (std::size_t operation = 0; operation < 2; ++operation) {
              const auto id = key * 2 + operation;
              ++producer_results[producer].attempted;
              Request request{id, operation == 0 ? Operation::Insert : Operation::Lookup,
                              static_cast<std::int64_t>(key), value};
              if (!queue.push(std::move(request))) return;
              accepted[id] = 1;
            }
          }
        } catch (...) {
          producer_results[producer].error = std::current_exception();
          queue.close();
        }
      });
    }
  } catch (...) {
    launch_error = std::current_exception();
    queue.close(); // A thread-creation failure must not strand already started threads.
  }
  for (auto& producer : producers) producer.join();
  queue.close();
  consumer.join();

  // Joining establishes visibility of each thread's results. No statistics mutex needed.
  std::exception_ptr error = consumer_error ? consumer_error : launch_error;
  for (const auto& producer : producer_results) {
    result.attempted += producer.attempted;
    if (!error) error = producer.error;
  }
  for (auto entry : accepted) result.accepted += entry;
  result.unprocessed = result.accepted - result.processed - result.failed;
  result.max_depth = queue.max_depth();
  try {
    if (error) std::rethrow_exception(error);
    if (result.accepted != total || accepted != completed) {
      throw std::runtime_error("accepted/completed request IDs do not match the workload");
    }
    // The consumer and its Db are gone. Only now may this thread reopen the database.
    Db reopened(directory);
    const auto rows = ItemsTable(reopened).scan();
    if (rows.size() != keys) throw std::runtime_error("final row count mismatch");
    for (std::size_t key = 0; key < keys; ++key) {
      if (rows[key].id != static_cast<std::int64_t>(key) ||
          rows[key].value != "value-" + std::to_string(key)) {
        throw std::runtime_error("final row contents mismatch");
      }
    }
    result.verified = true;
  } catch (const std::exception& e) {
    result.error = e.what();
  } catch (...) {
    result.error = "unknown workload failure";
  }
  return result;
}
}
