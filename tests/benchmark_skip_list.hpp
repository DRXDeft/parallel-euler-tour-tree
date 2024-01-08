#pragma once

#include <cassert>
#include <iostream>
#include <random>
#include <vector>

#include "parse_command_line.h"
#include <parlay/internal/get_time.h>
#include <psl/debug.hpp>
#include <psl/utils.h>

namespace sequence_benchmark {

using parlay::internal::timer;
struct BenchmarkParameters {
  int num_elements;
  int batch_size;
  int num_iterations;
};

inline BenchmarkParameters GetBenchmarkParameters(int argc, char **argv) {
  commandLine P{argc, argv,
                "./benchmark [-n (num elements)] [-k (batch size)] [-iters]"};
  BenchmarkParameters parameters{};
  parameters.num_elements = P.getOptionIntValue("-n", 1000);
  parameters.batch_size =
      P.getOptionIntValue("-k", parameters.num_elements / 32);
  parameters.num_iterations = P.getOptionIntValue("-iters", 5);
  return parameters;
}

// Pick `batch_size` many element locations at random. For `num_iterations`
// iterations, construct a list and then split and join on those locations.
// Report the median time to perform all these splits and joins.
template <typename Element>
void RunBenchmark(Element *elements, const BenchmarkParameters &parameters) {
  const int num_elements{parameters.num_elements};
  const int batch_size{parameters.batch_size};
  const int num_iterations{parameters.num_iterations};

  assert(0 <= batch_size && batch_size < num_elements);
  std::cout << "Running with " << parlay::num_workers() << " workers"
            << std::endl;

  int *perm{new_array_no_init<int>(num_elements - 1)};
  for (int i = 0; i < num_elements - 1; i++) {
    perm[i] = i;
  }
  std::mt19937 generator{0};
  std::shuffle(perm, perm + num_elements - 1, generator);

  std::vector<double> split_times(num_iterations);
  std::vector<double> join_times(num_iterations);

  for (int j = 0; j < num_iterations; j++) {
    // construct list
    parlay::parallel_for(0, num_elements - 1, [&](size_t i) {
      Element::Join(&elements[perm[i]], &elements[perm[i] + 1]);
    });

    timer split_t;
    split_t.start();
    parlay::parallel_for(0, batch_size,
                         [&](size_t i) { elements[perm[i]].Split(); });
    split_times[j] = split_t.stop();

    timer join_t;
    join_t.start();
    parlay::parallel_for(0, batch_size, [&](size_t i) {
      int idx{perm[i]};
      Element::Join(&elements[idx], &elements[idx + 1]);
    });
    join_times[j] = join_t.stop();

    // destroy list
    parlay::parallel_for(0, num_elements - 1,
                         [&](size_t i) { elements[i].Split(); });
  }

  std::cout << "join " << median(join_times) << " split" << median(split_times)
            << '\n';

  Element *representative_0{elements[0].FindRepresentative()};
  parlay::parallel_for(0, num_elements, [&](size_t i) {
    assert(representative_0 == elements[i].FindRepresentative());
  });

  delete_array(perm, num_elements - 1);
}

} // namespace sequence_benchmark