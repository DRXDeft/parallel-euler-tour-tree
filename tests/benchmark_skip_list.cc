#include "benchmark_skip_list.hpp"
#include "benchmark_augmented_skip_list.hpp"
#include "psl/augmented_skip_list.hpp"
#include <psl/skip_list.hpp>
#include <psl/utils.h>
void skip_list(int argc, char **argv) {
  namespace sb = sequence_benchmark;
  using Element = parallel_skip_list::Element;
  sb::BenchmarkParameters parameters{sb::GetBenchmarkParameters(argc, argv)};
  Element::Initialize();
  Element *elements{new_array_no_init<Element>(parameters.num_elements)};
  parlay::random r{};
  parlay::parallel_for(0, parameters.num_elements, [&](size_t i) {
    new (&elements[i]) Element{r.ith_rand(i)};
  });

  sequence_benchmark::RunBenchmark(elements, parameters);

  delete_array(elements, parameters.num_elements);
  Element::Finish();
}
void augmented_skip_list(int argc, char **argv) {
  namespace bsb = batch_sequence_benchmark;
  using Element = parallel_skip_list::AugmentedElement;
  using std::string;
  bsb::BenchmarkParameters parameters{bsb::GetBenchmarkParameters(argc, argv)};
  Element::Initialize();
  Element *elements{new_array_no_init<Element>(parameters.num_elements)};
  parlay::random r{};
  parlay::parallel_for(0, parameters.num_elements, [&](size_t i) {
    new (&elements[i]) Element{r.ith_rand(i)};
  });

  bsb::RunBenchmark(elements, parameters);

  delete_array(elements, parameters.num_elements);
  Element::Finish();
}
int main(int argc, char **argv) {
  skip_list(argc, argv);
  augmented_skip_list(argc, argv);
  return 0;
}