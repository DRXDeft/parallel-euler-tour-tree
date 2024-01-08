#include <cassert>
#include <parlay/parallel.h>
#include <parlay/random.h>
#include <psl/debug.hpp>
#include <psl/skip_list.hpp>
#include <psl/utils.h>

using Element = parallel_skip_list::Element;

constexpr int kNumElements{1000};
// initialized in main(), because otherwise calling the constructor for
// `Element` will fail due to its internal allocators not being initialized yet
Element *elements;

bool split_points[kNumElements];
int start_index_of_list[kNumElements];

// Mark that we will split at prime-numbered indices.
void PrimeSieve() {
  split_points[2] = true;
  for (int i = 3; i < kNumElements; i += 2)
    split_points[i] = true;
  for (int64_t i = 3; i * i < kNumElements; i += 2) {
    if (split_points[i]) {
      for (int64_t j = i * i; j < kNumElements; j += 2 * i) {
        split_points[j] = false;
      }
    }
  }
}

int main() {
  Element::Initialize();
  parlay::random r;
  elements = new_array_no_init<Element>(kNumElements);
  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    new (&elements[i]) Element(r.ith_rand(i));
  });

  PrimeSieve();

  int start_index{0};
  for (int i = 0; i < kNumElements; i++) {
    start_index_of_list[i] = start_index;
    if (split_points[i]) {
      start_index = i + 1;
    }
  }
  start_index_of_list[0] = start_index_of_list[1] = start_index_of_list[2] =
      start_index % kNumElements;

  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    Element *representative_i{elements[i].FindRepresentative()};
    for (int j = i + 1; j < kNumElements; j++) {
      assert(representative_i != elements[j].FindRepresentative());
    }
  });

  // Join all elements together
  parlay::parallel_for(0, kNumElements - 1, [&](size_t i) {
    Element::Join(&elements[i], &elements[i + 1]);
  });

  Element *representative_0{elements[0].FindRepresentative()};
  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    assert(representative_0 == elements[i].FindRepresentative());
  });

  // Join into one big cycle
  Element::Join(&elements[kNumElements - 1], &elements[0]);

  representative_0 = elements[0].FindRepresentative();
  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    assert(representative_0 == elements[i].FindRepresentative());
  });

  // Split into lists
  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    if (split_points[i]) {
      elements[i].Split();
    }
  });

  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    const int start{start_index_of_list[i]};
    assert(elements[start].FindRepresentative() ==
           elements[i].FindRepresentative());
    if (start > 0) {
      assert(elements[start - 1].FindRepresentative() !=
             elements[i].FindRepresentative());
    }
  });

  // Join individual lists into individual cycles
  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    if (split_points[i]) {
      Element::Join(&elements[i], &elements[start_index_of_list[i]]);
    }
  });

  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    const int start{start_index_of_list[i]};
    assert(elements[start].FindRepresentative() ==
           elements[i].FindRepresentative());
    if (start > 0) {
      assert(elements[start - 1].FindRepresentative() !=
             elements[i].FindRepresentative());
    }
  });

  // Break cycles back into lists
  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    if (split_points[i]) {
      elements[i].Split();
    }
  });

  // Join lists together
  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    if (split_points[i]) {
      Element::Join(&elements[i], &elements[(i + 1) % kNumElements]);
    }
  });

  representative_0 = elements[0].FindRepresentative();
  parlay::parallel_for(0, kNumElements, [&](size_t i) {
    assert(representative_0 == elements[i].FindRepresentative());
  });

  delete_array(elements, kNumElements);
  Element::Finish();

  std::cout << "Test complete." << std::endl;

  return 0;
}
