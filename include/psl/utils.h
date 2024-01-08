#pragma once
#include <iostream>
#include <parlay/parallel.h>
#if defined(MCX16)
// ET should be 128 bits and 128-bit aligned
template <class ET> inline bool CAS128(ET *a, ET b, ET c) {
  return __sync_bool_compare_and_swap_16((__int128 *)a, *((__int128 *)&b),
                                         *((__int128 *)&c));
}
#endif
template <class ET> inline bool CAS(ET *ptr, ET oldv, ET newv) {
  if (sizeof(ET) == 1) {
    return __sync_bool_compare_and_swap((bool *)ptr, *((bool *)&oldv),
                                        *((bool *)&newv));
  } else if (sizeof(ET) == 4) {
    return __sync_bool_compare_and_swap((int *)ptr, *((int *)&oldv),
                                        *((int *)&newv));
  } else if (sizeof(ET) == 8) {
    return __sync_bool_compare_and_swap((long *)ptr, *((long *)&oldv),
                                        *((long *)&newv));
  }
#if defined(MCX16)
  else if (sizeof(ET) == 16) {
    return CAS128(ptr, oldv, newv);
  }
#endif
  else {
    std::cout << "CAS bad length : " << sizeof(ET) << std::endl;
    std::abort();
  }
}

template <class ET> inline bool writeMin(ET *a, ET b) {
  ET c;
  bool r = 0;
  do
    c = *a;
  while (c > b && !(r = CAS(a, c, b)));
  return r;
}

template <class ET> inline void writeAdd(ET *a, ET b) {
  volatile ET newV, oldV;
  do {
    oldV = *a;
    newV = oldV + b;
  } while (!CAS(a, oldV, newV));
}
// Does not initialize the array
template <typename E> E *new_array_no_init(size_t n, bool touch_pages = false) {
  // pads in case user wants to allign with cache lines
  size_t line_size = 64;
  size_t bytes = ((n * sizeof(E)) / line_size + 1) * line_size;
#ifndef __APPLE__
  E *r = (E *)aligned_alloc(line_size, bytes);
#else
  E *r;
  if (posix_memalign((void **)&r, line_size, bytes) != 0) {
    std::cout << "Cannot allocate space\n";
    exit(1);
  }
#endif
  if (r == NULL) {
    std::cout << "Cannot allocate space";
    exit(1);
  }
  // a hack to make sure tlb is full for huge pages
  if (touch_pages)
    parlay::blocked_for(
        0, bytes, (1 << 21),
        [&](size_t i, size_t start, size_t end) { ((bool *)r)[i] = 0; });
  return r;
}
// Destructs in parallel
template <typename E> void delete_array(E *A, size_t n) {
  // C++14 -- suppored by gnu C++11
  if (!std::is_trivially_destructible<E>::value) {
    if (n > 2048)
      parlay::parallel_for(0, n, [&](size_t i) { A[i].~E(); });
    else
      for (size_t i = 0; i < n; i++)
        A[i].~E();
  }
  free(A);
}
template <typename T> T median(std::vector<T> v) {
  const size_t len = v.size();
  if (len == 0) {
    std::cerr << "median(): empty vector" << std::endl;
    abort();
  }
  std::sort(v.begin(), v.end());
  if (len % 2) {
    return v[len / 2];
  } else {
    return v[len / 2 - 1] + (v[len / 2] - v[len / 2 - 1]) / 2;
  }
}