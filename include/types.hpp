#pragma once
#include "meta_data.hpp"

namespace type_selector_detail {
template <size_t Bits>
struct TypeSelector;

template <>
struct TypeSelector<8> {
  using UnsignedType = uint8_t;
  using SignedType = int8_t;
};

template <>
struct TypeSelector<16> {
  using UnsignedType = uint16_t;
  using SignedType = int16_t;
};

template <>
struct TypeSelector<32> {
  using UnsignedType = uint32_t;
  using SignedType = int32_t;
};

template <>
struct TypeSelector<64> {
  using UnsignedType = uint64_t;
  using SignedType = int64_t;
};
}  // namespace type_selector_detail

template <size_t Bits>
using SignedType = typename type_selector_detail::TypeSelector<Bits>::SignedType;
template <size_t Bits>
using UnsignedType = typename type_selector_detail::TypeSelector<Bits>::UnsignedType;

constexpr size_t BLOCK_BIT_OFT = 12;  // 4KB
constexpr size_t BLOCK_SIZE = 1 << BLOCK_BIT_OFT;

constexpr uint32_t MAX_JOB_NUM = 16;

// using VertexID = uint32_t;
// using EdgeID = uint64_t;
// using EdgeT = std::pair<VertexID, VertexID>;
// constexpr uint32_t EDGE_NUM_PER_BLOCK = BLOCK_SIZE / sizeof (VertexID);

using ull = unsigned long long;
using Priority = int32_t;
enum PriorityOrder { DECREASING, INCREASING };
template <PriorityOrder order>
bool prioriTo(Priority a, Priority b) {
  if constexpr (order == PriorityOrder::DECREASING) {
    return a > b;
  } else {
    return a < b;
  }
}

constexpr Priority MAX_PRIORITY = 4096;

template <PriorityOrder order>
constexpr Priority leastPriority() {
  if constexpr (order == PriorityOrder::INCREASING) {
    return std::numeric_limits<Priority>::max();
  } else {
    return std::numeric_limits<Priority>::min();
  }
}

using BlockID = uint32_t;
using VertexID = uint32_t;
using EdgeID = uint64_t;

constexpr size_t CACHELINE_SIZE = 64;
constexpr size_t PARALLEL_HASH_MAP_N = 4;  // 2^4=16

#ifndef O_DIRECT
#define O_DIRECT 0
#endif
