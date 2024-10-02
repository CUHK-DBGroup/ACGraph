#ifndef ACG_ADAPTIVE_SET_HPP
#define ACG_ADAPTIVE_SET_HPP
#pragma once

#include <algorithm>
#include <bit>
#include <cassert>
#include <functional>
#include <limits>
#include <ranges>
#include <set>
#include <span>

namespace acg {

#pragma pack(push, 1)
template <typename ElemT, size_t SZ>
struct AdaptiveSet {
  using CellT = uint8_t;
  using CountT = uint16_t;

  static constexpr size_t ELEM_BIT_NUM = sizeof(CellT) * 8;
  static constexpr size_t CELL_NUM = (SZ - sizeof(ElemT) - sizeof(CountT)) / sizeof(CellT);
  static constexpr size_t SPARSE_MAX_NUM = CELL_NUM * sizeof(CellT) / sizeof(ElemT);
  static constexpr size_t MAX_RANGE = CELL_NUM * ELEM_BIT_NUM;

  /// @note SZ must be greater than the size of v_op
  static_assert(SZ > sizeof(ElemT) + sizeof(CountT));
  /// @note MAX_RANGE must be less than or equal to the maximum value of CountT
  static_assert(MAX_RANGE <= std::numeric_limits<CountT>::max());

  void set_vop(ElemT v) { v_op = v; }

  void insert(ElemT v) {
    if (num < SPARSE_MAX_NUM) {
      insertSparse(v);
      return;
    }
    if (num == SPARSE_MAX_NUM) {
      reconstruct();
    }
    insertDense(v);
  }

  void for_each(std::function<void(ElemT)> &&f) {
    if (num <= SPARSE_MAX_NUM) {
      for (int i = 0; i < num; ++i) {
        f(to_ptr()[i]);
      }
      return;
    }
    ElemT oft = v_op;
    std::ranges::for_each(data, [&](CellT &v) {
      if (std::popcount(v) != 0) {
        for (size_t i = 0; i < ELEM_BIT_NUM; ++i) {
          if (v & (1 << i)) f(oft + i);
        }
      }
      oft += ELEM_BIT_NUM;
    });
  }

  void clear() {
    data.fill(0);
    num = 0;
  }

  uint32_t size() const { return num; }

  bool empty() const { return num == 0; }

 private:
  void insertDense(ElemT v) {
    v -= v_op;
    size_t idx = v / ELEM_BIT_NUM;
    size_t mask = 1 << (v % ELEM_BIT_NUM);
    if (data[idx] & mask) return;
    data[idx] |= mask;
    num++;
  }

  void insertSparse(ElemT v) {
    assert(num < SPARSE_MAX_NUM);
    ElemT *ptr = to_ptr();
    for (CountT i = 0; i < num; ++i) {
      if (ptr[i] == v) return;
    }
    ptr[num] = v;
    num++;
  }

  void reconstruct() {
    assert(num == SPARSE_MAX_NUM);
    std::array<CellT, CELL_NUM> vs = data;
    data.fill(0);
    ElemT *op = reinterpret_cast<ElemT *>(vs.begin());
    ElemT *ed = op + SPARSE_MAX_NUM;
    std::for_each(op, ed, [&](ElemT v) { insertDense(v); });
  }

  constexpr ElemT *to_ptr() { return reinterpret_cast<ElemT *>(&data); }

  ElemT v_op = 0;
  std::array<CellT, CELL_NUM> data;
  CountT num = 0;
};
#pragma pack(pop)
}  // namespace acg
#endif