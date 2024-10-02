#ifndef ACG_BLOCK_INFO_HPP
#define ACG_BLOCK_INFO_HPP

#include <concepts>
#include <functional>
#include <span>

#include "adaptive_set.hpp"
#include "atomic_ptr.hpp"
#include "types.hpp"

namespace acg {

template <typename VID, PriorityOrder ORDER = INCREASING, size_t JOB_NUM = 1>
struct alignas(64) BlockInfo {
  static_assert(JOB_NUM <= MAX_JOB_NUM, "JOB_NUM must be less than or equal to MAX_JOB_NUM");
  static_assert(JOB_NUM >= 1, "JOB_NUM must be greater than or equal to 1");

  static constexpr size_t FIXED_FIELDS_SIZE = sizeof(atomic_ptr<void *>) + sizeof(Priority);
  using Frontiers = AdaptiveSet<VID, CACHELINE_SIZE - FIXED_FIELDS_SIZE-2>;

  alignas(8) atomic_ptr<void> data;
  alignas(4) Priority prio = leastPriority<ORDER>();
  alignas(4) std::array<Frontiers, JOB_NUM> frontiers_set;
  // alignas(1) uint8_t num_exec = 0;
  alignas(1) bool cached = false;

  void init(VID v) {
    for (auto &frontiers : frontiers_set) frontiers.set_vop(v);
  }

  bool isActive() {
    for (auto &frontiers : frontiers_set) {
      if (!frontiers.empty()) return true;
    }
    return false;
  }

  bool isActive(size_t jid) { return !frontiers_set[jid].empty(); }

  void insert(VID v, size_t jid = 0) { frontiers_set[jid].insert(v); }

  template <std::ranges::input_range R>
  void insertBatch(R &&vs, size_t jid = 0) {
    for (auto &&v : vs) {
      frontiers_set[jid].insert(std::forward<decltype(v)>(v));
    }
  }

  bool updatePriority(Priority new_prio, VID v, size_t jid = 0) {
    frontiers_set[jid].insert(v);
    if (!prio || prioriTo<ORDER>(prio, new_prio)) {
      prio = new_prio;
      return true;
    }
    return false;
  }

  template <std::ranges::input_range R>
  bool updatePriorityBatch(R &&pvs, size_t jid = 0) {
    Priority new_prio = leastPriority<ORDER>();
    for (auto &&[p, v] : pvs) {
      if (prioriTo<ORDER>(p, new_prio)) new_prio = p;
      frontiers_set[jid].insert(std::forward<decltype(v)>(v));
    }
    if (!prio || prioriTo<ORDER>(new_prio, prio)) {
      prio = new_prio;
      return true;
    }
    return false;
  }

  Frontiers getFrontiers(size_t jid = 0) { return frontiers_set[jid]; }

  void foreach_active_job(std::function<void(size_t)> &&func) {
    for (size_t i = 0; i < JOB_NUM; i++) {
      if (!frontiers_set[i].empty()) {
        func(i);
      }
    }
  }

  void resetPriority() { prio = leastPriority<ORDER>(); }

  void clearFrontiers(size_t jid = 0) { frontiers_set[jid].clear(); }

  void clearAll() {
    for (size_t i = 0; i < JOB_NUM; i++) {
      frontiers_set[i].clear();
    }
  }

  void lock() { data.lock(); }

  bool try_lock() { return data.try_lock(); }

  void unlock() { data.unlock(); }
};

// template <typename VID, PriorityOrder ORDER>
// struct alignas(64) BlockInfo<VID, ORDER, 1> {
//   static constexpr size_t FIXED_FIELDS_SIZE = sizeof(atomic_ptr<void *>) + sizeof(Priority);
//   static constexpr Priority LEAST_PRIORITY = leastPriority<ORDER>();

//   using Frontiers = AdaptiveSet<VID, CACHELINE_SIZE - FIXED_FIELDS_SIZE>;

//   alignas(8) atomic_ptr<void> data;
//   alignas(4) Priority prio = LEAST_PRIORITY;
//   alignas(4) Frontiers frontiers;

//   void init(VID v) { frontiers.set_vop(v); }

//   bool isActive() { return !frontiers.empty(); }

//   bool insert(VID v) {
//     bool ret = frontiers.empty();
//     frontiers.insert(v);
//     return ret;
//   }

//   template <std::ranges::input_range R>
//   bool insertBatch(R &&vs) {
//     bool ret = frontiers.empty() && !vs.empty();
//     for (auto &&v : vs) {
//       frontiers.insert(std::forward<decltype(v)>(v));
//     }
//     return ret;
//   }

//   bool updatePriority(Priority new_prio, VID v) {
//     frontiers.insert(v);
//     if (!prio || prioriTo<ORDER>(prio, new_prio)) {
//       prio = new_prio;
//       return true;
//     }
//     return false;
//   }

//   template <std::ranges::input_range R>
//   bool updatePriorityBatch(R &&pvs) {
//     Priority new_prio = LEAST_PRIORITY;
//     for (auto &&[p, v] : pvs) {
//       if (prioriTo<ORDER>(p, new_prio)) new_prio = p;
//       frontiers.insert(std::forward<decltype(v)>(v));
//     }
//     if (!prio || prioriTo<ORDER>(new_prio, prio)) {
//       prio = new_prio;
//       return true;
//     }
//     return false;
//   }

//   Frontiers getFrontiers() { return frontiers; }

//   void foreach_active_job(std::function<void()> &&func) {
//     if (!frontiers.empty()) {
//       func();
//     }
//   }

//   void clear() {
//     frontiers.clear();
//     prio = LEAST_PRIORITY;
//   }

//   void lock() { data.lock(); }

//   bool try_lock() { return data.try_lock(); }

//   void unlock() { data.unlock(); }
// };

// Explicit instantiations
extern template struct BlockInfo<uint32_t, INCREASING, 1>;
extern template struct BlockInfo<uint32_t, DECREASING, 1>;
extern template struct BlockInfo<uint32_t, INCREASING, 2>;
extern template struct BlockInfo<uint32_t, DECREASING, 2>;

}  // namespace acg
#endif  // ACG_BLOCK_INFO_HPP
