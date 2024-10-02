#ifndef ACG_ATOMIC_PTR_HPP
#define ACG_ATOMIC_PTR_HPP

#include <atomic>
#include <cassert>
namespace acg {
template <typename T>
class atomic_ptr {
  std::atomic<uintptr_t> ptr;

 public:
  atomic_ptr() : ptr(0) {}

  explicit atomic_ptr(T* ptr) : ptr(reinterpret_cast<uintptr_t>(ptr)) {}

  inline T* lock() {
    while (true) {
      auto old_val = ptr.load(std::memory_order_relaxed);
      if (old_val & 1) continue;
      if (ptr.compare_exchange_weak(old_val, old_val | 1, std::memory_order_acq_rel,
                                    std::memory_order_relaxed)) {
        assert(is_locked());
        return reinterpret_cast<T*>(old_val);
      }
    }
  }

  inline void unlock() {
    assert(is_locked());
    ptr.store(ptr.load(std::memory_order_relaxed) & ~(uintptr_t)1, std::memory_order_release);
  }

  inline void unlock_and_clear() {
    assert(is_locked());
    ptr.store(0, std::memory_order_release);
  }

  inline void unlock_and_set(T* val) {
    assert(is_locked());
    assert(!((uintptr_t)val & 1));
    ptr.store((uintptr_t)val, std::memory_order_release);
  }

  inline T* load() const { return (T*)(ptr.load(std::memory_order_relaxed) & ~(uintptr_t)1); }

  inline void store(T* val) {
    auto new_val = ((uintptr_t)val) | (ptr & 1);
    // relaxed OK since this doesn't clear lock
    ptr.store(new_val, std::memory_order_relaxed);
  }

  inline bool try_lock() {
    uintptr_t old_val = ptr.load(std::memory_order_relaxed);
    if ((old_val & 1) != 0) return false;
    old_val = ptr.fetch_or(1, std::memory_order_acq_rel);
    return !(old_val & 1);
  }

  inline bool is_locked() const { return ptr.load(std::memory_order_acquire) & 1; }

  //! CAS only works on unlocked values
  //! the lock bit will prevent a successful cas
  inline bool CAS(T* old_val, T* new_val) {
    assert(!((uintptr_t)old_val & 1) && !((uintptr_t)new_val & 1));
    auto old = (uintptr_t)old_val;
    return ptr.compare_exchange_strong(old, (uintptr_t)new_val);
  }
};
}  // namespace acg
#endif  // ACG_ATOMIC_PTR_HPP
