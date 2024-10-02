#ifndef ACG_UTIL_HPP
#define ACG_UTIL_HPP

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

namespace acg {
struct Murmurhash3_32_Impl {
 public:
  template <typename T>
  static constexpr uint32_t hash(T &&key, uint32_t seed) {
    const uint32_t *data = reinterpret_cast<const uint32_t *>(&key);

    constexpr size_t len = sizeof(T);
    constexpr size_t n = len / 4;
    uint32_t h = seed;
    Detail<n>::apply(data, h);

    uint32_t k = 0;
    std::memcpy(&k, data + n, len % 4);
    h ^= murmurhash3_32_scramble(k);
    h ^= len;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
  }

 private:
  template <size_t N>
  struct Detail {
    static constexpr void apply(const uint32_t *data, uint32_t &ret) {
      if constexpr (N > 0) {
        uint32_t k = *data;
        ret ^= murmurhash3_32_scramble(k);
        ret = (ret << 13) | (ret >> 19);
        ret = ret * 5 + 0xe6546b64;
        return Detail<N - 1>::apply(data + 1, ret);
      }
    }
  };

  static constexpr uint32_t murmurhash3_32_scramble(uint32_t key) {
    key *= 0xcc9e2d51;
    key = (key << 15) | (key >> 17);
    key *= 0x1b873593;
    return key;
  }
};

template <typename T>
uint32_t murmurhash3_32(T &&key, uint32_t seed) {
  return Murmurhash3_32_Impl::hash(std::forward<T>(key), seed);
}

std::string_view trim(std::string_view s);

template <typename S, typename T>
struct converter {
  static_assert(sizeof(S) >= sizeof(T) && (sizeof(S) % sizeof(T) == 0));

  template <size_t oft, typename = void>
  struct __helper {
    template <typename F>
    static void apply(S s, T ed, F &&f) {
      static_assert(sizeof(S) <= sizeof(T) * oft);
    }

    template <typename F>
    static void apply(S s, F &&f) {
      static_assert(sizeof(S) <= sizeof(T) * oft);
    }
  };

  template <size_t oft>
  struct __helper<oft, typename std::enable_if<(sizeof(S) >= sizeof(T) * (oft + 1)), void>::type> {
    template <typename F>
    static void apply(S s, T ed, F &&f) {
      auto &dst = *(reinterpret_cast<T *>(&s) + oft);
      if (dst != ed) {
        f(dst);
        __helper<oft + 1>::apply(s, ed, std::forward<F>(f));
      }
    }

    template <typename F>
    static void apply(S s, F &&f) {
      auto &dst = *(reinterpret_cast<T *>(&s) + oft);
      f(dst);
      __helper<oft + 1>::apply(s, std::forward<F>(f));
    }
  };

  static int count(S s, T ed) {
    int num = 0;
    __helper<0>::apply(s, [&](auto x) { num += (x != ed); });
    return num;
  }

  static int count_until(S s, T ed) {
    int num = 0;
    __helper<0>::apply_until(s, [&](auto) { num++; });
    return num;
  }

  template <typename F>
  static void apply(S s, T ed, F &&f) {
    __helper<0>::apply(s, ed, std::forward<F>(f));
  }

  template <typename F>
  static void apply(S s, F &&f) {
    __helper<0>::apply(s, std::forward<F>(f));
  }
};

using ExecInfo = std::tuple<uint64_t, uint64_t, uint64_t>;

struct StatInfo {
  StatInfo() : n_tasks(0), n_vertices(0), n_edges(0) {}

  void hit_tasks(uint64_t num = 1);
  void hit_vertices(uint64_t num = 1);
  void hit_edges(uint64_t num = 1);
  void hit(uint64_t n_tasks, uint64_t n_vertices, uint64_t n_edges);

  ExecInfo get() const;

 private:
  std::atomic_uint64_t n_tasks;
  std::atomic_uint64_t n_vertices;
  std::atomic_uint64_t n_edges;
};

std::string getCurrentTime();

}  // namespace acg

#endif  // ACG_UTIL_HPP
