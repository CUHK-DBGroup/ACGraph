#ifndef ACG_BUFFERPOOL_HPP
#define ACG_BUFFERPOOL_HPP

#include <concurrentqueue.h>

#include <filesystem>

#include "types.hpp"

namespace acg {
template <std::integral BlockID>
class BufferPool {
 private:
  struct BlockData {
    char data[BLOCK_SIZE];
  } __attribute__((aligned(BLOCK_SIZE)));
  using FreeQueue = moodycamel::ConcurrentQueue<char *>;

 public:
  BufferPool(size_t cache_num_) : cache_num_(cache_num_), cache_blocks(cache_num_) {
    std::vector<char *> buf(cache_num_);
    for (size_t i = 0; i < cache_num_; i++) {
      buf[i] = cache_blocks[i].data;
    }
    free_blocks.enqueue_bulk(buf.begin(), cache_num_);
    num_free_blocks.fetch_add(cache_num_, std::memory_order_relaxed);
  }

  char *alloc() {
    char *ptr = nullptr;
    if (free_blocks.try_dequeue(ptr)) {
      num_free_blocks.fetch_sub(1, std::memory_order_relaxed);
      return ptr;
    }
    return nullptr;
  }

  std::vector<char *> allocBatch(size_t num) {
    std::vector<char *> ptrs(num);
    auto count = free_blocks.try_dequeue_bulk(ptrs.begin(), num);
    ptrs.resize(count);
    num_free_blocks.fetch_sub(count, std::memory_order_relaxed);
    return ptrs;
  }

  void release(char *ptr) {
    free_blocks.enqueue(ptr);
    num_free_blocks.fetch_add(1, std::memory_order_relaxed);
  }

  void releaseBatch(std::span<char *> ptrs) {
    free_blocks.enqueue_bulk(ptrs.begin(), ptrs.size());
    num_free_blocks.fetch_add(ptrs.size(), std::memory_order_relaxed);
  }

  size_t size() const { return num_free_blocks.load(std::memory_order_relaxed); }

  size_t getCacheNum() const { return cache_num_; }

 private:
  size_t cache_num_ = 0;
  std::atomic_uint64_t num_free_blocks = 0;
  std::vector<BlockData> cache_blocks;
  FreeQueue free_blocks;
};
}  // namespace acg
#endif  // ACG_BUFFERPOOL_HPP
