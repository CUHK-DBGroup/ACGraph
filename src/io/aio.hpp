#include <thread>
#include <filesystem>
#include "concurrentqueue.hpp"
#include <fcntl.h>
#include <unistd.h>

class BlockReader {
public:
  BlockReader(size_t num_threads, const std::filesystem::path &path)
    : path(path)
//    , sq(csz)
//    , fast_sq(bsz)
  {
    fd = open(path.c_str(), O_RDONLY|O_DIRECT);
    assert(fd != -1);
    for (size_t i = 0; i < num_threads; ++i) {
      threads.emplace_back([&]() {
        work();
      });
    }
  }

  ~BlockReader() {
    exit();
    for (auto &thread : threads) {
      assert(thread.joinable());
      thread.join();
    }
    close(fd);
  }

  template<std::convertible_to<std::function<void()>> From>
  void load(char *dst, uint32_t sz, ssize_t oft, From &&callback) {
    pread(fd, dst, sz, oft);
    callback();
    // sq.enqueue( Item { .dst = dst, .sz = sz, .oft = oft, .callback = std::forward<From>(callback)});
  }

  template<std::convertible_to<std::function<void()>> From>
  void load_fast(size_t oft, uint32_t sz, char *dst, From &&callback) {
    fast_sq.enqueue(Item {.oft = oft, .sz = sz, .dst = dst, .callback = std::forward<From>(callback)});
  }

  void exit() { done.test_and_set(std::memory_order_release); }

private:
  void work() {
    while (!done.test(std::memory_order_acquire)) {
      Item item;
      if (!fast_sq.try_dequeue(item)) {
        if (!sq.try_dequeue(item)) {
          std::this_thread::yield();
          continue;
        }
      }
      assert(pread(fd, item.dst, item.sz, item.oft) == item.sz);
//      std::cout << "load: " << size_t(item.dst) << ", " << item.sz << ", " << item.oft << std::endl;
      item.callback();
    }
  }

  struct Item { char *dst{}; uint32_t sz{}; ssize_t oft{}; std::function<void()> callback; };

  int fd;
  std::filesystem::path path;
  std::vector<std::thread> threads;
  moodycamel::ConcurrentQueue<Item> sq; //submission queue
  moodycamel::ConcurrentQueue<Item> fast_sq;

  std::atomic_flag done;
};