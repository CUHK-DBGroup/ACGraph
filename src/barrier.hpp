
#include <cassert>
#include <atomic>

struct barrier_t {

  barrier_t(): total(0), running(0) {}
  explicit barrier_t(size_t num): total(num), running(num) {}

  ~barrier_t() { while (done.load() != total) std::this_thread::yield(); }

  void set(size_t num) { running = num; }

  size_t wait() {
    std::unique_lock<std::mutex> lock(mutex);
    running.fetch_sub(1);
    if (running.load() == 0) {
      cv.notify_all();
      done.fetch_add(1);
    } else {
      cv.wait(lock, [&]() { return running == 0; });
      done.fetch_add(1);
    }
    return 0;
  }


private:
  std::mutex mutex;
  std::condition_variable cv;
  size_t total;
  std::atomic_size_t done;
  std::atomic_size_t running;
};