#ifndef ACG_THREAD_POOL_HPP
#define ACG_THREAD_POOL_HPP

#include <condition_variable>
#include <thread>

namespace acg {
class ThreadPool {
 public:
 private:
  size_t num_threads;
};

struct thread_wrapper_t {
 public:
  //  using job_t = job_wrapper;
  using job_t = std::function<void()>;

  thread_wrapper_t() : looper(), thread([&]() { looper(); }) {}

  //  thread_wrapper_t(thread_wrapper_t &&) = delete;
  //  thread_wrapper_t(const thread_wrapper_t &) = delete;

  void push(job_t &&f) { looper.push(std::forward<job_t>(f)); }

  void exit() {
    looper.exit();
    assert(thread.joinable());
    thread.join();
  }

  //  [[nodiscard]] bool joinable() const { return thread.joinable(); }

  //  void join() { thread.join(); }

  auto get_tid() { return thread.get_id(); }

 private:
  struct _looper_t {
    _looper_t() : done(false) {}

    void operator()() {
      //      job_t job;
      while (true) {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&]() { return !jobs.empty() || done; });
        if (done) break;
        auto job = jobs.front();
        jobs.pop();
        lock.unlock();
        job();
      }
    }

    void push(job_t &&f) {
      std::lock_guard<std::mutex> lg(mutex);
      jobs.push(std::forward<job_t>(f));
      cv.notify_all();
    }

    void exit() {
      std::lock_guard<std::mutex> lg(mutex);
      done = true;
      cv.notify_all();
    }

    bool done;
    std::queue<job_t> jobs;
    std::mutex mutex;
    std::condition_variable cv;
  };

  _looper_t looper;

  std::thread thread;
};

}  // namespace acg
#endif  // ACG_THREAD_POOL_HPP
