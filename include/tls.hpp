#ifndef ACG_TLS_HPP
#define ACG_TLS_HPP

#include <atomic>
#include <cassert>
#include <mutex>
#include <vector>

namespace acg {
template <typename T>
class ThreadLocalStorage {
 public:
  ThreadLocalStorage() { iid = iid_counter++; }

  ~ThreadLocalStorage() {
    std::lock_guard<std::mutex> lg(instance_lock);
    for (auto data : data_per_instance) {
      delete data;
    }
  }
  template <typename... Args>
  void initThread(Args &&...args) {
    while (data_per_thread.size() <= iid) {
      data_per_thread.push_back(nullptr);
    }
    data_per_thread[iid] = new T(std::forward<Args>(args)...);

    std::lock_guard<std::mutex> lg(instance_lock);
    data_per_instance.push_back(data_per_thread[iid]);
  }

  T *get_data() {
    assert(data_per_thread.size() > iid);
    return data_per_thread[iid];
  }

  const T *get_data() const {
    assert(data_per_thread.size() > iid);
    return data_per_thread[iid];
  }

  // ! thread unsafe
  std::vector<T *> getInstanceAll() { return data_per_instance; }

  const std::vector<T *> &getInstanceAll() const { return data_per_instance; }

 private:
  static std::atomic_size_t iid_counter;
  size_t iid;
  std::mutex instance_lock;
  std::vector<T *> data_per_instance;
  static thread_local std::vector<T *> data_per_thread;
};

template <typename T>
std::atomic_size_t ThreadLocalStorage<T>::iid_counter = 0;

template <typename T>
thread_local std::vector<T *> ThreadLocalStorage<T>::data_per_thread{};
}  // namespace acg
#endif  // ACG_TLS_HPP