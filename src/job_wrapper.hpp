#pragma once

#include <atomic>
#include <cassert>
#include <span>
#include <thread>
#include <vector>

#include "util.hpp"

struct job_wrapper {
  explicit job_wrapper(size_t num_workers = 1) : num_workers(num_workers) {}

  virtual ~job_wrapper() = default;

  void operator()() {
    assert(spawned.load() < num_workers);
    spawned.fetch_add(1);
    execute();
    done.fetch_add(1);
  }

  [[nodiscard]] bool finished() const noexcept {
    return spawned.load() == num_workers && done.load() == spawned.load();
  }

  void wait() const noexcept {
    while (!finished()) std::this_thread::yield();
    //    std::cout << done << "/ " << spawned << std::endl;
  }

 protected:
  virtual void execute() = 0;
  size_t num_workers{0};
  std::atomic_size_t spawned;
  std::atomic_size_t done;
};

struct simple_job : job_wrapper {
  template <std::convertible_to<std::function<void()>> From>
  explicit simple_job(From &&f) : f(f) {}

 private:
  void execute() override { f(); }

  std::function<void()> f;
};

template <typename F, typename WL, size_t BLOCK_SIZE = 4096>
struct async_job : job_wrapper {
 private:
  static constexpr size_t BUF_BLOCK_NUM = 16;
  static constexpr size_t BUF_SIZE = BLOCK_SIZE * BUF_BLOCK_NUM;

  using graph_t = WL::graph_t;
  using vid_t = WL::vid_t;
  using prio_t = WL::prio_t;
  using labeled_vt = WL::labeled_vt;
  F &f;
  WL &wl;
  graph_t &g;

  std::vector<labeled_vt> local_queue;
  std::vector<vid_t> bound_v;
  StatInfo stat_info;

 public:
  async_job(WL &s, F &f) : job_wrapper(1), f(f), wl(s), g(*wl.get_graph()) {}

  async_job(WL &s, F &f, std::span<const labeled_vt> init_list)
      : job_wrapper(1), f(f), wl(s), g(*wl.get_graph()) {
    for (auto &[v, p] : init_list) {
      if (g.is_bound(v))
        bound_v.push_back(v);
      else
        local_queue.emplace_back(v, p);
    }
    submit();
  }

  async_job(WL &s, F &f, vid_t v, prio_t p) : job_wrapper(1), f(f), wl(s), g(*wl.get_graph()) {
    if (g.is_bound(v))
      bound_v.push_back(v);
    else
      local_queue.emplace_back(v, p);
    submit();
  }

 private:
  void update_neighbors(F::msg_t &val, vid_t dst) {
    auto prio = f.process(val, dst);
    if (prio > prio_t{0}) {
      if (g.is_bound(dst))
        bound_v.push_back(dst);
      else
        local_queue.emplace_back(dst, std::min(prio, 128));
    }
  }

  void submit() {
    for (size_t i = 0; i < bound_v.size(); ++i) {
      auto v = bound_v[i];
      auto val = f.apply(v);
      g.apply_neighbors(v, [&](vid_t dst) { update_neighbors(val, dst); });
    }
    bound_v.clear();

    if (!local_queue.empty()) {
      wl.submit(local_queue);
      local_queue.clear();
    }
  }

 public:
  void execute() override {
    local_queue.reserve(4096);
    bound_v.reserve(4096);
    auto *buf = (char *)std::aligned_alloc(BLOCK_SIZE, BUF_SIZE);

    while (true) {
      // auto task = wl.pull_task();
      // if (!task) break;
      // process(task.value(), buf);
      auto tasks = wl.pullBatch();
      if (tasks.empty()) break;
      for (auto &task : tasks) {
        process(task, buf);
      }
      submit();
      wl.finishBatch(tasks | std::views::transform([](auto &task) { return task.bid; }));
    }
    free(buf);
  }

  void process(WL::task_t &task, char *buf) {
    auto &[tid, frontiers, data] = task;
    stat_info.hit_tasks();
    frontiers.for_each([&](vid_t v) {
      stat_info.hit_vertices();

      auto val = f.apply(v);
      uint32_t cnt = 0;

      if (g.is_oversize(v)) {
        auto bop = g.v2bid_op(v);
        auto n = g.deg(v) * sizeof(vid_t) / BLOCK_SIZE;
        for (size_t i = 0; i < n; i += BUF_BLOCK_NUM) {
          size_t bnum = std::min(n - i, BUF_BLOCK_NUM);
          ssize_t bsize = bnum * BLOCK_SIZE;
          ssize_t read_size = g.read_blocks(buf, bop + i, bnum);
          assert(bsize == read_size);
          g.apply_neighbors(buf, buf + bsize, [&](vid_t dst) {
            update_neighbors(val, dst);
            cnt++;
          });
          if (local_queue.size() + bound_v.size() > 4096) submit();
        }
      }
      g.apply_neighbors(v, data, [&](vid_t dst) {
        update_neighbors(val, dst);
        cnt++;
      });
      assert(cnt == g.deg(v));
      f.finally(v);
      stat_info.hit_edges(cnt);
      if (local_queue.size() + bound_v.size() > 4096) submit();
    });
  }

  std::tuple<size_t, size_t, size_t> get_statistics() const { return stat_info.get(); }
};