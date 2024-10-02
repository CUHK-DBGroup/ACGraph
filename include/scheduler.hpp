#ifndef ACG_SCHEDULER_HPP
#define ACG_SCHEDULER_HPP

#pragma once
#include <future>
#include <map>
#include <unordered_set>

#include "barrier.hpp"
#include "executor.hpp"
#include "graph.hpp"
#include "thread_pool.hpp"
#include "types.hpp"
#include "util.hpp"
#include "worklist.hpp"

namespace acg {
template <PriorityOrder order = INCREASING, typename BID = uint32_t, typename graph_t = Graph<>>
class ASQScheduler {
 private:
  using vid_t = graph_t::vid_t;
  using worker_id_t = uint32_t;
  using BufPool = BufferPool<BID>;

 public:
  using PV = SQWorklist<order, BID, graph_t>::PV;

  ASQScheduler(graph_t *graph, BufPool *buffer_pool, uint32_t num_threads)
      : num_workers(num_threads),
        g(graph),
        wl(graph, buffer_pool, num_threads),
        threads(num_workers - 1),
        local_buckets(num_workers) {
    barrier_t barrier(num_workers);
    for (auto &w : threads) {
      w.push([&]() {
        wl.init_thread();
        barrier.wait();
      });
    }
    wl.init_thread();
    barrier.wait();
  }

  ~ASQScheduler() { shutdown(); }

  template <typename F>
  void foreachVertex(F f) {
    barrier_t b(num_workers);
    for (worker_id_t i = 1; i < num_workers; ++i) {
      threads[i - 1].push([=, this, &f, &b]() {
        _foreachVertex(i, std::ref<F>(f));
        b.wait();
      });
    }
    _foreachVertex(0, std::ref<F>(f));
    b.wait();
  }

  template <typename F>
  void async_run(F &f, const std::vector<PV> &init_list) {
    const size_t batch_sz = (init_list.size()) / num_workers;
    barrier_t b1(num_workers);
    barrier_t b2(num_workers);

    auto op = init_list.cbegin();
    auto ed = init_list.cbegin() + batch_sz;
    for (worker_id_t i = 1; i < num_workers; ++i) {
      threads[i - 1].push([=, this, &f, &b1, &b2]() {
        SQExecutor<F, decltype(wl), BLOCK_SIZE> e(std::ref(wl), std::ref(f));
        e.init(std::span(op, ed));
        b1.wait();
        e.execute();
        b2.wait();
        auto [n_tasks, n_vertices, n_edges] = e.getStatistics();
        stat_info.hit(n_tasks, n_vertices, n_edges);
      });
      op = ed;
      ed = op + batch_sz;
    }
    SQExecutor<F, decltype(wl), BLOCK_SIZE> e(std::ref(wl), std::ref(f));
    e.init(std::span(op, init_list.cend()));
    b1.wait();
    e.execute();
    b2.wait();
    auto [n_tasks, n_vertices, n_edges] = e.getStatistics();
    stat_info.hit(n_tasks, n_vertices, n_edges);
    num_active_nodes.store(0, std::memory_order_relaxed);
  }

  template <typename F>
  void async_run(F &f, vid_t v, Priority p = 1) {
    barrier_t b1(num_workers);
    barrier_t b2(num_workers);
    for (worker_id_t i = 1; i < num_workers; ++i) {
      threads[i - 1].push([=, this, &f, &b1, &b2]() {
        SQExecutor<F, decltype(wl), BLOCK_SIZE> e(std::ref(wl), std::ref(f));
        b1.wait();
        e.execute();
        auto [n_tasks, n_vertices, n_edges] = e.getStatistics();
        b2.wait();
        stat_info.hit(n_tasks, n_vertices, n_edges);
      });
    }

    SQExecutor<F, decltype(wl), BLOCK_SIZE> e(std::ref(wl), std::ref(f));
    e.init(v, p);
    b1.wait();
    e.execute();
    b2.wait();
    auto [n_tasks, n_vertices, n_edges] = e.getStatistics();
    stat_info.hit(n_tasks, n_vertices, n_edges);
    num_active_nodes.store(0, std::memory_order_relaxed);
  }

  template <typename F>
  void async_run(F &f) {
    StatInfo local_stat_info;
    barrier_t b1(num_workers);
    barrier_t b2(num_workers);
    for (worker_id_t i = 1; i < num_workers; ++i) {
      threads[i - 1].push([=, this, &f, &b1, &b2, &local_stat_info]() {
        SQExecutor<F, decltype(wl), BLOCK_SIZE> e(std::ref(wl), std::ref(f));
        e.init(local_buckets[i]);
        local_buckets[i].resize(0);
        b1.wait();

        e.execute();
        auto [n_tasks, n_vertices, n_edges] = e.getStatistics();
        b2.wait();
        stat_info.hit(n_tasks, n_vertices, n_edges);
        local_stat_info.hit(n_tasks, n_vertices, n_edges);
      });
    }

    SQExecutor<F, decltype(wl), BLOCK_SIZE> e(std::ref(wl), std::ref(f));
    e.init(local_buckets[0]);
    local_buckets[0].resize(0);
    b1.wait();
    e.execute();
    b2.wait();
    auto [n_tasks, n_vertices, n_edges] = e.getStatistics();
    stat_info.hit(n_tasks, n_vertices, n_edges);
    local_stat_info.hit(n_tasks, n_vertices, n_edges);
    num_active_nodes.store(0, std::memory_order_relaxed);
    std::tie(n_tasks, n_vertices, n_edges) = local_stat_info.get();
    log_report("n_tasks: %llu, n_vertices: %llu, n_edges: %llu",
               ull(n_tasks), ull(n_vertices), ull(n_edges));
  }

  bool hasActiveNodes() const {
    return num_active_nodes.load(std::memory_order_relaxed) > 0;
  }

  uint64_t getNumActiveNodes() const {
    return num_active_nodes.load(std::memory_order_relaxed);
  }

  void shutdown() {
    for (auto &w : threads) {
      w.exit();
    }
  }

  [[nodiscard]] ExecInfo get_statistics() const { return stat_info.get(); }

  [[nodiscard]] uint64_t getIOStatistic() { return wl.getIOStatistic(); }

 private:
  template <typename F>
  void _foreachVertex(worker_id_t wid, F &&f) {
    vid_t id_bound = g->id_bound();
    vid_t batch_sz = id_bound / num_workers + 1;
    vid_t op = wid * batch_sz;
    vid_t ed = std::min((wid + 1) * batch_sz, id_bound);
    _foreachNormalVertex(op, ed, f);

    vid_t n_nodes = g->node_num();
    batch_sz = (n_nodes - id_bound) / num_workers + 1;
    op = id_bound + wid * batch_sz;
    ed = std::min(id_bound + (wid + 1) * batch_sz, n_nodes);
    _foreachBoundVertex(wid, op, ed, f);
  }

  template <typename F>
  void _foreachNormalVertex(vid_t op, vid_t ed, F &&f) {
    std::vector<PV> buffer;
    buffer.reserve(4096);
    uint64_t cnt = 0;
    for (vid_t v = op; v < ed; ++v) {
      if (g->is_virtual(v)) continue;
      Priority p = f(v);
      if (p > Priority{0}) {
        buffer.emplace_back(std::min(p, MAX_PRIORITY - 1), v);
        cnt++;
      }
      if (buffer.size() >= 4096) {
        wl.submit(buffer);
        buffer.clear();
      }
    }
    if (!buffer.empty()) {
      wl.submit(buffer);
    }
    num_active_nodes.fetch_add(cnt, std::memory_order_relaxed);
  }

  template <typename F>
  void _foreachBoundVertex(worker_id_t wid, vid_t op, vid_t ed, F &&f) {
    uint64_t cnt = 0;
    for (vid_t v = op; v < ed; ++v) {
      if (g->is_virtual(v)) continue;
      Priority p = f(v);
      if (p > Priority{0}) {
        cnt++;
        local_buckets[wid].emplace_back(v);
      }
    }
    num_active_nodes.fetch_add(cnt, std::memory_order_relaxed);
  }

  template <typename F>
  void wait_until(F &&done) {
    while (!done()) std::this_thread::yield();
  }

  worker_id_t num_workers;
  graph_t *g;
  SQWorklist<order, BID, graph_t> wl;
  std::vector<thread_wrapper_t> threads;
  StatInfo stat_info;
  std::vector<std::vector<vid_t>> local_buckets;
  std::atomic_uint64_t num_active_nodes{0};
};
}  // namespace acg
#endif  // ACG_SCHEDULER_HPP
