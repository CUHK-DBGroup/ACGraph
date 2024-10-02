#include <iostream>

#include "config.hpp"
#include "graph.hpp"
#include "scheduler.hpp"
#include "timer.hpp"

using namespace acg;
using vid_t = uint32_t;
using eid_t = uint64_t;
using bid_t = uint32_t;
using graph_t = Graph<vid_t, eid_t, bid_t>;
using BufPool = BufferPool<bid_t>;
using scheduler_t = ASQScheduler<PriorityOrder::INCREASING, bid_t, graph_t>;

struct ExampleTask {
  explicit ExampleTask(const Config &cfg) {
    log_report("build_time: %s", getCurrentTime().c_str());
    graph = new graph_t(cfg.graph_path);
    buf_pool = new BufPool(cfg.num_caches);
    scheduler = new scheduler_t(graph, buf_pool, cfg.num_threads);
    assert(buf_pool != nullptr);
    assert(scheduler != nullptr);
  }

  ~ExampleTask() {
    delete graph;
    delete scheduler;
    delete buf_pool;
  }

  template <typename F, typename... Arg>
  std::invoke_result_t<F, graph_t&, scheduler_t&, Arg...> run(F &&task, Arg... args) {
    Timer timer(TIMER::CALC);
    return task(*graph, *scheduler, args...);
  }

  void report() const {
    auto [n_tasks, n_vertices, n_edges] = scheduler->get_statistics();
    log_report("n_ptasks: %llu", ull(n_tasks));
    log_report("n_pvertices: %llu", ull(n_vertices));
    log_report("n_pedges: %llu", ull(n_edges));

    log_report("total_time: %.3lfms, graph_init: %.3lfms, calc: %.3lfms",
               Timer::used(TIMER::ALL) / 1e3, Timer::used(TIMER::GRAPH_INIT) / 1e3,
               Timer::used(TIMER::CALC) / 1e3);
    uint64_t io_statistic = scheduler->getIOStatistic() + graph->stat_info();
    log_report("IO_read: %lluB", io_statistic);
  }

  graph_t *graph = nullptr;
  BufPool *buf_pool = nullptr;
  scheduler_t *scheduler = nullptr;
};


std::vector<std::pair<uint32_t, double>> topk(uint32_t n, std::atomic<float> *a, size_t k) {
  std::vector<double> vec(n);
  for (uint32_t i = 0; i < n; ++i) {  vec[i] = a[i].load(std::memory_order_relaxed); }
  std::nth_element(vec.begin(), vec.begin() + k, vec.end(), std::greater<double>());
  double threshold = vec[k];
  std::vector<std::pair<uint32_t, double>> ret;
  ret.reserve(k);
  for (uint32_t i = 0; i < n; ++i) {
    auto val = a[i].load(std::memory_order_relaxed);
    if (val >= threshold) {
      ret.emplace_back(i, val);
    }
  }
  std::sort(ret.begin(), ret.end(),
            [](const auto &a, const auto &b) { return a.second > b.second; });
  return ret;
}


 void report(scheduler_t &scheduler, graph_t &graph) {
    auto [n_tasks, n_vertices, n_edges] = scheduler.get_statistics();
    log_report("n_ptasks: %llu", ull(n_tasks));
    log_report("n_pvertices: %llu", ull(n_vertices));
    log_report("n_pedges: %llu", ull(n_edges));

    log_report("total_time: %.3lfms, graph_init: %.3lfms, calc: %.3lfms",
               Timer::used(TIMER::ALL) / 1e3, Timer::used(TIMER::GRAPH_INIT) / 1e3,
               Timer::used(TIMER::CALC) / 1e3);
    uint64_t io_statistic = scheduler.getIOStatistic() + graph.stat_info();
    log_report("IO_read: %lluB", io_statistic);
  }
