#include <iostream>

#include "bq_scheduler.hpp"
#include "config.hpp"
#include "graph.hpp"
#include "timer.hpp"

using namespace acg;
using vid_t = uint32_t;
using eid_t = uint64_t;
using bid_t = uint32_t;
using graph_t = Graph<vid_t, eid_t, bid_t>;
using BufPool = BufferPool<bid_t>;
constexpr size_t JOB_NUM = 2;
using scheduler_t = ABQScheduler<PriorityOrder::INCREASING, bid_t, graph_t, JOB_NUM>;

struct ExampleTask {
  explicit ExampleTask(const Config &cfg) : graph(cfg.graph_path) {
    buffer_pool = new BufPool(cfg.buf_pool_size);
    scheduler = new scheduler_t(&graph, buffer_pool, cfg.num_threads);
    assert(buffer_pool != nullptr);
    assert(scheduler != nullptr);
  }

  ~ExampleTask() {
    delete scheduler;
    delete buffer_pool;
  }

  template <typename F, typename... Arg>
  std::invoke_result_t<F, graph_t &, scheduler_t &, Arg...> run(F &&task, Arg... args) {
    Timer timer(TIMER::CALC);
    return task(graph, *scheduler, args...);
  }

  void report() const {
    auto [n_tasks, n_vertices, n_edges] = scheduler->get_statistics();
    log_report("execed_tasks: %llu, proced_vertices: %llu, proced_edges: %llu", ull(n_tasks),
               ull(n_vertices), ull(n_edges));
    log_report("total_time: %.3lfms, graph_init: %.3lfms, calc: %.3lfms",
               Timer::used(TIMER::ALL) / 1e3, Timer::used(TIMER::GRAPH_INIT) / 1e3,
               Timer::used(TIMER::CALC) / 1e3);
  }

  graph_t graph;
  BufPool *buffer_pool = nullptr;
  scheduler_t *scheduler = nullptr;
};

std::tuple<uint32_t, int32_t> bfs_ans(graph_t &graph, std::atomic<int> *dis) {
  uint32_t c = 0;
  int32_t max_dis = 0;
  for (vid_t i = 0; i < graph.node_num(); ++i) {
    auto d = dis[i].load(std::memory_order_relaxed);
    if (d != graph.node_num()) {
      c++;
      max_dis = std::max(max_dis, d);
    }
  }
  return std::make_tuple(c, max_dis);
}

struct BFS_F {
  using msg_t = int;

  graph_t &g;
  vid_t src_onde;
  std::atomic<msg_t> *dis;

  // template <typename G>
  explicit BFS_F(graph_t &g, vid_t src_onde) : g(g), src_onde(src_onde), dis(nullptr) {
    // dis = new std::atomic<msg_t>[g.node_num()];
    // for (vid_t i = 0; i < g.node_num(); ++i) {
    //   dis[i].store(g.node_num());
    // }
    // memset(dis, 0xff, g.node_num() * sizeof(std::atomic<msg_t>));
    // dis[src_onde].store(0, std::memory_order_relaxed);
  }

  void init() {
    dis = new std::atomic<msg_t>[g.node_num()];
    // for (vid_t i = 0; i < g.node_num(); ++i) {
    //   dis[i].store(g.node_num());
    // }
    memset(dis, 0xff, g.node_num() * sizeof(std::atomic<msg_t>));
    dis[src_onde].store(0, std::memory_order_relaxed);
    // dis = new std::atomic<msg_t>[g.node_num()];
    // for (vid_t i = 0; i < g.node_num(); ++i) {
    //   dis[i].store(g.node_num());
    // }
    // dis[src_onde].store(0, std::memory_order_relaxed);
  }

  void finish() {
    // auto [num_visited, max_dis] = bfs_ans(g, dis);
    // log_info("bfs from %d visited %u (max dis: %d)", src_onde, num_visited, max_dis);
    delete[] dis;
    dis = nullptr;
  }

  [[nodiscard]] int apply(vid_t v) const { return dis[v].load(std::memory_order_acquire); }

  [[nodiscard]] Priority process(int val, vid_t dst) {
    while (true) {
      auto d = dis[dst].load();
      if (d < 0 || d > val + 1) {
        if (dis[dst].compare_exchange_weak(d, val + 1)) {
          return val + 1;
        }
      } else
        break;
    }

    return 0;
  }

  void finally(vid_t) const {}
};

void batchBFS(graph_t &graph, scheduler_t &scheduler, std::vector<vid_t> srcs) {
  std::vector<BFS_F *> bfs_fs(srcs.size());
  std::vector<std::tuple<vid_t, Priority>> init_list(srcs.size());
  for (uint32_t i = 0; i < srcs.size(); ++i) {
    bfs_fs[i] = new BFS_F(graph, srcs[i]);
    init_list[i] = std::make_tuple(srcs[i], Priority{1});
  }
  log_report("init_done: %s", getCurrentTime().c_str());
  scheduler.async_run(bfs_fs, init_list);
}

int main(int argc, char *argv[]) {
  log_report("start_time: %s", getCurrentTime().c_str());
  Config cfg = parse_parameters(argc, argv);
  ExampleTask et(cfg);
  log_report("et_done: %s", getCurrentTime().c_str());
  // auto [bfs_f1, bfs_f2] = et.run(bfs, 10345, 2);
  // auto bfs_fs = et.run(batchBFS<JOB_NUM>, std::to_array<vid_t>({10345, 2, 456789}));
  et.run(batchBFS, std::vector<vid_t>{10345, 2, 456789, 234, 5439, 4280123});
  log_report("done_time: %s", getCurrentTime().c_str());
  et.report();

  log_info("init time: %lfms, total time: %lfms", Timer::used(TIMER::INIT) / 1e3,
           Timer::used(TIMER::ALL) / 1e3);
  log_report("end_time: %s", getCurrentTime().c_str());
  return 0;
}
