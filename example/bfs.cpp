#include <omp.h>

#include "example.hpp"

struct BFS_F {
  using msg_t = int;

  std::atomic<msg_t> *dis;

  template <typename G>
  explicit BFS_F(G &g) : dis(new std::atomic<msg_t>[g.node_num()]) {
    Timer timer(TIMER::INIT);
    if constexpr (std::atomic<msg_t>::is_always_lock_free) {
      memset(dis, 0xff, sizeof(std::atomic<msg_t>) * g.node_num());
      std::atomic_thread_fence(std::memory_order_release);
    } else {
      for (size_t i = 0; i < g.node_num(); ++i) {
        dis[i].store(-1, std::memory_order_relaxed);
      }
    }
  }

  BFS_F(BFS_F &&other) : dis(nullptr) { swap(other); }
  BFS_F(const BFS_F &) = delete;

  BFS_F &operator=(BFS_F other) {
    swap(other);
    return *this;
  }

  void swap(BFS_F &other) { std::swap(dis, other.dis); }

  ~BFS_F() { delete[] dis; }

  [[nodiscard]] int apply(vid_t v) const { return dis[v].load(std::memory_order_acquire); }

  [[nodiscard]] Priority process(int val, vid_t dst) {
    while (true) {
      auto d = dis[dst].load(std::memory_order_acquire);
      if (d < 0 || d > val + 1) {
        if (dis[dst].compare_exchange_weak(d, val + 1, std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
          return val + 1;
        }
      } else
        break;
    }

    return 0;
  }

  void finally(vid_t) const {}
};

BFS_F bfs(graph_t &graph, scheduler_t &scheduler, vid_t s) {
  log_report("init_time: %s", getCurrentTime().c_str());
  BFS_F bfs_f(graph);
  bfs_f.dis[s].store(0, std::memory_order_relaxed);

  log_report("iter_time: %s", getCurrentTime().c_str());
  scheduler.async_run(bfs_f, s, 1);
  return bfs_f;
}

std::tuple<uint32_t, int32_t> bfs_ans(graph_t &graph, BFS_F &bfs_f) {
  uint32_t c = 0;
  int32_t max_dis = 0;
  for (vid_t i = 0; i < graph.node_num(); ++i) {
    if (bfs_f.dis[i].load() != -1) {
      c++;
      max_dis = std::max(max_dis, bfs_f.dis[i].load());
    }
  }
  return std::make_tuple(c, max_dis);
}

int main(int argc, char *argv[]) {
  log_report("start_time: %s", getCurrentTime().c_str());
  Config cfg = parse_parameters(argc, argv);
  log_report("src: %u", cfg.src);
  ExampleTask et(cfg);
  vid_t src = cfg.src;
  BFS_F bfs_f = et.run(bfs, src);
  log_report("done_time: %s", getCurrentTime().c_str());
  et.report();

  auto &graph = *et.graph;
  auto [num_visited, max_dis] = bfs_ans(graph, bfs_f);
  log_info("init time: %lfms, total time: %lfms", Timer::used(TIMER::INIT) / 1e3,
           Timer::used(TIMER::ALL) / 1e3);
  log_report("ans: (%u, %d)", num_visited, max_dis);
  return 0;
}
