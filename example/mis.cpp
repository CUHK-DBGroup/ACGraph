#include <omp.h>
#include <random>

#include "example.hpp"

struct MIS_DEACTIVE_F {
  using msg_t = uint32_t;
  const uint32_t *priorities;
  std::atomic_bool *active;

  explicit MIS_DEACTIVE_F(const uint32_t *priorities, std::atomic_bool *active) : priorities(priorities), active(active) { }

  [[nodiscard]] msg_t apply(vid_t v) const {
    return priorities[v];
  }

  [[nodiscard]] Priority process(const msg_t &msg, vid_t dst) {
    if (msg < priorities[dst]) {
      active[dst].store(false, std::memory_order_relaxed);
    }
    return 0;
  }

  void finally(vid_t) const {}
};

struct MIS_DELIVE_F {
  struct msg_t { };
  std::atomic_bool *live;

  explicit MIS_DELIVE_F(std::atomic_bool *live) : live(live) { }

  [[nodiscard]] msg_t apply(vid_t v) const { return {}; }

  [[nodiscard]] Priority process(const msg_t &, vid_t dst) {
    live[dst].store(false, std::memory_order_relaxed);
    return 0;
  }

  void finally(vid_t) const {}
};

std::vector<uint32_t> random_permutation(uint32_t n, int seed) {
  std::vector<uint32_t> perm(n);
  std::iota(perm.begin(), perm.end(), 1);
  std::mt19937 rng(seed);
  std::shuffle(perm.begin(), perm.end(), rng);
  return perm;
}

std::vector<uint32_t> mis_algo(graph_t &graph, scheduler_t &scheduler, int seed) {
  log_report("init_time: %s", getCurrentTime().c_str());
  auto *timer = new Timer(TIMER::CALC);
  auto temp_priority = random_permutation(graph.rnode_num(), seed);
  auto id2v = graph.load_id2v();
  std::vector<uint32_t> priorities(graph.node_num());
  std::atomic_bool *active = new std::atomic_bool[graph.node_num()];
  std::atomic_bool *live = new std::atomic_bool[graph.node_num()];
  scheduler.foreachVertex([&](vid_t v) {
    priorities[v] = temp_priority[id2v[v]];
    active[v].store(true, std::memory_order_relaxed);
    live[v].store(true, std::memory_order_relaxed);
    return 1;
  });
  log_report("iter_time: %s", getCurrentTime().c_str());
  temp_priority = {};
  std::vector<bool> is_mis(graph.node_num(), false);
  uint32_t iter_num = 0;
  while (scheduler.hasActiveNodes()) {
    log_report("round_%u_time: %s", iter_num++, getCurrentTime().c_str());
    log_report("frontier_size_%u: %llu", iter_num, (unsigned long long)scheduler.getNumActiveNodes());

    MIS_DEACTIVE_F mis_deactive_f(priorities.data(), active);
    scheduler.async_run(mis_deactive_f);
    scheduler.foreachVertex([&](vid_t v) {
      if (!active[v].load(std::memory_order_relaxed)) return 0;
      if (!live[v].load(std::memory_order_relaxed)) return 0;
      is_mis[v] = true;
      live[v].store(false, std::memory_order_relaxed);
      return 1;
    });
    MIS_DELIVE_F mis_delive_f(live);
    log_report("frontier_size_%u: %llu", iter_num, (unsigned long long)scheduler.getNumActiveNodes());
    scheduler.async_run(mis_delive_f);
    scheduler.foreachVertex([&](vid_t v) {
      if (!live[v].load(std::memory_order_relaxed)) return 0;
      active[v].store(true, std::memory_order_relaxed);
      return 1;
    });
  }
  log_report("done_time: %s", getCurrentTime().c_str());
  delete timer;
  std::vector<uint32_t> mis;
  for (vid_t id = 0; id < graph.node_num(); ++id) {
    if (is_mis[id]) { mis.push_back(id2v[id]); }
  }
  std::sort(mis.begin(), mis.end());
  delete[] active;
  delete[] live;
  return mis;
}


int main(int argc, char *argv[]) {
  log_report("start_time: %s", getCurrentTime().c_str());
  Config cfg = parse_parameters(argc, argv);
  log_report("build_time: %s", getCurrentTime().c_str());
  graph_t *graph = new graph_t(cfg.graph_path);
  BufPool buf_pool(cfg.num_caches);
  scheduler_t *scheduler = new scheduler_t(graph, &buf_pool, cfg.num_threads);

  auto mis = mis_algo(*graph, *scheduler, cfg.seed);


  report(*scheduler, *graph);

  log_info("#MIS: %llu", (unsigned long long)mis.size());
  for (auto u : std::views::take(mis, 10)) {
    printf("%u ", u);
  }
  printf("\n");
  log_report("ans: -");

  delete scheduler;
  delete graph;
  return 0;
}
