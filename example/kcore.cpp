#include <omp.h>

#include "example.hpp"

struct KCORE_F {
  using msg_t = int;

  uint32_t k;
  std::vector<std::atomic<uint32_t>> degs;

  template <typename G>
  explicit KCORE_F(G &g, uint32_t k) : k(k), degs(g.node_num()) {
    Timer timer(TIMER::INIT);
  }

  [[nodiscard]] int apply(vid_t v) const { return int(v); }

  [[nodiscard]] Priority process(int, vid_t dst) {
    while (true) {
      auto d = degs[dst].load(std::memory_order_acquire);
      if (degs[dst].compare_exchange_weak(d, d - 1, std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
        assert(d > 0);
        if (d == k) {
          return 1;
        } else
          return 0;
      }
    }
  }

  void finally(vid_t) const {}
};

KCORE_F kcore(graph_t &graph, scheduler_t &scheduler, uint32_t k) {
  log_report("init_time: %s", getCurrentTime().c_str());

  KCORE_F kcore_f(graph, k);
  scheduler.foreachVertex([&](vid_t v) {
    kcore_f.degs[v] = graph.deg(v);
    if (kcore_f.degs[v] < k) {
      return Priority(1);
    } else {
      return Priority(0);
    }
  });

  log_report("iter_time: %s", getCurrentTime().c_str());
  scheduler.async_run(kcore_f);
  return kcore_f;
}

int main(int argc, char *argv[]) {
  log_report("start_time: %s", getCurrentTime().c_str());
  Config cfg = parse_parameters(argc, argv);
  log_report("k: %u", cfg.k);
  ExampleTask et(cfg);
  auto kcore_f = et.run(kcore, cfg.k);
  log_report("done_time: %s", getCurrentTime().c_str());

  et.report();
  auto &graph = *et.graph;

  uint32_t c = 0;
  for (vid_t i = 0; i < graph.node_num(); ++i) {
    if (kcore_f.degs[i] >= cfg.k) c++;
  }
  log_report("ans: %u", c);
  return 0;
}
