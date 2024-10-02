#include <omp.h>

#include "example.hpp"

struct WCC_F {
  using msg_t = int;

  std::vector<std::atomic<int>> label;

  template <typename G>
  explicit WCC_F(G &g) : label(g.node_num()) {
  }

  [[nodiscard]] int apply(vid_t v) const { return label[v].load(); }

  [[nodiscard]] Priority process(int val, vid_t dst) {
    while (true) {
      auto d = label[dst].load(std::memory_order_relaxed);
      if (d > val) {
        if (label[dst].compare_exchange_weak(d, val, std::memory_order_acq_rel,
                                             std::memory_order_relaxed)) {
          return (val >> 4) + 1;
        }
      } else
        break;
    }

    return 0;
  }

  void finally(vid_t) const {}
};

WCC_F wcc(graph_t &graph, scheduler_t &scheduler) {
  log_report("init_time: %s", getCurrentTime().c_str());

  WCC_F wcc_f(graph);
  // std::vector<scheduler_t::PV> init_list;
  // init_list.reserve(graph.rnode_num());
  // for (vid_t v = 0; v < graph.node_num(); ++v) {
  //   if (!graph.is_virtual(v)) init_list.emplace_back((v >> 4) + 1, v);
  // }
  scheduler.foreachVertex([&](vid_t v) {
    wcc_f.label[v].store(v, std::memory_order_relaxed);
    return Priority(v >> 4) + 1;
  });
  log_report("iter_time: %s", getCurrentTime().c_str());
  scheduler.async_run(wcc_f);
  return wcc_f;
}

int main(int argc, char *argv[]) {
  log_report("start_time: %s", getCurrentTime().c_str());
  Config cfg = parse_parameters(argc, argv);
  ExampleTask et(cfg);

  auto wcc_f = et.run(wcc);
  log_report("done_time: %s", getCurrentTime().c_str());

  et.report();
  auto &graph = *et.graph;

  std::vector<uint32_t> wcc(graph.node_num());
  uint32_t wcc_num = 0;
  uint32_t max_wcc = 0;
  for (vid_t i = 0; i < graph.node_num(); ++i) {
    if (graph.is_virtual(i)) continue;
    auto label = wcc_f.label[i].load(std::memory_order_relaxed);
    wcc[label]++;
  }
  for (auto size : wcc) {
    if (size > max_wcc) {
      max_wcc = size;
    }
    if (size > 0) {
      wcc_num++;
    }
  }
  log_report("ans: (%u, %u)", wcc_num, max_wcc);

  return 0;
}
