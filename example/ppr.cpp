#include "example.hpp"

struct PPR_F {
  using msg_t = float;

  std::vector<std::atomic<float>> rsd;
  std::vector<float> rsv;
  graph_t *g;
  float alpha = 0.15;
  float rmax = 1e-7;

  explicit PPR_F(graph_t *g, float alpha, float rmax)
      : g(g), rsd(g->node_num()), rsv(g->node_num()), alpha(alpha), rmax(rmax) {}

  msg_t apply(vid_t v) {
    auto t = rsd[v].load(std::memory_order_acquire);
    while (!rsd[v].compare_exchange_weak(t, 0.0, std::memory_order_acq_rel,
                                         std::memory_order_relaxed)) {
      t = rsd[v].load(std::memory_order_acquire);
    }
    rsv[v] += alpha * t;

    return (1 - alpha) * t / float(g->deg(v));
  }

  [[nodiscard]] Priority process(msg_t val, vid_t dst) {
    auto t = rsd[dst].fetch_add(val, std::memory_order_acq_rel);

    auto threshold = rmax * g->deg(dst);
    if (g->deg(dst) == 0) {
      apply(dst);
      return 0;
    }
    if (t < threshold && t + val >= threshold) {
      return 1;
    }
    return 0;
  }

  void finally(vid_t) const {}
};

std::vector<std::pair<vid_t, float>> topk(const std::vector<float> &rsv, size_t k) {
  std::vector<float> vec(rsv);
  std::nth_element(vec.begin(), vec.begin() + k, vec.end(), std::greater<float>());
  float threshold = vec[k];
  std::vector<std::pair<vid_t, float>> ret;
  ret.reserve(k);
  for (size_t i = 0; i < rsv.size(); ++i) {
    if (rsv[i] >= threshold) {
      ret.emplace_back(i, rsv[i]);
    }
  }
  std::sort(ret.begin(), ret.end(),
            [](const auto &a, const auto &b) { return a.second > b.second; });
  return ret;
}

PPR_F run(graph_t &graph, scheduler_t &scheduler, vid_t src, float alpha, float rmax) {
  log_report("init_time: %s", getCurrentTime().c_str());

  PPR_F ppr(&graph, alpha, rmax);
  ppr.rsv.assign(graph.node_num(), 0);
  ppr.rsd = std::vector<std::atomic<float>>(graph.node_num());
  ppr.rsd[src].store(1);

  log_report("iter_time: %s", getCurrentTime().c_str());
  scheduler.async_run(ppr, src, 1);
  return ppr;
}

int main(int argc, char *argv[]) {
  log_report("start_time: %s", getCurrentTime().c_str());
  Config cfg = parse_parameters(argc, argv);
  log_report("src: %u", cfg.src);
  log_report("alpha: %f", cfg.alpha);
  log_report("rmax: %f", cfg.rmax);
  ExampleTask et(cfg);
  auto ppr = et.run(run, cfg.src, cfg.alpha, cfg.rmax);
  log_report("done_time: %s", getCurrentTime().c_str());
  et.report();

  log_report("ans: -");
  // auto &g = *ppr.g;
  // auto topk_rsv = topk(ppr.rsv, 20);
  // auto id2v = g.load_id2v();
  // for (auto [v, rsv] : topk_rsv) {
  //   std::cout << id2v[v] << ": " << rsv << std::endl;
  // }

  return 0;
}
