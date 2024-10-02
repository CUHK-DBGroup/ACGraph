#include <cmath>

#include "example.hpp"

using Value = double;
using PPR_Vec = std::vector<Value>;

struct PR_F {
  using msg_t = double;

  std::vector<std::atomic<double>> rsd;
  std::vector<double> rsv;
  graph_t *g;
  double alpha = 0.15;
  double rmax = 1e-7;

  explicit PR_F(graph_t *g, double alpha, double rmax)
      : g(g), rsd(g->node_num()), rsv(g->node_num()), alpha(alpha), rmax(rmax) {}

  msg_t apply(vid_t v) {
    auto t = rsd[v].exchange(0.0, std::memory_order_relaxed);
    rsv[v] += t * alpha;
    if (g->deg(v) == 0) { return 0.0f; }
    return (1 - alpha) * t / double(g->deg(v));
  }

  [[nodiscard]] Priority process(msg_t val, vid_t dst) {
    auto t = rsd[dst].fetch_add(val, std::memory_order_acq_rel);

    auto threshold = rmax;
    if (t < threshold && t + val >= threshold) {
      return 1;
    }
    return 0;
  }

  void finally(vid_t) const {}
};

std::vector<std::pair<vid_t, double>> topk(const std::vector<double> &rsv, size_t k) {
  std::vector<double> vec(rsv);
  std::nth_element(vec.begin(), vec.begin() + k, vec.end(), std::greater<double>());
  double threshold = vec[k];
  std::vector<std::pair<vid_t, double>> ret;
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

PR_F run(graph_t &graph, scheduler_t &scheduler, vid_t src, double alpha, double rmax) {
  log_report("init_time: %s", getCurrentTime().c_str());

  PR_F pr(&graph, alpha, rmax);
  pr.rsv.resize(graph.node_num(), 0);
  pr.rsd = std::vector<std::atomic<double>>(graph.node_num());
  double init_val = 1.0 / graph.rnode_num();
  scheduler.foreachVertex([&pr, init_val](vid_t v) { pr.rsd[v].store(init_val); return 1; });
  log_info("initial frontier size: %zu", scheduler.getNumActiveNodes());

  log_report("iter_time: %s", getCurrentTime().c_str());
  scheduler.async_run(pr);
  return pr;
}

double get_error(const PPR_Vec &rsv, const std::vector<vid_t> &ids, const PPR_Vec &gt) {
  double error = 0.0;
  for (size_t i = 0; i < gt.size(); ++i) {
    error += std::abs(rsv[ids[i]] - gt[i]);
  }
  return error;
}

int main(int argc, char *argv[]) {
  log_report("start_time: %s", getCurrentTime().c_str());
  Config cfg = parse_parameters(argc, argv);
  ExampleTask et(cfg);
  auto pr = et.run(run, cfg.src, cfg.alpha, cfg.rmax);
  log_report("done_time: %s", getCurrentTime().c_str());
  et.report();

  log_report("ans: -");

  if (cfg.gt_path) {
    double sum = 0.0f;
    std::for_each(pr.rsv.begin(), pr.rsv.end(), [&sum](double &val) { sum += val; });
    std::for_each(pr.rsv.begin(), pr.rsv.end(), [sum](double &val) { val /= sum; });
    auto gt = load_raw<double>(cfg.gt_path.value());
    auto ids = et.graph->load_v2id();
    std::cout << get_error(pr.rsv, ids, gt) << std::endl;
  }
  return 0;
}
