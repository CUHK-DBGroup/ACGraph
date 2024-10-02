#include <cstring>
#include <filesystem>
#include <string>
#include <optional>

#include "log.h"

struct Config {
  std::filesystem::path graph_path;
  bool output = false;
  size_t num_threads = 32;
  size_t num_caches = 10240;
  size_t num_queues = 64;

  uint32_t src = 0;
  float alpha = 0.15;
  float rmax = 1e-7;
  int k = 10;
  int seed = 0;

  std::optional<std::string> gt_path;
  std::optional<std::filesystem::path> in_path;

};

Config parse_parameters(int argc, char *argv[]) {
  Config config{};
  config.graph_path = argv[1];
  constexpr char help[] =
      "format <data_path> [options]\n"
      "options:\n"
      "  -tn    <thread_num>       (default: 32)\n"
      "  -cn    <cache_num>        (default: 4096)\n"
      "  -cmb   <cache_num>        (optinal)\n"
      "  -cp    <fifo|lru>         (default: fifo)\n"
      "  -src   <src_node>         (default: 1)\n"
      "  -alpha <alpha>            (default: 0.15)\n"
      "  -rmax  <rmax>             (default: 1e-7)\n"
      "  -k     <k>                (default: 10)\n"
      "  -seed  <seed>             (default: 0)\n"
      "  -gt    <gt_path>          (optional)\n"
      "  -in    <ingraph_path>     (optional)\n"
      "  -output";
  for (int i = 2; i < argc; ++i) {
    if (strcmp(argv[i], "-tn") == 0) {
      config.num_threads = std::stoull(argv[++i]);
    } else if (strcmp(argv[i], "-cn") == 0) {
      config.num_caches = std::stoi(argv[++i]);
    } else if (strcmp(argv[i], "-output") == 0) {
      config.output = true;
    } else if (strcmp(argv[i], "-src") == 0) {
      config.src = std::stoull(argv[++i]);
    } else if (strcmp(argv[i], "-alpha") == 0) {
      config.alpha = std::stof(argv[++i]);
    } else if (strcmp(argv[i], "-rmax") == 0) {
      config.rmax = std::stof(argv[++i]);
    } else if (strcmp(argv[i], "-k") == 0) {
      config.k = std::stoi(argv[++i]);
    } else if (strcmp(argv[i], "-cmb") == 0) {
      config.num_caches = std::stoi(argv[++i]) * 256;
    } else if (strcmp(argv[i], "-gt") == 0) {
      config.gt_path = argv[++i];
    } else if (strcmp(argv[i], "-in") == 0) {
      config.in_path = argv[++i];
    } else {
      log_fatal("unknown option %s\nusage:\n%s", argv[i], help);
      exit(1);
    }
  }

  log_report("graph_path: %s", config.graph_path.c_str());
  log_report("num_threads: %zu", config.num_threads);
  log_report("num_caches: %zu", config.num_caches);
  log_report("params: src=%u, alpha=%.2f, rmax=%.2e, k=%d",
             config.src, config.alpha, config.rmax, config.k);
  return config;
}