#include "util.hpp"

namespace acg {
std::string_view trim(std::string_view s) {
  auto op = s.find_first_not_of(" \t\n\r");
  if (op == std::string::npos) return "";
  auto ed = s.find_last_not_of(" \t\n\r");
  return s.substr(op, ed - op + 1);
}

using ExecInfo = std::tuple<uint64_t, uint64_t, uint64_t>;

void StatInfo::hit_tasks(uint64_t num) { n_tasks.fetch_add(num, std::memory_order_relaxed); }
void StatInfo::hit_vertices(uint64_t num) { n_vertices.fetch_add(num, std::memory_order_relaxed); }
void StatInfo::hit_edges(uint64_t num) { n_edges.fetch_add(num, std::memory_order_relaxed); }
void StatInfo::hit(uint64_t n_tasks, uint64_t n_vertices, uint64_t n_edges) {
  hit_tasks(n_tasks);
  hit_vertices(n_vertices);
  hit_edges(n_edges);
}
ExecInfo StatInfo::get() const {
  return std::make_tuple(n_tasks.load(std::memory_order_relaxed),
                         n_vertices.load(std::memory_order_relaxed),
                         n_edges.load(std::memory_order_relaxed));
}

std::string getCurrentTime() {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()) % 1000000;
  std::stringstream ss;
  ss << std::put_time(std::localtime(&time), "%Y-%m-%d-%H:%M:%S") << "." << std::setfill('0')
     << std::setw(6) << ms.count();
  return ss.str();
}

}  // namespace acg
