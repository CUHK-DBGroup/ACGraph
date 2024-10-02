#ifndef ACG_EXECUTOR_HPP
#define ACG_EXECUTOR_HPP
#pragma once

#include <atomic>
#include <cassert>
#include <ranges>
#include <span>
#include <thread>
#include <vector>

#include "util.hpp"
namespace acg {
template <typename F, typename WL, size_t BLOCK_SIZE = 4096>
struct SQExecutor {
 private:
  static constexpr size_t BUF_BLOCK_NUM = 16;
  static constexpr size_t BUF_SIZE = BLOCK_SIZE * BUF_BLOCK_NUM;

  using graph_t = WL::graph_t;
  using vid_t = WL::vid_t;
  using PV = WL::PV;
  using Frontiers = WL::Frontiers;
  F &f;
  WL &wl;
  graph_t &g;

  std::vector<PV> labeled;
  std::vector<vid_t> bounded;
  StatInfo stat_info;

  void push(Priority p, vid_t v) {
    if (g.is_bound(v)) {
      bounded.push_back(v);
    } else
      labeled.emplace_back(std::min(p, MAX_PRIORITY - 1), v);
  }

  void submit() {
    std::vector<vid_t> new_bounded;

    while (!bounded.empty()) {
      stat_info.hit_vertices(bounded.size());
      for (vid_t v : bounded) {
        auto val = f.apply(v);
        g.apply_neighbors(v, [&](vid_t dst) {
          auto p = f.process(val, dst);
          if (p > Priority{0}) {
            if (g.is_bound(dst)) {
              new_bounded.push_back(dst);
            } else {
              labeled.emplace_back(std::min(p, MAX_PRIORITY - 1), dst);
            }
          }
        });
      }
      std::swap(bounded, new_bounded);
      new_bounded.clear();
    }
    if (!labeled.empty()) {
      wl.submit(labeled);
      labeled.clear();
    }
  }

  void submitWhenFull() {
    if (bounded.size() + labeled.size() > 4096) submit();
  }

  void update_neighbors(F::msg_t &val, vid_t dst) {
    auto prio = f.process(val, dst);
    if (prio > Priority{0}) {
      push(prio, dst);
    }
  }

 public:
  SQExecutor(WL &s, F &f) : f(f), wl(s), g(*wl.get_graph()) {}

  SQExecutor &init(std::span<const PV> init_list) {
    for (auto &[p, v] : init_list) {
      push(p, v);
    }
    submit();
    return *this;
  }

  SQExecutor &init(vid_t v, Priority p) {
    push(p, v);
    submit();
    return *this;
  }

  SQExecutor &init(std::vector<vid_t> &init_list) {
    std::swap(bounded, init_list);
    submit();
    return *this;
  }

  void execute() {
    auto *buf = (char *)std::aligned_alloc(BLOCK_SIZE, BUF_SIZE);

    while (true) {
      auto tasks = wl.pullBatch();
      if (tasks.empty()) break;
      for (auto &[bid, data, frontiers] : tasks) {
        process(frontiers, data, buf);
      }
      submit();
      wl.finishBatch(tasks | std::views::transform([](auto &task) { return task.bid; }));
    }
    free(buf);
  }

  void process(Frontiers &frontiers, const char *data, char *buf) {
    stat_info.hit_tasks();

    frontiers.for_each([&](vid_t v) {
      stat_info.hit_vertices();

      auto val = f.apply(v);
      uint32_t cnt = 0;

      if (g.is_oversize(v)) {
        auto bop = g.v2bid_op(v);
        auto n = g.deg(v) * sizeof(vid_t) / BLOCK_SIZE;
        for (size_t i = 0; i < n; i += BUF_BLOCK_NUM) {
          size_t bnum = std::min(n - i, BUF_BLOCK_NUM);
          ssize_t bsize = bnum * BLOCK_SIZE;
          ssize_t read_size = g.read_blocks(buf, bop + i, bnum);
          assert(bsize == read_size);
          g.apply_neighbors(buf, buf + bsize, [&](vid_t dst) {
            update_neighbors(val, dst);
            cnt++;
          });
          submitWhenFull();
        }
      }
      g.apply_neighbors(v, data, [&](vid_t dst) {
        update_neighbors(val, dst);
        cnt++;
      });
      assert(cnt == g.deg(v));
      f.finally(v);
      stat_info.hit_edges(cnt);
      submitWhenFull();
    });
  }

  std::tuple<size_t, size_t, size_t> getStatistics() const { return stat_info.get(); }
};
}  // namespace acg
#endif  // ACG_EXECUTOR_HPP