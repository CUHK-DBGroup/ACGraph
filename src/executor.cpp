#include "executor.hpp"

namespace acg {
PrioT BQExecutor::execute(PrioT prio) {
  //    std::vector<labeled_vt> local_queue;
  //    std::queue<vid_t> bound_v;
  local_queue.reserve(4096);
  bound_v.reserve(4096);
  auto *buf = (char *)std::aligned_alloc(BLOCK_SIZE, BUF_SIZE);

  while (true) {
    auto task = wl.pull_task();
    if (!task) break;
    auto &[tid, frontiers, data] = task.value();
    local_queue.clear();
    bound_v.clear();

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
          if (local_queue.size() + bound_v.size() > 4096) submit();
        }
        //          ssize_t bsize = n*BLOCK_SIZE;
        //          auto *buf = (char *)std::aligned_alloc(BLOCK_SIZE, bsize);
        //          assert(bsize == g.read_blocks(buf, bop, n));
        //          g.apply_neighbors(buf, buf+bsize, [&](vid_t dst) {
        //            update_neighbors(val, dst);
        //            cnt++;
        //          });
      }
      g.apply_neighbors(v, data, [&](vid_t dst) {
        update_neighbors(val, dst);
        cnt++;
      });
      assert(cnt == g.deg(v));
      f.finally(v);
      stat_info.hit_edges(cnt);
      if (local_queue.size() + bound_v.size() > 4096) submit();
    });
    submit();

    wl.finish_task(tid);
  }
  free(buf);
}
}  // namespace acg