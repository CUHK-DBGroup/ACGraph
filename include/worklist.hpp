#ifndef ACG_WORKLIST_HPP
#define ACG_WORKLIST_HPP
#pragma once
#include <chrono>
#include <set>
#include <span>

#include "adaptive_set.hpp"
#include "atomic_ptr.hpp"
#include "block_info.hpp"
#include "bufferpool.hpp"
#include "graph.hpp"
#include "io_engine.hpp"
#include "mbq.hpp"
#include "types.hpp"
namespace acg {
using namespace std::chrono;

template <PriorityOrder order = INCREASING, typename BID = uint32_t, typename GT = Graph<> >
class SQWorklist {
 public:
  using graph_t = GT;
  using vid_t = graph_t::vid_t;
  struct PV {
    Priority p;
    vid_t v;
  };
  using BufPool = BufferPool<BID>;
  using BInfo = BlockInfo<vid_t, order, 1>;
  using Frontiers = typename BInfo::Frontiers;

 private:
  using CachedQueue = acg::mbq::MultiBucketQueue<Priority, BID>;
  using UncachedQueue = acg::mbq::MultiBucketQueue<Priority, BID>;

  static constexpr size_t UPGRADE_BATCH_SIZE = 64;

 public:
  struct Task {
    BID bid = 0;
    const char *block = nullptr;
    Frontiers vs;
  };

  SQWorklist(graph_t *graph, BufPool *buffer_pool, uint32_t num_threads)
      : graph(graph),
        buffer_pool(buffer_pool),
        cached_queue(num_threads * 2),
        uncached_queue(num_threads * 2),
        blocks(graph->block_num()),
        active_block_num(0) {
    log_info("sizeof(BInfo): %zuB with max range: [v_op, v_op+%zu), sizeof(blocks): %zuKB",
     sizeof(BInfo), Frontiers::MAX_RANGE, (sizeof(BInfo) * blocks.size() >> 10));
    auto binfo = load_raw<vid_t>(graph->binfo_path());
    for (BID bid = 0; bid < graph->block_num(); bid++) {
      blocks[bid].init(binfo[bid]);
    }
    fd = open(graph->edge_block_path().c_str(), O_RDONLY | O_DIRECT);
    assert(fd > 0);
    auto cache_num = buffer_pool->getCacheNum();
    io_engine.init(fd, cache_num / num_threads);
    // print_thread = std::jthread([this](std::stop_token st) { print(st); });
  }

  // std::jthread print_thread;

  // void print(std::stop_token st) {
  //   int t = 0;
  //   while (!st.stop_requested()) {
  //     std::cout << active_block_num.load(std::memory_order_relaxed) << ", " <<
  //     cached_queue.size()
  //               << ", " << uncached_queue.size() << ", " << buffer_pool->size() << std::endl;
  //     std::this_thread::sleep_for(100ms);
  //   }
  // }

  ~SQWorklist() { close(fd); }

  void init_thread() {
    cached_queue.initTID();
    uncached_queue.initTID();
    io_engine.initThread();
  }

  std::vector<Task> pullBatch() {
    while (!done()) {
      upgrade();
      auto [prio, bids] = cached_queue.popBatch();
      if (bids.empty()) {
        std::this_thread::yield();
        continue;
      }
      assert(prio == 1);

      std::vector<Task> ret;
      ret.reserve(bids.size());
      for (auto bid : bids) {
        BInfo &block = blocks[bid];
        auto *ptr = block.data.lock();
        assert(ptr != nullptr);
        char *data = static_cast<char *>(ptr);

        ret.emplace_back(bid, data, block.getFrontiers());
        block.clearFrontiers();
        block.resetPriority();
        block.data.unlock();
      }
      if (!ret.empty()) return ret;
    }
    return {};
  }

  template <std::ranges::input_range R>
    requires std::same_as<std::ranges::range_value_t<R>, BID>
  void finishBatch(R &&bids) {
    std::vector<BID> waiting;
    waiting.reserve(bids.size());
    std::vector<char *> no_needs;
    no_needs.reserve(bids.size());
    uint64_t inactive_num = 0;
    for (auto bid : bids) {
      auto &block = blocks[bid];
      auto *ptr = block.data.lock();
      if (!block.isActive()) {
        block.cached = false;
        // block.num_exec = 0;
        block.data.unlock_and_clear();
        no_needs.push_back(static_cast<char *>(ptr));
        inactive_num++; 
      // } else if (block.num_exec >= 3) {
      //   block.cached = false;
      //   block.num_exec = 0;
      //   block.data.unlock_and_clear();
      //   uncached_queue.push(block.prio, bid);
      //   no_needs.push_back(static_cast<char *>(ptr));
      } else {
        // block.num_exec++;
        block.data.unlock();
        waiting.push_back(bid);
      }
    }
    if (!waiting.empty()) {
      cached_queue.pushBatch(1, std::span(waiting.begin(), waiting.end()));
    }
    if (!no_needs.empty()) {
      buffer_pool->releaseBatch(std::span(no_needs.begin(), no_needs.end()));
    }
    active_block_num.fetch_sub(inactive_num, std::memory_order_acq_rel);
  }

  [[nodiscard]] bool done() const { return active_block_num.load(std::memory_order_acquire) == 0; }

  void submit(std::vector<PV> &buf) {
    std::sort(buf.begin(), buf.end(), [&](const auto &x, const auto &y) {
      if (x.v == y.v) {
        // put the item with higher prio in the front of the other
        return prioriTo<order>(x.p, y.p);
      }
      return x.v < y.v;
    });

    size_t n = buf.size();

    auto op = buf.begin();
    auto lq_end = buf.begin() + n;
    BID lst_bid = graph->v2bid_ed(buf.front().v);
    for (auto ed = buf.begin(); ed != lq_end; ed++) {
      auto cur_bid = graph->v2bid_ed(ed->v);
      if (cur_bid == lst_bid) continue;
      push(std::span(op, ed));
      op = ed;
      lst_bid = cur_bid;
    }
    if (op != lq_end) {
      push(std::span(op, lq_end));
    }
  }

  graph_t *get_graph() { return graph; }

  uint64_t getIOStatistic() { return ull(io_engine.getStatisticAll()); }

 private:
  void push(std::span<PV> vs) {
    if (vs.empty()) return;
    auto bid = graph->v2bid_ed(vs.front().v);
    for (auto [p, v] : vs) {
      assert(graph->v2bid_ed(v) == bid);
    }

    BInfo &block = blocks[bid];
    std::lock_guard<BInfo> lg(block);
    if (block.cached) {
      block.insertBatch(vs | std::views::transform([](auto &&x) { return x.v; }));
      return;
    }

    if (!block.isActive()) {
      active_block_num.fetch_add(1, std::memory_order_acq_rel);
    }

    if (block.updatePriorityBatch(vs)) {
      uncached_queue.push(block.prio, bid);
    }
  }

  size_t processCompletedIO() {
    auto items = io_engine.receiveComplete();
    cached_queue.pushBatch(1, std::span(items.begin(), items.end()));
    return items.size();
  }

  // return true if all bids are processed (means still free space to upgrade)
  // return false if there are still bids to be processed (means no free space to upgrade)
  bool upgradeBatch() {
    processCompletedIO();
    auto [prio, bids] = uncached_queue.popBatch();
    if (bids.empty()) {
      return false;
    }

    auto bufs = buffer_pool->allocBatch(bids.size());
    auto bid_num = bids.size();
    auto buf_num = bufs.size();
    std::vector<IOTask> io_tasks(std::min(bid_num, buf_num));
    size_t bid_i = 0, buf_i = 0, num_io = 0;
    while (bid_i < bid_num && buf_i < buf_num) {
      auto bid = bids[bid_i++];
      auto &block = blocks[bid];
      block.data.lock();
      if (block.cached || !block.isActive()) {
        block.unlock();
        continue;
      }
      auto buf = bufs[buf_i++];
      block.cached = true;
      // block.num_exec = 1;
      block.data.unlock_and_set(buf);
      io_tasks[num_io++] = IOTask{bid, buf};
    }

    if (bid_i < bid_num) {
      uncached_queue.pushBatch(prio, std::span(bids.begin() + bid_i, bids.end()));
    }
    if (buf_i < buf_num) {
      buffer_pool->releaseBatch(std::span(bufs.begin() + buf_i, bufs.end()));
    }

    if (num_io == 0) return false;
    if (io_engine.waitComplete(num_io)) {
      processCompletedIO();
    }
    auto num_submit = io_engine.submitIO(io_tasks.begin(), num_io);
    assert(num_submit == num_io);
    return bid_i == bid_num;
  }

  void upgrade() { while (upgradeBatch()); }

  int fd;
  graph_t *graph;
  BufPool *buffer_pool;
  CachedQueue cached_queue;
  UncachedQueue uncached_queue;
  IOEngine io_engine;
  alignas(CACHELINE_SIZE) std::vector<BInfo> blocks;
  alignas(CACHELINE_SIZE) std::atomic_uint64_t active_block_num = 0;
};
}  // namespace acg
#endif  // ACG_WORKLIST_HPP
