#ifndef ACG_IO_WORKER_HPP
#define ACG_IO_WORKER_HPP

#include <liburing.h>
#undef BLOCK_SIZE
#include <chrono>
#include <cstring>
#include <queue>
#include <span>
#include <thread>
#include <vector>

#include "tls.hpp"
#include "types.hpp"

using namespace std::chrono_literals;

namespace acg {
constexpr uint32_t MIN_QUEUE_DEPTH = 8192;
constexpr uint32_t IO_BATCH_SIZE = 64;

struct IOTask {
  BlockID bid;
  char *buf;
};

class IOWorker {
 public:
  IOWorker(int fd, uint32_t queue_depth=MIN_QUEUE_DEPTH) : fd(fd), queue_depth(queue_depth) {
    int ret = io_uring_queue_init(queue_depth, &ring, 0);
    assert(ret == 0);
  }

  ~IOWorker() { io_uring_queue_exit(&ring); }

  template <typename Iterator>
  uint32_t submitIO(Iterator op, uint32_t num_submit) {
    num_submit = std::min(num_submit, IO_BATCH_SIZE);
    num_submit = std::min(num_submit, queue_depth - inflight);

    for (uint32_t i = 0; i < num_submit; ++i) {
      auto [bid, buf] = op[i];
      struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
      assert(sqe != nullptr);
      io_uring_sqe_set_data64(sqe, bid);
      io_uring_prep_read(sqe, fd, buf, BLOCK_SIZE, bid * BLOCK_SIZE);
    }
    io_num_statistic += num_submit * BLOCK_SIZE;
    if (num_submit > 0) {
      int ret = io_uring_submit(&ring);
      assert(ret == int(num_submit));
      inflight += num_submit;
    }
    return num_submit;
  }

  bool waitComplete(uint32_t target_free_sqe) {
    if (inflight + target_free_sqe <= queue_depth) return false;
    auto num = inflight + target_free_sqe - queue_depth;
    struct io_uring_cqe *cqe;
    int ret = io_uring_wait_cqe_nr(&ring, &cqe, num);
    assert(ret == 0);
    return true;
  }

  void waitUntil(uint32_t target_depth) { waitComplete(queue_depth - target_depth); }

  std::vector<BlockID> receiveComplete(uint32_t num_receive = 0) {
    if (num_receive == 0) {
      num_receive = queue_depth;
    }
    std::vector<BlockID> out_buf(std::min(num_receive, inflight));
    struct io_uring_cqe *cqe;
    unsigned head;
    uint32_t cnt = 0;
    io_uring_for_each_cqe(&ring, head, cqe) {
      assert(cqe->res >= 0);
      BlockID bid = static_cast<BlockID>(io_uring_cqe_get_data64(cqe));
      out_buf[cnt++] = bid;
      if (cnt >= num_receive) {
        break;
      }
    }
    io_uring_cq_advance(&ring, cnt);
    inflight -= cnt;
    out_buf.resize(cnt);
    return out_buf;
  }

  uint64_t getStatistic() const { return io_num_statistic; }

  int fd;
  uint32_t inflight{0};
  uint32_t queue_depth{0};
  struct io_uring ring;
  uint64_t io_num_statistic{0};
};

class IOEngine {
 public:
  IOEngine() = default;

  IOEngine(int fd, uint32_t queue_depth)
      : fd(fd), queue_depth(std::max(queue_depth, MIN_QUEUE_DEPTH)) {}

  void init(int fd, uint32_t queue_depth) {
    this->fd = fd;
    this->queue_depth = 1;
    while (this->queue_depth < queue_depth) {
      this->queue_depth <<= 1;
    }
    this->queue_depth = std::max(this->queue_depth, MIN_QUEUE_DEPTH);
  }

  void initThread() { io_workers.initThread(fd); }

  template <typename Iterator>
  uint32_t submitIO(Iterator op, uint32_t num_submit) {
    return io_workers.get_data()->submitIO(op, num_submit);
  }

  bool waitComplete(uint32_t target_free_sqe) {
    return io_workers.get_data()->waitComplete(target_free_sqe);
  }

  std::vector<BlockID> receiveComplete() { return io_workers.get_data()->receiveComplete(); }

  uint64_t getStatistic() const { return io_workers.get_data()->getStatistic(); }

  // ! thread unsafe
  uint64_t getStatisticAll() const {
    auto all = io_workers.getInstanceAll();
    uint64_t total = 0;
    for (auto worker : all) {
      total += worker->getStatistic();
    }
    return total;
  }

 private:
  int fd = -1;
  uint32_t queue_depth = 0;
  ThreadLocalStorage<IOWorker> io_workers;
};

}  // namespace acg
#endif  // ACG_IO_WORKER_HPP