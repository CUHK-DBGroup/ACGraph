#ifndef ACG_MBQ_HPP
#define ACG_MBQ_HPP

#include <atomic>
#include <cassert>
#include <cfloat>
#include <cstring>
#include <iostream>
#include <optional>
#include <span>
#include <tuple>
#include <vector>

#include "tls.hpp"

namespace acg {
namespace mbq {

// #define SHRINK 1
#define newA(__E, __n) (__E*)malloc((__n) * sizeof(__E))

using BucketID = int32_t;
static constexpr BucketID NULL_BKT = -1;
static constexpr uint64_t MIN_LG_CAPACITY = 4;
static constexpr uint64_t MIN_CAPACITY = 128;
static constexpr uint64_t LG_CAPACITY_STEP = 1;
static constexpr uint64_t POP_BATCH_NUM = 64;
static constexpr uint64_t CACHELINE = 64;

// Bucket (ring buffer) with wrap around:
//  - when head and tail meets --> empty
//  - when tail-head == capacity --> full, resize
//  - for both head and tail, max is capacity-1
template <typename T, bool SHRINK = true>
struct Bucket {
  size_t lgCapacity;
  size_t head;
  size_t tail;
  T* A;
  using BktElement = std::tuple<BucketID, T>;

  Bucket() : lgCapacity(2), head(0), tail(0), A(newA(T, 1 << lgCapacity)) { assert(A != nullptr); }

  ~Bucket() {
    if (A != nullptr) free(A);
  }

  size_t nextPosition(size_t pos) { return (pos + 1) & (getCapacity() - 1); }
  size_t nextPosition(size_t pos, size_t step) { return (pos + step) & (getCapacity() - 1); }

  size_t size() const {
    size_t capacity = getCapacity();
    return (head <= tail) ? tail - head : capacity - head + tail;
  }

  size_t getCapacity() const {
    size_t cap = 1;
    return cap << lgCapacity;
  }

  bool isEmpty() { return head == tail; }

  void upsize() {
    size_t capacity = getCapacity();
    lgCapacity += LG_CAPACITY_STEP;
    size_t new_capacity = getCapacity();

    // Since we are here, the original slots should
    // already be completely full.
    A = (T*)realloc(A, new_capacity * sizeof(T));
    assert(A != NULL);
    head = 0;
    tail = capacity;
  }

  void upsize(size_t ned_cap) {
    assert(head == 0);
    while (getCapacity() <= ned_cap) {
      lgCapacity++;
    }
    A = (T*)realloc(A, getCapacity() * sizeof(T));
    assert(A != nullptr);
  }

  void downsize() {
    if (!SHRINK) return;
    // shrink if only 1/4 of space is occupied
    // and capacity is > MIN_CAPACITY
    uint64_t capacity = 1 << lgCapacity;
    uint64_t remain = size();
    if (remain >= (capacity >> 2) || lgCapacity == MIN_LG_CAPACITY) return;

    // check if copying is needed
    uint64_t half = capacity >> 1;
    if (head >= half || tail >= half) {
      if (head > tail) {
        // wrapped around, copy the head portion to the tail
        // before:  A = [--t           h--]
        // after:   A = [h----t           ]
        uint64_t copySize = capacity - head;
        memcpy(A + tail, A + head, copySize * sizeof(T));
        head = 0;
        tail = remain;
      } else if (head < half && tail >= half) {
        // crossed halfway point, copy the portion above half
        // to the start of A
        // before:  A = [     h-----t     ]
        // after:   A = [---t h--         ]
        uint64_t copySize = tail - half;
        memcpy(A, A + half, copySize * sizeof(T));
        tail = copySize;
      } else {
        // simply copy to the start
        // before:  A = [         h----t  ]
        // after:   A = [h----t           ]
        memcpy(A, A + head, remain * sizeof(T));
        head = 0;
        tail = remain;
      }
    }

    // Note: apparently this triggers extra
    //       copying and results in slow down.
    // A = (T*) realloc(A, half * sizeof(T));
    assert(A != NULL);
    lgCapacity--;
  }

  // increment tail and fill in the slot
  bool push(T val) {
    // try to increment tail without surpassing head
    A[tail] = val;
    tail = nextPosition(tail);
    if (tail == head) upsize();
    return true;
  }

  bool pushBatch(std::span<T> buffer) {
    if (buffer.empty()) return true;

    size_t capacity = getCapacity();
    size_t available_space = capacity - size();
    size_t needed_cap = size() + buffer.size();
    // A = [..h---t..]/[--t...h--], where
    // '-' means there exists a element, while '.' instead.

    // When there is no enough space for entire buffer,
    // copy the part of buffer to make A form [h---t...] first,
    // and then realloc enough space and copy the rest to A.
    if (buffer.size() >= available_space) {
      auto it = buffer.begin();
      if (tail < head) {
        it += available_space;
        std::copy(buffer.begin(), it, A + tail);
        assert(tail + available_space == head);
        head = 0;
        tail = capacity;
      } else {
        if (head != 0) {
          it += head;
          std::copy(buffer.begin(), it, A);
          head = 0;
        }
      }
      upsize(needed_cap);
      if (it != buffer.end()) {
        std::copy(it, buffer.end(), A + tail);
        tail += (buffer.end() - it);
      }
      return true;
    }

    // [---t..h---]
    if (tail < head) {
      std::copy(buffer.begin(), buffer.end(), A + tail);
      tail = nextPosition(tail, buffer.size());
    } else {
      // [..h---t..]
      // copy to the end of the buffer
      size_t suf_size = capacity - tail;
      size_t cp_size = std::min(buffer.size(), suf_size);
      auto it = buffer.begin() + cp_size;
      std::copy(buffer.begin(), it, A + tail);
      tail = nextPosition(tail, cp_size);
      if (it != buffer.end()) {
        assert(tail == 0);
        std::copy(it, buffer.end(), A + tail);
        tail = nextPosition(tail, buffer.size() - cp_size);
      }
    }

    return true;
  }

  // increment head and return the item
  // loop when the key is not yet filled in
  T pop() {
    // try to increment head without surpassing tail
    assert(!isEmpty());

    T val = A[head];
    head = nextPosition(head);

    downsize();
    return val;
  }

  uint64_t popBatch(uint64_t num, std::vector<T>& popBuffer) {
    // try to increment head without surpassing tail
    if (head == tail) return 0;

    size_t capacity = getCapacity();

    // if head > tail, wrapped around, then we
    // only pop up to the capacity to ensure we do not
    // incur extra cache misses
    uint32_t popNum = std::min(head < tail ? tail - head : capacity - head, num);
    uint64_t modMask = capacity - 1;
    popBuffer.insert(popBuffer.end(), A + head, A + head + popNum);
    head = (head + popNum) & modMask;

    downsize();
    return popNum;
  }
};  // namespace mbq

enum BucketOrder { decreasing, increasing };

// Priority Range from [0, MAX_BKT)
// Prio == 0 means null bucket
template <typename T, size_t NUM_BKTS = 1024>
struct BucketQueue {
 public:
  Bucket<T>* bkts;  // list of buckets
  BucketID cur_bkt;       // index of the lowest bucket in current range, no underflow
  uint64_t n;             // total number of elements across open buckets
  static constexpr T EMPTY_KEY = 0;

  using BktElement = std::tuple<BucketID, T>;
  using PQElement = std::tuple<BucketID, T>;

  // Create a bucketing structure.
  //   d : map from identifier -> bucket
  //   order : the order to iterate over the buckets
  //   total_buckets: the total buckets to materialize
  //
  //   For an identifier i:
  //   d(i) is the bucket currently containing i
  //   d(i) = UINT64_MAX if i is not in any bucket
  BucketQueue() : cur_bkt(NUM_BKTS), n(0) {
    // Initialize array consisting of the materialized buckets.
    bkts = new Bucket<T>[NUM_BKTS]();
  }

  ~BucketQueue() { delete[] bkts; }

  bool empty() { return n == 0; }

  bool nextBucket() {
    if (n == 0) return false;
    while (bkts[cur_bkt].isEmpty()) {
      cur_bkt++;
    }
    return true;
  }

  // insert the task to its corresponding bucket
  void insertInBucket(BucketID b, T val) {
    cur_bkt = std::min(cur_bkt, b);
    bkts[b].push(val);
  }

  // Push a single task
  BucketID push(BucketID b, T key) {
    insertInBucket(b, key);
    n++;
    return cur_bkt;
  }

  BucketID pushBatch(std::span<PQElement> pushBuffer) {
    for (auto& [bkt, t] : pushBuffer) {
      insertInBucket(bkt, t);
    }
    n += pushBuffer.size();
    return cur_bkt;
  }

  BucketID pushBatch(BucketID b, std::span<T> buffer) {
    cur_bkt = std::min(cur_bkt, b);
    bkts[b].pushBatch(buffer);
    n += buffer.size();
    return cur_bkt;
  }

  std::tuple<T, BucketID> pop() {
    if (!nextBucket()) {
      return {EMPTY_KEY, NULL_BKT};
    }
    auto key = bkts[cur_bkt].pop();
    n--;
    return {key, cur_bkt};
  }

  std::tuple<uint64_t, BucketID> popBatch(std::vector<T>& popBuffer) {
    if (!nextBucket()) {
      return {0, NULL_BKT};
    }
    uint64_t numPopped = bkts[cur_bkt].popBatch(POP_BATCH_NUM, popBuffer);
    n -= numPopped;
    return {numPopped, cur_bkt};
  }

  BucketID getTopBkt() {
    if (nextBucket()) return cur_bkt;
    return NULL_BKT;
  }
};

template <typename PrioType, typename T, BucketOrder order = increasing,
          size_t NUM_BKTS = MAX_PRIORITY>
class MultiBucketQueue {
 private:
  struct PQElement {
    PrioType prio;
    T key;
  };

  // Wrapper for individual bucket queues
  struct PQContainer {
    uint64_t pushes = 0;
    uint64_t pops = 0;
    BucketQueue<T, NUM_BKTS>* bq;
    std::atomic<BucketID> topBkt{NULL_BKT};
    std::atomic<bool> queueLock{false};

    void unlock() { queueLock.store(false, std::memory_order_release); }
    // Returns true if lock was successfully acquired, false otherwise
    bool tryLock() {
      bool flag = queueLock.load(std::memory_order_acquire);
      if (flag) return false;
      return queueLock.compare_exchange_weak(flag, true, std::memory_order_acq_rel,
                                             std::memory_order_acquire);
    }

    ~PQContainer() { delete bq; }
  } __attribute__((aligned(CACHELINE)));

  // The list of bucket queues
  std::vector<PQContainer> queues;
  uint64_t numQueues;

  struct ThreadLocalData {
    ThreadLocalData() {
      this->seed = static_cast<uint64_t>(pthread_self());
      this->popBuffer.reserve(POP_BATCH_NUM);
    }

    void init() {
      this->seed = static_cast<uint64_t>(pthread_self());
      this->popBuffer.reserve(POP_BATCH_NUM);
    }

    uint64_t next_random() {
      uint64_t z = (seed += UINT64_C(0x9E3779B97F4A7C15));
      z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
      z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
      return z ^ (z >> 31);
    }

    uint64_t seed = 0;
    BucketID poppedBkt = 0;
    std::vector<T> popBuffer;
  } __attribute__((aligned(CACHELINE)));

  acg::ThreadLocalStorage<ThreadLocalData> tls;
  ThreadLocalData& get_tld() { return *tls.get_data(); }

  // std::vector<ThreadLocalData> tlds;
  // static std::atomic_int32_t thread_counter;
  // static thread_local int32_t tid;
  // ThreadLocalData& get_tld() { return tlds[tid]; }

  // std::atomic<uint64_t> numIdle{0};
  std::atomic<uint64_t> numEmpty;
  std::atomic_uint64_t numElems{0};

 public:
  MultiBucketQueue(uint64_t nQ) : queues(nQ), numQueues(nQ), numEmpty(nQ) {
    assert(nQ >= 2);
    for (uint64_t i = 0; i < numQueues; i++) {
      queues[i].bq = new BucketQueue<T, NUM_BKTS>();
    }
  };

  void initTID() { tls.initThread(); }
  // void initTID() {
  //   if (tid < 0) {
  //     tid = thread_counter.fetch_add(1);
  //   }
  //   assert(tid < tlds.size());
  //   tlds[tid].init();
  // }

  bool empty() const { return numEmpty.load(std::memory_order_acquire) == numQueues; }

  // Performs actual insertion of tasks in the push buffer
  void push(PrioType prio, T key) {
    uint64_t qid = selectRandomQueue(get_tld());
    auto& q = queues[qid];

    // Update numEmpty status
    if (q.bq->empty()) {
      numEmpty.fetch_sub(1, std::memory_order_acq_rel);
    }

    BucketID b = priority2bucket(prio);
    BucketID cur_bkt = q.bq->push(b, key);
    q.pushes++;
    q.topBkt.store(cur_bkt, std::memory_order_release);
    numElems.fetch_add(1, std::memory_order_acq_rel);
    q.unlock();
  }

  // Push the task into local pushBuffer until it fills up
  void pushBatch(std::span<PQElement> buffer) {
    if (buffer.empty()) return;

    uint64_t qid = selectRandomQueue(get_tld());
    auto& q = queues[qid];

    if (q.bq->empty()) {
      numEmpty.fetch_sub(1, std::memory_order_acq_rel);
    }

    BucketID cur_bkt;
    for (auto& [prio, key] : buffer) {
      BucketID b = priority2bucket(prio);
      cur_bkt = q.bq->push(b, key);
    }
    q.pushes += buffer.size();
    q.topBkt.store(cur_bkt, std::memory_order_release);
    numElems.fetch_add(buffer.size(), std::memory_order_acq_rel);
    q.unlock();
  }

  uint64_t size() const { return numElems.load(std::memory_order_relaxed); }

  void pushBatch(PrioType prio, std::span<T> buffer) {
    if (buffer.empty()) return;

    uint64_t qid = selectRandomQueue(get_tld());
    auto& q = queues[qid];

    if (q.bq->empty()) {
      numEmpty.fetch_sub(1, std::memory_order_acq_rel);
    }

    BucketID cur_bkt = q.bq->pushBatch(priority2bucket(prio), buffer);
    q.pushes += buffer.size();
    q.topBkt.store(cur_bkt, std::memory_order_release);
    numElems.fetch_add(buffer.size(), std::memory_order_acq_rel);
    q.unlock();
  }

  std::optional<PQElement> pop() {
    ThreadLocalData& tld = get_tld();
    std::vector<T>& popBuffer = tld.popBuffer;
    // increment count and keep on trying to pop
    if (popBuffer.empty() && !fill_pop_buffer(tld)) {
      return std::nullopt;
    }
    assert(!popBuffer.empty());
    PQElement ret = {bucket2priority(tld.poppedBkt), std::move(popBuffer.back())};
    popBuffer.pop_back();
    numElems.fetch_sub(1, std::memory_order_acq_rel);
    return ret;
  }

  std::tuple<PrioType, std::vector<T>> popBatch() {
    ThreadLocalData& tld = get_tld();
    std::vector<T>& popBuffer = tld.popBuffer;

    // increment count and keep on trying to pop
    if (popBuffer.empty() && !fill_pop_buffer(tld)) {
      return std::make_tuple(-1, std::vector<T>{});
    }
    assert(!popBuffer.empty());
    std::vector<T> ret(popBuffer.begin(), popBuffer.end());
    popBuffer.clear();
    numElems.fetch_sub(ret.size(), std::memory_order_acq_rel);
    return {bucket2priority(tld.poppedBkt), ret};
  }

 private:
  uint64_t selectRandomQueue(ThreadLocalData& tld) {
    uint64_t qid = tld.next_random() % numQueues;
    while (!queues[qid].tryLock()) {
      qid = tld.next_random() % numQueues;
    }
    return qid;
  }

  BucketID priority2bucket(PrioType prio) {
    if (increasing) {
      return prio;
    } else {
      return NUM_BKTS - prio;
    }
  }

  PrioType bucket2priority(BucketID bkt) {
    if (increasing) {
      return bkt;
    } else {
      return NUM_BKTS - bkt;
    }
  }

  bool fill_pop_buffer(ThreadLocalData& tld) {
    // uint64_t num = numIdle.fetch_add(1, std::memory_order_acq_rel);
    do {
      auto bid = try_fill_pop(tld);
      if (bid != NULL_BKT) {
        tld.poppedBkt = bid;
        break;
      }
      // if (num >= numThreads) return false;
      if (empty()) return false;
      // num = numIdle.load(std::memory_order_relaxed);
    } while (true);
    // numIdle.fetch_sub(1, std::memory_order_acq_rel);
    return true;
  }

  BucketID try_fill_pop(ThreadLocalData& tld) {
    uint64_t poppingQueue = numQueues;
    while (true) {
      // If the last used queue is locked, go with the normal procedure
      // Pick the higher priority max of queue i and j
      uint64_t i = tld.next_random() % numQueues;
      uint64_t j = tld.next_random() % numQueues;
      while (j == i) {
        j = tld.next_random() % numQueues;
      }

      BucketID bI = queues[i].topBkt.load(std::memory_order_acquire);
      BucketID bJ = queues[j].topBkt.load(std::memory_order_acquire);

      // check if there are no tasks available
      if (bI == NULL_BKT && bJ == NULL_BKT) {
        uint64_t emptyQueues = numEmpty.load(std::memory_order_acquire);
        if (emptyQueues >= queues.size())
          break;
        else
          continue;
      } else if (bI == NULL_BKT) {
        poppingQueue = j;
      } else if (bJ == NULL_BKT) {
        poppingQueue = i;
      } else {
        poppingQueue = (bI <= bJ ? i : j);
      }

      if (!queues[poppingQueue].tryLock()) continue;

      // lock acquired, perform popping
      PQContainer& q = queues[poppingQueue];
      if (q.bq->empty()) {
        q.topBkt.store(NULL_BKT, std::memory_order_release);
        q.unlock();
        continue;
      }
      auto [size, bid] = q.bq->popBatch(tld.popBuffer);

      // Update this queue's topBkt if necessary
      BucketID cur = q.bq->getTopBkt();
      q.topBkt.store(cur, std::memory_order_release);
      if (cur == NULL_BKT) {
        numEmpty.fetch_add(1, std::memory_order_acq_rel);
      }
      q.unlock();

      if (size == 0) {
        continue;
      }
      q.pops += size;
      return bid;
    }
    return NULL_BKT;
  }
};

// template <typename PrioType, typename T, BucketOrder order, size_t NUM_BKTS>
// std::atomic_int32_t MultiBucketQueue<PrioType, T, order, NUM_BKTS>::thread_counter{0};

// template <typename PrioType, typename T, BucketOrder order, size_t NUM_BKTS>
// thread_local int32_t MultiBucketQueue<PrioType, T, order, NUM_BKTS>::tid{-1};

#undef newA
}  // namespace mbq
}  // namespace acg
#endif  // ACG_MBQ_HPP
