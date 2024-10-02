#ifndef ACG_GRAPH_HPP
#define ACG_GRAPH_HPP

#include <cmath>
#include <functional>
#include <queue>
#include <unordered_set>
#include <vector>

#include "io/file.hpp"
#include "meta_data.hpp"
#include "types.hpp"
#include "util.hpp"
#include "timer.hpp"

namespace acg {

template <typename VID = uint32_t, typename EID = uint64_t, typename BID = uint64_t,
          bool wgt = false, typename WID = float>
class Graph {
 public:
  using vid_t = VID;
  using eid_t = EID;
  using bid_t = BID;
  using partition_t = vid_t;
  static constexpr uint32_t EDGE_NUM_PER_BLOCK = BLOCK_SIZE / sizeof(vid_t);

  static constexpr EID VIRTUAL_BIT = static_cast<EID>(1) << (sizeof(EID) * 8 - 1);
  static constexpr VID INVALID_V = std::numeric_limits<VID>::max();

  struct v_wrapper_t {
   public:
    v_wrapper_t() : ptr(nullptr), ed(nullptr) {}

    v_wrapper_t(const char *op, const char *ed) : ptr(op), ed(ed) {}

    [[nodiscard]] bool has_next() const {
      return ptr != ed && *reinterpret_cast<const vid_t *>(ptr) != INVALID_V;
    }

    vid_t next() {
      assert(has_next());
      vid_t ret = *reinterpret_cast<const vid_t *>(ptr);
      ptr += sizeof(vid_t);
      return ret;
    }

   private:
    const char *ptr;
    const char *ed;
  };

  struct vinfo_t {
    uint64_t oft;
  };

  explicit Graph(const std::filesystem::path &graph_path) : graph_path(graph_path) {
    Timer timer(TIMER::GRAPH_INIT);
    // log_info("graph path: %s", graph_path.c_str());
    auto meta = load_meta<vid_t, eid_t, bid_t>(meta_path());

    n_nodes_ = meta.n_nodes;
    n_edges_ = meta.n_edges;
    n_blocks_ = meta.n_blocks;
    n_rnodes_ = meta.n_rnodes;
    n_vnodes_ = meta.n_vnodes;
    id_bound_ = meta.id_bound;
    deg_bound_ = meta.deg_bound;
    log_report("deg_bound: %u", deg_bound_);
    // log_info("#nodes: %llu (#rnodes: %llu, #vnodes: %llu), id_bound: %llu, deg_bound: %llu.",
    //          (ull)n_nodes_, (ull)n_rnodes_, (ull)n_vnodes_, (ull)id_bound_, (ull)deg_bound_);
    // log_info("#edges: %llu, #edge_blocks: %llu.", (ull)n_edges_, (ull)n_blocks_);

    size_t vinfo_size = id_bound_ * sizeof(vinfo_t);
    size_t lv_nbs_size = (n_nodes_ - id_bound_) * deg_bound_ * sizeof(vid_t);
    size_t loaded_size = load_aligned(vinfo_path(), vinfo);
    assert(loaded_size == vinfo_size);
    loaded_size = load_aligned(lv_nbs_path(), lv_nbs);
    assert(loaded_size == lv_nbs_size);
    // log_info("#size_of_vinfo: %zuKB, #size_of_lv_nbs: %zuKB", vinfo_size / 1024,
    //  lv_nbs_size / 1024);

    assert(std::filesystem::file_size(edge_block_path()) == n_blocks_ * BLOCK_SIZE);

    fd = open(edge_block_path().c_str(), O_RDONLY | O_DIRECT);
    assert(fd > 0);
  }

  virtual ~Graph() {
    close(fd);
    std::free(lv_nbs);
    std::free(vinfo);
  }

  [[nodiscard]] std::filesystem::path meta_path() const { return graph_path / "meta"; }
  [[nodiscard]] std::filesystem::path vinfo_path() const { return graph_path / "vinfo"; }
  [[nodiscard]] std::filesystem::path lv_nbs_path() const { return graph_path / "lv_nbs"; }
  [[nodiscard]] std::filesystem::path edge_block_path() const { return graph_path / "edge_blocks"; }
  [[nodiscard]] std::filesystem::path binfo_path() const { return graph_path / "binfo"; }
  [[nodiscard]] std::filesystem::path v2id_path() const { return graph_path / "v2id"; }
  [[nodiscard]] std::filesystem::path id2v_path() const { return graph_path / "id2v"; }

  vid_t node_num() const { return n_nodes_; }
  vid_t rnode_num() const { return n_rnodes_; }
  vid_t vnode_num() const { return n_vnodes_; }
  eid_t edge_num() const { return n_edges_; }
  bid_t block_num() const { return n_blocks_; }
  vid_t id_bound() const { return id_bound_; }

  vid_t deg(vid_t v) const {
    if (is_bound(v)) {
      auto ptr = &lv_nbs[(v - id_bound_) * deg_bound_];
      if (deg_bound_ == 0 || ptr[0] == INVALID_V) return 0;
      if (deg_bound_ == 1 || ptr[1] == INVALID_V) return 1;
      if (deg_bound_ == 2 || ptr[2] == INVALID_V) return 2;
      if (deg_bound_ == 3 || ptr[3] == INVALID_V) return 3;
      //      if (deg_bound_ == 4 || ptr[4] == INVALID_V) return 4;
      assert(deg_bound_ == 4);
      return 4;
    }
    return (oft(v + 1) - oft(v)) / sizeof(vid_t);
  }

  uint64_t oft(vid_t v) const { return vinfo[v].oft & ~VIRTUAL_BIT; }

  bool is_virtual(vid_t v) const { return v < id_bound_ && VIRTUAL_BIT & vinfo[v].oft; }
  bool is_oversize(vid_t v) const { return store_size_of(v) >= BLOCK_SIZE; }
  bool is_bound(vid_t v) const { return v >= id_bound_; }

  v_wrapper_t get_neighbors(vid_t v) const {
    assert(is_bound(v));
    auto op = reinterpret_cast<const char *>(&lv_nbs[(v - id_bound_) * deg_bound_]);
    auto ed = reinterpret_cast<const char *>(&lv_nbs[(v - id_bound_ + 1) * deg_bound_]);
    return get_neighbors(op, ed);
  }

  v_wrapper_t get_neighbors(vid_t v, const char *data) const {
    assert(v < id_bound_);
    size_t op = oft(v) % BLOCK_SIZE;
    size_t ed = op + deg(v) * sizeof(vid_t) % BLOCK_SIZE;
    return v_wrapper_t{data + op, data + ed};
  }

  v_wrapper_t get_neighbors(const char *data_op, const char *data_ed) const {
    return v_wrapper_t{data_op, data_ed};
  }

  template <typename F>
  void apply_neighbors(vid_t v, F &&f) {
    auto ptr = &lv_nbs[(v - id_bound_) * deg_bound_];
    if (deg_bound_ == 0 || ptr[0] == INVALID_V) return;
    f(ptr[0]);
    if (deg_bound_ == 1 || ptr[1] == INVALID_V) return;
    f(ptr[1]);
    if (deg_bound_ == 2 || ptr[2] == INVALID_V) return;
    f(ptr[2]);
    if (deg_bound_ == 3 || ptr[3] == INVALID_V) return;
    f(ptr[3]);
  }

  template <typename F>
  void apply_neighbors(vid_t v, const char *data, F &&f) {
    assert(v < id_bound_);
    auto op = reinterpret_cast<const vid_t *>(data + oft(v) % BLOCK_SIZE);
    auto ed = op + (deg(v) % (BLOCK_SIZE / sizeof(vid_t)));
    while (op != ed) {
      f(*op++);
    }
  }

  template <typename F>
  void apply_neighbors(const char *data_op, const char *data_ed, F &&f) {
    auto op = reinterpret_cast<const vid_t *>(data_op);
    auto ed = reinterpret_cast<const vid_t *>(data_ed);
    while (op != ed) {
      f(*op++);
    }
  }

  // return the size of the neighbors of v in disk
  size_t store_size_of(vid_t v) const {
    if (is_virtual(v)) return 0;
    return vinfo[v + 1].oft - vinfo[v].oft;
  }

  bid_t v2bid_ed(bid_t v) const {
    assert(!is_virtual(v));
    assert(!is_bound(v));
    assert(oft(v + 1) != oft(v));

    return (oft(v + 1) - 1) / BLOCK_SIZE;
  }

  bid_t v2bid_op(bid_t v) const {
    assert(!is_virtual(v));
    assert(!is_bound(v));
    assert(oft(v + 1) != oft(v));

    return oft(v) / BLOCK_SIZE;
  }

  template <typename T>
  ssize_t read_block(T *data, BID bid) const {
    return read_blocks(data, bid, 1);
  }

  template <typename T>
  ssize_t read_blocks(T *data, BID bid, uint32_t n) {
    total_read.fetch_add(n * BLOCK_SIZE);
    return pread(fd, data, n * BLOCK_SIZE, bid * BLOCK_SIZE);
  }

  partition_t get_partition(vid_t v) const { return v >> 14; }

  uint64_t stat_info() const { return total_read.load(); }

  std::vector<vid_t> load_v2id() const { return load_raw<vid_t>(v2id_path()); }
  std::vector<vid_t> load_id2v() const { return load_raw<vid_t>(id2v_path()); }

 protected:
  template <typename T>
  size_t load_aligned(const std::filesystem::path &filename, T *&buf) {
    const auto fsize = std::filesystem::file_size(filename);
    assert(fsize % sizeof(T) == 0);
    auto buf_size = (fsize + 4095) / 4096 * 4096;
    buf = (T *)std::aligned_alloc(4096, buf_size);
    assert(buf != nullptr);

    int fd = open(filename.c_str(), O_RDONLY | O_DIRECT);
    assert(fd != -1);

    size_t aligend_size = fsize - fsize % 4096;
    size_t oft = 0;
    while (oft < aligend_size) {
      auto rd = read(fd, ((char *)buf) + oft, aligend_size - oft);
      assert(rd > 0);
      oft += rd;
    }
    assert(oft == aligend_size);
    close(fd);

    if (fsize != aligend_size) {
      fd = open(filename.c_str(), O_RDONLY);
      while (oft < fsize) {
        auto rd = pread(fd, ((char *)buf) + oft, fsize - oft, oft);
        assert(rd > 0);
        oft += rd;
      }
      close(fd);
    }
    assert(oft == fsize);
    // total_read.fetch_add(fsize, std::memory_order_relaxed);

    return fsize;
  }

  const std::filesystem::path graph_path;
  vid_t n_nodes_{};
  eid_t n_edges_{};
  bid_t n_blocks_{};

  vid_t n_rnodes_{};
  vid_t n_vnodes_{};
  vid_t id_bound_{};
  vid_t deg_bound_{};

  vinfo_t *vinfo = nullptr;
  vid_t *lv_nbs = nullptr;
  int fd;

  alignas(CACHELINE_SIZE) std::atomic_uint64_t total_read{0};
};

template <typename VID = uint32_t, typename EID = uint64_t, typename BID = uint64_t,
          bool wgt = false, typename WID = float>
class mem_graph : public Graph<VID, EID, BID, wgt, WID> {
  using base_graph = Graph<VID, EID, BID, wgt, WID>;

 public:
  explicit mem_graph(const std::filesystem::path &path) : base_graph(path) {
    Timer timer(TIMER::LOAD);

    assert(std::filesystem::file_size(base_graph::edge_block_path()) ==
           base_graph::n_blocks_ * BLOCK_SIZE);
    edges = load_raw<char>(base_graph::edge_block_path());

    log_info("#size_of_edges: %zuKB", sizeof(char) * edges.size() >> 10);
    log_info("end loading.");
  }

  base_graph::v_wrapper_t get_neighbors(base_graph::vid_t v) const {
    if (base_graph::is_bound(v)) {
      return base_graph::get_neighbors(v);
    }
    return base_graph::get_neighbors(&edges[base_graph::oft(v)], &edges[base_graph::oft(v + 1)]);
  }
  
  template <typename F>
  void apply_neighbors(base_graph::vid_t v, F &&f) {
    if (base_graph::is_bound(v)) {
      return base_graph::apply_neighbors(v, std::forward<F>(f));
    }
    base_graph::apply_neighbors(&edges[base_graph::oft(v)], &edges[base_graph::oft(v + 1)],
                                std::forward<F>(f));
  }

 private:
  std::vector<char> edges;
};

template <typename VID, typename EID>
struct csr_graph {
  using vid_t = VID;
  using eid_t = EID;
  using oft_vec = std::vector<eid_t>;
  using data_vec = std::vector<vid_t>;
  using csr_meta = std::tuple<uint8_t, uint8_t, uint8_t, vid_t, eid_t>;

  struct v_wrapper_t {
   public:
    v_wrapper_t() : ptr(nullptr), ed(nullptr) {}

    v_wrapper_t(const char *op, const char *ed) : ptr(op), ed(ed) {}

    [[nodiscard]] bool has_next() const { return ptr != ed; }

    vid_t next() {
      assert(has_next());
      vid_t ret = *reinterpret_cast<const vid_t *>(ptr);
      ptr += sizeof(vid_t);
      return ret;
    }

   private:
    const char *ptr;
    const char *ed;
  };

  explicit csr_graph(const std::filesystem::path &in_path) : graph_path(in_path) { load(in_path); }

  void load(const std::filesystem::path &in_path) {
    Timer timer(TIMER::LOAD);
    auto [vt_size, et_size, wt_size, n_nodes, n_edges] = load_file<csr_meta>(in_path / "meta");
    assert(vt_size == sizeof(vid_t));
    assert(et_size == sizeof(eid_t));
    assert(wt_size == 0);

    n = n_nodes;
    m = n_edges;
    oft = load_raw<eid_t>(in_path / "offset");
    assert(oft.size() == n_nodes + 1);
    data = load_raw<vid_t>(in_path / "data");
    assert(data.size() == n_edges);
    log_info("Load Num: %lf MB", (n_edges * sizeof(vid_t)) / 1024. / 1024.);
  }

  [[nodiscard]] vid_t deg(vid_t v) const { return oft[v + 1] - oft[v]; }

  [[nodiscard]] vid_t node_num() const { return n; }
  [[nodiscard]] eid_t edge_num() const { return m; }

  [[nodiscard]] const vid_t *neg_op(vid_t v) const { return &data[oft[v]]; }
  [[nodiscard]] const vid_t *neg_ed(vid_t v) const { return &data[oft[v + 1]]; }

  v_wrapper_t get_neighbors(vid_t v) {
    return {reinterpret_cast<const char *>(neg_op(v)), reinterpret_cast<const char *>(neg_ed(v))};
  }

 private:
  std::filesystem::path graph_path;
  vid_t n{0};
  eid_t m{0};
  oft_vec oft;
  data_vec data;
};

}  // namespace acg
#endif  // ACG_GRAPH_HPP
