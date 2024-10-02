#include <set>
#include "io/file.hpp"
#include "types.hpp"
#include <omp.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <queue>
#include "meta_data.hpp"
#include "partitioner.hpp"

using namespace acg;
using std::filesystem::path;

static constexpr int MAX_BLOCK_GAP  = 16;
static constexpr VertexID INVALID_V = std::numeric_limits<VertexID>::max();
static constexpr EdgeID VIRTUAL_BIT = static_cast<EdgeID>(1)<<(sizeof(EdgeID)*8-1);

struct Timer {
  Timer() {}

  void report(const std::string &msg) {
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    start_time = end_time;
    printf("%s: %lld ms\n", msg.c_str(), duration);
  }

  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
};


enum ParitionMethod {
  SIMPLE,
  LPLF,
  BEST_FIT,
};

struct Config {
  std::string in_path;
  std::string out_path;
  uint32_t tdeg = 2;
  int max_block_gap = 8;
  ParitionMethod partition_method = LPLF;
  bool degree_packing = false;
};

Config parseParameters(int argc, char *argv[]) {
  static constexpr char help[] =
    "%s <in_path> <out_path> [options]\n"
    "options:\n"
    "  --tdeg     [default 2]\n"
    "  --pm       [default lplf] (simple, lplf, bf)\n"
    "  --max_gap  [default 8]    (used in lplf only)\n"
    "  --dp       [default false]";
  
  Config config{};
  config.in_path = argv[1];
  config.out_path = argv[2];
  for (int i = 3; i < argc; ++i) {
    if (strcmp(argv[i], "--tdeg") == 0) {
      config.tdeg = std::stoi(argv[++i]); } 
    else if (strcmp(argv[i], "--pm") == 0) {
      std::string pm = argv[++i];
      if (pm == "simple")    { config.partition_method = SIMPLE; } 
      else if (pm == "lplf") { config.partition_method = LPLF; } 
      else if (pm == "bf")   { config.partition_method = BEST_FIT; } 
      else { 
        log_fatal("unknown partition method %s\nusage:\n%s", pm.c_str(), help);
        abort(); } } 
    else if (strcmp(argv[i], "--max_gap") == 0) {
      config.max_block_gap = std::stoi(argv[++i]); }
    else if (strcmp(argv[i], "--dp") == 0) {
      config.degree_packing = true; }
    else {
      log_fatal("unknown option %s\nusage:\n%s", argv[i], help);
      abort(); } }
  return config;
}


static constexpr size_t EDGE_NUM_PER_BLOCK = BLOCK_SIZE/sizeof(VertexID);

struct MappedReadFile {
  MappedReadFile(const std::filesystem::path &path) {
    fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
      throw std::runtime_error("Failed to open file: " + path.string());
    }
    size = std::filesystem::file_size(path);
    data = static_cast<uint8_t *>(mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
      throw std::runtime_error("Failed to map file: " + path.string());
    }
  }

  ~MappedReadFile() {
    munmap(data, size);
    close(fd);
  }
  
  MappedReadFile(const MappedReadFile &) = delete;
  MappedReadFile &operator=(const MappedReadFile &) = delete;
  MappedReadFile(MappedReadFile &&) = delete;
  MappedReadFile &operator=(MappedReadFile &&) = delete;

  const uint8_t *get_data() const {
    return data;
  }

  uint8_t *data;
  int fd;
  size_t size;
};

struct MappedWriteFile {
  MappedWriteFile(const std::filesystem::path &path, size_t size) {
    fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd < 0) {
      throw std::runtime_error("Failed to open file: " + path.string());
    }
    int ret = ftruncate(fd, size);
    if (ret < 0) {
      throw std::runtime_error("Failed to truncate file: " + path.string());
    }
    this->size = size;
    data = static_cast<uint8_t *>(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
    if (data == MAP_FAILED) {
      throw std::runtime_error("Failed to map file: " + path.string());
    }
  }

  ~MappedWriteFile() {
    munmap(data, size);
    close(fd);
  }
  
  MappedWriteFile(const MappedWriteFile &) = delete;
  MappedWriteFile &operator=(const MappedWriteFile &) = delete;
  MappedWriteFile(MappedWriteFile &&) = delete;
  MappedWriteFile &operator=(MappedWriteFile &&) = delete;

  uint8_t *get_data(){
    return data;
  }

  uint8_t *data;
  int fd;
  size_t size;
};

struct CSRGraph {
  explicit CSRGraph(const path &in_path): graph_path(in_path) {
    vector<uint8_t> csr_meta = load_raw<uint8_t>(meta_path());

    n_nodes = *reinterpret_cast<VertexID *>(&csr_meta[3]);
    n_edges = *reinterpret_cast<EdgeID *>(&csr_meta[3+sizeof(VertexID)]);
    // log_info("#nodes: %llu, #edges: %llu", ull(n_nodes), ull(n_edges));
  }

  std::filesystem::path meta_path()   const { return graph_path/"meta"; }
  std::filesystem::path offset_path() const { return graph_path/"offset"; }
  std::filesystem::path data_path()   const { return graph_path/"data"; }
  std::vector<EdgeID>   load_offset() const { return load_raw<EdgeID>(offset_path()); }
  std::vector<VertexID> load_data()   const { return load_raw<VertexID>(data_path()); }

  path graph_path;
  VertexID n_nodes = 0;
  EdgeID n_edges = 0;
};

std::vector<VertexInfo> initVertexInfo(const std::vector<EdgeID> &offset) {
  std::vector<VertexInfo> vinfo(offset.size() - 1);
  #pragma omp parallel for default(none) shared(vinfo, offset)
  for (VertexID v = 0; v < vinfo.size(); ++v) {
    vinfo[v].offset = 0;
    vinfo[v].deg = offset[v + 1] - offset[v];
    vinfo[v].v = v; }
  #pragma omp barrier
  return vinfo;
}

VertexID preReorder(std::vector<VertexInfo> &vinfo, uint32_t t_deg) {
  size_t lp = 0, hp = 0;
  while (true) {
    while (lp < vinfo.size() && vinfo[lp].deg > t_deg) lp++;
    if (lp >= vinfo.size()) break;
    hp = std::max(lp+1, hp);
    while (hp < vinfo.size() && vinfo[hp].deg <= t_deg) hp++;
    if (hp >= vinfo.size()) break;
    std::swap(vinfo[hp], vinfo[lp]); }
  std::sort(vinfo.begin()+lp, vinfo.end(), [](const auto &v1, const auto &v2) {
    if (v1.deg == v2.deg) { return v1.v < v2.v; }
    return v1.deg > v2.deg; } );
  for (size_t i = 0; i < lp; ++i) {
    if (vinfo[i].deg <= t_deg) {
      abort(); } }
  for (size_t i = lp; i < vinfo.size(); ++i) {
    if (vinfo[i].deg > t_deg) {
      abort(); } }
  return lp;
}

VertexID insertVirtualVertex(std::vector<VertexInfo> &vinfo) {
    std::sort(vinfo.begin(), vinfo.end(), [](const auto &v1, const auto &v2) {
        return v1.offset < v2.offset; } );
    size_t n = vinfo.size();
    VertexID ret = 0;
    for (size_t i = 0; i < n; i++) {
      if (i == n - 1 || vinfo[i+1].offset-vinfo[i].offset != vinfo[i].deg*sizeof(VertexID)) {
        vinfo.emplace_back((vinfo[i].offset+vinfo[i].deg*sizeof(VertexID))|VIRTUAL_BIT, 0, INVALID_V);
        ret++; } }
    std::sort(vinfo.begin(), vinfo.end(), [](const auto &v1, const auto &v2) {
        return (v1.offset & ~VIRTUAL_BIT) < (v2.offset & ~VIRTUAL_BIT); } );
    return ret;
}


void copyAndTransform(VertexID *dst, const VertexID*src, size_t size, const std::vector<VertexID> &v2id) {
  memcpy(dst, src, size*sizeof(VertexID));
  for (size_t i = 0; i < size; ++i) {
    dst[i] = v2id[dst[i]];
  }
}

template <typename P>
void format(const path &in_path, const path &out_path, uint32_t t_deg, P &&partitioner) {
  if (!std::filesystem::exists(out_path)) {
    if (!std::filesystem::create_directory(out_path)) {
      printf("Failed to create output directory: %s\n", out_path.c_str());
      abort(); } }
    
  CSRGraph in_graph(in_path);
  std::vector<EdgeID> offset = in_graph.load_offset();
  VertexID n_rnodes = in_graph.n_nodes;
  EdgeID   n_edges  = in_graph.n_edges;


  Timer timer;
  std::vector<VertexInfo> vinfo = initVertexInfo(offset);
  VertexID t_v = preReorder(vinfo, t_deg);
  std::vector<VertexInfo> mini_vertices(vinfo.begin() + t_v, vinfo.end());
  std::vector<VertexID> lv_nbs(mini_vertices.size()*t_deg, INVALID_V);
  vinfo.erase(vinfo.begin() + t_v, vinfo.end());
  
  BlockID  n_blocks = partitioner(vinfo);
  VertexID n_vnodes = insertVirtualVertex(vinfo);
  VertexID n_nodes  = n_vnodes + n_rnodes;
  for (VertexID id = 0; id < vinfo.size(); ++id) {
    VertexID v = vinfo[id].v;
    if (v != INVALID_V) {
      auto ofs1 = vinfo[id].offset & ~VIRTUAL_BIT;
      auto ofs2 = vinfo[id+1].offset & ~VIRTUAL_BIT;
      if (ofs2 - ofs1 != vinfo[id].deg*sizeof(VertexID)) {
        log_fatal("Vertex %u has invalid offset: %llu, %llu, deg: %u",
                  v, ull(ofs1), ull(ofs2), vinfo[id].deg);
        abort(); } } }
  vinfo.insert(vinfo.end(), mini_vertices.begin(), mini_vertices.end());
  
  std::vector<VertexID> v2id(n_rnodes);
  std::vector<VertexID> id2v(n_nodes);
  for (VertexID id = 0; id < vinfo.size(); ++id) {
    id2v[id] = vinfo[id].v;
    if (vinfo[id].v != INVALID_V) { 
      v2id[vinfo[id].v] = id; } }
  
  VertexID t_id = t_v + n_vnodes;
  std::vector<VertexID> blk_op(n_blocks, INVALID_V);
  for (VertexID id = 0; id < t_id - 1; ++id) {
    BlockID bid = ((vinfo[id+1].offset & ~VIRTUAL_BIT)-1)/BLOCK_SIZE;
    if (blk_op[bid] == INVALID_V) {
      blk_op[bid] = id; } }
  timer.report("partition");
  
  MappedReadFile data_file(in_graph.data_path());
  auto in_data = reinterpret_cast<const VertexID *>(data_file.get_data());
  MappedWriteFile out_file(out_path/"edge_blocks", n_blocks*BLOCK_SIZE);
  uint8_t *out_data = out_file.get_data();

  #pragma omp parallel for
  for (VertexID v = 0; v < n_rnodes; v++) {
    VertexID id = v2id[v];
    VertexID deg  = vinfo[id].deg;
    const VertexID *src = in_data + offset[v];
    VertexID *dst = deg > t_deg ? (VertexID*)(out_data+vinfo[id].offset) : &lv_nbs[(id-t_id)*t_deg];
    copyAndTransform(dst, src, deg, v2id);
  }

  std::vector<EdgeID> vertices;
  vertices.reserve(t_id);
  std::for_each(vinfo.begin(), vinfo.begin()+t_id, [&](auto x) { 
      vertices.push_back(x.offset); });
  save_raw(out_path/"vinfo", vertices);
  save_mate(out_path/"meta", n_nodes, n_edges, n_blocks,
            n_rnodes, n_vnodes, t_id, t_deg);
  save_raw(out_path/"v2id", v2id);
  save_raw(out_path/"id2v", id2v);
  save_raw(out_path/"binfo", blk_op);
  save_raw(out_path/"lv_nbs", lv_nbs); 
  timer.report("copy");
}


int main(int argc, char *argv[]) {
  auto config = parseParameters(argc, argv);
  if (config.degree_packing) {
    if (config.partition_method == SIMPLE) {
      DegreePackingPartitioner<SimplePartitioner> partitioner;
      format(config.in_path, config.out_path, config.tdeg, partitioner); }
    else if (config.partition_method == LPLF) {
      DegreePackingPartitioner<LPLFPartitioner> partitioner(config.max_block_gap);
      format(config.in_path, config.out_path, config.tdeg, partitioner); }
    else if (config.partition_method == BEST_FIT) {
      DegreePackingPartitioner<BestFitPartitioner> partitioner;
      format(config.in_path, config.out_path, config.tdeg, partitioner); }
    else {
      log_fatal("Unknown partition method");
      return -1; } } 
  else {
    if (config.partition_method == SIMPLE) {
      SimplePartitioner partitioner;
      format(config.in_path, config.out_path, config.tdeg, partitioner); }
    else if (config.partition_method == LPLF) {
      LPLFPartitioner partitioner(config.max_block_gap);
      format(config.in_path, config.out_path, config.tdeg, partitioner); }
    else if (config.partition_method == BEST_FIT) {
      BestFitPartitioner partitioner;
      format(config.in_path, config.out_path, config.tdeg, partitioner); }
    else {
      log_fatal("Unknown partition method");
      return -1; } }
  
  return 0;
}
