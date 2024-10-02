#ifndef ACG_META_DATA_HPP
#define ACG_META_DATA_HPP

#include <string>

#include "io/file.hpp"
#include "log.h"
#include "util.hpp"

namespace acg {

template <typename VID = uint32_t, typename EID = uint64_t, typename BID = EID, bool wgt = false,
          typename WID = float>
struct MetaData {
  uint32_t vt_size{}, et_size{}, bt_size{}, wt_size{};
  VID n_nodes{};
  EID n_edges{};
  BID n_blocks{};

  VID n_rnodes{}, n_vnodes{};
  VID id_bound{}, deg_bound{};

  void parse_and_set(std::string_view s) {
    auto sep = s.find(':');
    assert(sep < s.size());
    auto name = trim(s.substr(0, sep));
    auto val = std::string(trim(s.substr(sep + 1)));
    assert(!name.empty() && !val.empty());

    std::stringstream ss(val);
    if (name == "vt") {
      ss >> vt_size; } 
    else if (name == "et") {
      ss >> et_size; } 
    else if (name == "bt") {
      ss >> bt_size; } 
    else if (name == "wt")
      ss >> wt_size;
    else if (name == "n_nodes")
      ss >> n_nodes;
    else if (name == "n_edges")
      ss >> n_edges;
    else if (name == "n_blocks")
      ss >> n_blocks;
    else if (name == "n_rnodes")
      ss >> n_rnodes;
    else if (name == "n_vnodes")
      ss >> n_vnodes;
    else if (name == "id_bound")
      ss >> id_bound;
    else if (name == "deg_bound")
      ss >> deg_bound;
    else {
      assert(false);
    }
  }

  [[nodiscard]] std::string to_string() const {
    return "vt: " + std::to_string(vt_size) + ", \n" + "et: " + std::to_string(et_size) + ", \n" +
           "bt: " + std::to_string(bt_size) + ", \n" + "wt: " + std::to_string(wt_size) + ", \n" +
           "n_nodes: " + std::to_string(n_nodes) + ", \n" + "n_edges: " + std::to_string(n_edges) +
           ", \n" + "n_blocks: " + std::to_string(n_blocks) + ", \n" +
           "n_rnodes: " + std::to_string(n_rnodes) + ", \n" +
           "n_vnodes: " + std::to_string(n_vnodes) + ", \n" +
           "id_bound: " + std::to_string(id_bound) + ", \n" +
           "deg_bound: " + std::to_string(deg_bound);
    //    return std::format("vt: {}, et: {}, bt: {}, wt: {},\n"
    //                       "n_nodes: {}, n_edges: {}, n_blocks: {},\n"
    //                       "n_rnodes: {}, n_vnodes: {},\n"
    //                       "id_bound: {}, deg_bound: {}\n",
    //                       vt_size, et_size, bt_size, wt_size,
    //                       n_nodes, n_edges, n_blocks,
    //                       n_rnodes, n_vnodes,
    //                       id_bound, deg_bound);
  }

  static MetaData parse_string(std::string_view s) {
    MetaData meta;

    size_t op = 0;
    size_t ed = s.find(',');
    while (ed != std::string::npos) {
      auto w = s.substr(op, ed - op);
      meta.parse_and_set(w);
      op = ed + 1;
      ed = s.find(',', op);
    }
    meta.parse_and_set(s.substr(op));

    assert(meta.vt_size == sizeof(VID));
    assert(meta.et_size == sizeof(EID));
    assert(meta.bt_size == sizeof(BID));
    assert(meta.n_nodes > 0);
    assert(meta.n_edges > 0);
    assert(meta.n_blocks > 0);

    if (meta.n_rnodes != 0 || meta.n_vnodes != 0) {
      assert(meta.n_rnodes != 0);
      assert(meta.n_vnodes != 0);
      assert(meta.deg_bound <= 4);
      assert(meta.n_rnodes + meta.n_vnodes == meta.n_nodes);
      assert(meta.id_bound < meta.n_nodes);
    }

    return meta;
  }
};

template <typename VID = uint32_t, typename EID = uint64_t, typename BID = EID, bool wgt = false,
          typename WID = float>
static MetaData<VID, EID, BID, wgt, WID> load_meta(const std::string &path) {
  auto data = load_raw<char>(path);
  std::string_view s(data.data(), data.size());
  auto meta = MetaData<VID, EID, BID, wgt, WID>::parse_string(s);
  return meta;
}

template <bool wgt = false, typename WID = float, typename VID, typename EID, typename BID>
static void save_mate(const std::string &path, VID n_nodes, EID n_edges, BID n_blocks,
                      VID n_rnodes = 0, VID n_vnodes = 0, VID id_bound = 0, VID deg_bound = 0) {
  uint8_t vt = sizeof(VID), et_size = sizeof(EID), bt_size = sizeof(BID), wt_size = 0;
  if constexpr (wgt) {
    wt_size = sizeof(WID);
  }
  MetaData<VID, EID, BID, wgt, WID> meta{vt,       et_size,  bt_size,  wt_size,  n_nodes,  n_edges,
                                         n_blocks, n_rnodes, n_vnodes, id_bound, deg_bound};
  auto s = meta.to_string();
  std::vector<char> data(s.begin(), s.end());
  save_raw(path, data);
}

}  // namespace acg
#endif  // ACG_META_DATA_HPP
