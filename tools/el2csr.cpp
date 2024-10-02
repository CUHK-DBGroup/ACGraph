//
// Created by Dechuang CHEN on 2/9/2024.
//

#include <cassert>
#include <filesystem>
#include <iostream>

#include "pigo.hpp"

using namespace std;
using namespace pigo;

enum OUTPUT_TYPE { CSR_TYPE, EDGELIST_TYPE };

using UWCSR = CSR<uint32_t, uint64_t, uint32_t*, uint64_t*, false, float, float*>;

template <class L, class O, class LS, class OS, bool wgt, class W, class WS>
size_t save_size(const CSR<L, O, LS, OS, wgt, W, WS>& g) {
  size_t out_size = 0;
  // Find the template sizes
  out_size += sizeof(uint8_t) * 2;
  // Find the size of the size of the CSR
  out_size += sizeof(L) + sizeof(O);
  // Finally, find the actual CSR size
  size_t voff_size = sizeof(O) * (g.n() + 1);
  size_t vend_size = sizeof(L) * g.m();
  size_t w_size = detail::weight_size_<wgt, W, O>(g.m());
  out_size += voff_size + vend_size + w_size;

  return out_size;
}

template <class L, class O, class LS, class OS, bool wgt, class W, class WS>
void save(CSR<L, O, LS, OS, wgt, W, WS>& g, File& w) {
  // Output the template sizes
  uint8_t L_size = sizeof(L);
  uint8_t O_size = sizeof(O);
  w.write(L_size);
  w.write(O_size);

  // Output the sizes and data
  w.write(g.n());
  w.write(g.m());

  size_t voff_size = sizeof(O) * (g.n() + 1);
  size_t vend_size = sizeof(L) * g.m();
  size_t w_size = detail::weight_size_<wgt, W, O>(g.m());

  // Output the data
  char* voff = detail::get_raw_data_<OS>(g.offsets());
  w.parallel_write(voff, voff_size);

  char* vend = detail::get_raw_data_<LS>(g.endpoints());
  w.parallel_write(vend, vend_size);

  if (w_size > 0) {
    char* wend = detail::get_raw_data_<WS>(g.weights());
    w.parallel_write(wend, w_size);
  }
}

template <class L, class O, class LS, class OS, bool wgt, class W, class WS>
void save(CSR<L, O, LS, OS, wgt, W, WS>& g, const std::string& fn) {
  // Before creating the file, we need to find the size
  std::filesystem::path root_path(fn);
  if (!filesystem::exists(root_path)) {
    assert(std::filesystem::create_directory(root_path));
  }

  size_t meta_size =
      sizeof(uint8_t) * 3 + sizeof(L) + sizeof(O);  // meta: sizeof(L) sizeof(O) sizeof(W) n m
  WFile meta_file{root_path / "meta", meta_size};
  meta_file.write(static_cast<uint8_t>(sizeof(L)));
  meta_file.write(static_cast<uint8_t>(sizeof(O)));
  if (wgt) {
    meta_file.write(static_cast<uint8_t>(sizeof(L)));
  } else {
    meta_file.write(static_cast<uint8_t>(0));
  }
  meta_file.write(g.n());
  meta_file.write(g.m());

  size_t voff_size = sizeof(O) * (g.n() + 1);
  WFile voff_file{root_path / "offset", voff_size};
  char* voff = detail::get_raw_data_<OS>(g.offsets());
  voff_file.parallel_write(voff, voff_size);

  size_t vend_size = sizeof(L) * g.m();
  WFile vend_file{root_path / "data", vend_size};
  char* vend = detail::get_raw_data_<LS>(g.endpoints());
  vend_file.parallel_write(vend, vend_size);

  size_t w_size = detail::weight_size_<wgt, W, O>(g.m());
  if (w_size > 0) {
    WFile weight_file{root_path / "weight", w_size};
    char* wend = detail::get_raw_data_<WS>(g.weights());
    weight_file.parallel_write(wend, w_size);
  }
}

template <class L, class O, class LS, class OS, bool wgt, class W, class WS>
void save_as_el(CSR<L, O, LS, OS, wgt, W, WS>& g, const std::string& fn) {
  uint64_t m = g.m();
  uint64_t n = g.n();
  uint64_t edge_unit = sizeof(L) * 2;
  if (wgt) {
    edge_unit += sizeof(W);
  }
  uint64_t size = m * edge_unit;
  WFile w{fn, size};

  char* buffer = new char[size];
#pragma omp parallel for schedule(dynamic)
  for (L i = 0; i < n; i++) {
    auto op = g.offsets()[i];
    auto ed = g.offsets()[i + 1];
    for (auto j = op; j < ed; j++) {
      *((L*)&buffer[j * edge_unit]) = i;
      *((L*)&buffer[j * edge_unit + sizeof(L)]) = g.endpoints()[j];
      if (wgt) {
        *((W*)&buffer[j * edge_unit + sizeof(L) + sizeof(L)]) = g.weights()[j];
      }
    }
  }
#pragma omp barrier
  w.parallel_write(buffer, size);
}

template <class L, class O, class S, bool sym, bool ut, bool sl, bool wgt, class W, class WS>
void save(COO<L, O, S, sym, ut, sl, wgt, W, WS>& coo, std::string fn) {
  auto n = coo.n();
  auto m = coo.m();
  size_t out_size = sizeof(L) * m * 2;
  size_t w_size = detail::weight_size_<wgt, W, O>(m);

  out_size += w_size;

  WFile w{fn, out_size};

  // Output the data
  char* vx = detail::get_raw_data_<S>(coo.x());
  size_t vx_size = sizeof(L) * m;
  w.parallel_write(vx, vx_size);

  char* vy = detail::get_raw_data_<S>(coo.y());
  size_t vy_size = sizeof(L) * m;
  w.parallel_write(vy, vy_size);

  if (w_size > 0) {
    char* vw = detail::get_raw_data_<WS>(coo.w());
    w.parallel_write(vw, w_size);
  }
}

template <bool directed>
void convert2csr(const std::string& input_file, const std::string& output_dir, bool transpose) {
  COO<uint32_t, uint64_t, uint32_t*, !directed, false, true> coo{input_file, EDGE_LIST};
  std::cout << "coo.n(): " << coo.n() << ", coo.m(): " << coo.m() << std::endl;
  if (transpose) coo.transpose();
  UWCSR g{coo};
  g = g.new_csr_without_dups();
  save(g, output_dir);
}

template <bool directed>
void convert2edgelist(const std::string& input_file, const std::string& output_dir, bool transpose) {
  COO<uint32_t, uint64_t, uint32_t*, !directed, false, true> coo{input_file, EDGE_LIST};
  std::cout << "coo.n(): " << coo.n() << ", coo.m(): " << coo.m() << std::endl;
  if (transpose) coo.transpose();
  UWCSR g{coo};
  g = g.new_csr_without_dups();
  save_as_el(g, output_dir);
}

const char* USAGE_STRING =
    "Usage: %s <input_file> <output_dir> \n"
    "[-u] undirected\n"
    "[-t] transpose\n"
    "[-ot <csr|el>] output type (default: csr)";
    

int main(int argc, char** argv) {
  if (argc < 3) {
    cerr << USAGE_STRING << endl;
    return 1;
  }

  filesystem::path work_path(argv[1]);
  filesystem::path out_dir(argv[2]);

  // CSR<uint32_t, uint64_t, uint32_t*, uint64_t*, false, float, float*> csr{work_path};
  // COO<uint32_t, uint64_t, uint32_t*, false, false, true> coo{csr};
  
  bool directed = true;
  bool transpose = false;
  OUTPUT_TYPE output_type = CSR_TYPE;
  for (int i = 3; i < argc; i++) {
    if (string(argv[i]) == "-u") {
      directed = false;
    } else if (string(argv[i]) == "-t") {
      transpose = true;
    } else if (string(argv[i]) == "-ot") {
      auto ot = string(argv[++i]);
      if (ot == "csr") {
        output_type = CSR_TYPE;
      } else if (ot == "el") {
        output_type = EDGELIST_TYPE;
      } else {
        cerr << "Invalid output type: " << ot << endl;
        cerr << USAGE_STRING << endl;
        return 1;
      }
    } else {
      cerr << "Invalid argument: " << argv[i] << endl;
      cerr << USAGE_STRING << endl;
      return 1;
    }
  }

  if (!directed) {
    if (output_type == CSR_TYPE) {
      convert2csr<false>(work_path, out_dir, transpose);
    } else {
      convert2edgelist<false>(work_path, out_dir, transpose);
    }
  } else {
    if (output_type == CSR_TYPE) {
      convert2csr<true>(work_path, out_dir, transpose);
    } else {
      convert2edgelist<true>(work_path, out_dir, transpose);
    }
  }

  return 0;
}