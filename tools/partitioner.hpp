#include <fcntl.h>
#include <unistd.h>
#include <map>
#include <span>
#include <optional>

#include "types.hpp"
#include "segment_tree.hpp"


struct VertexInfo {
  uint64_t offset;
  uint32_t deg;
  VertexID v;
};


template<typename F, typename U>
uint64_t _partition(F&& find, U&& update, std::span<VertexInfo> vinfo) {
  uint64_t blk_num = 0;
  for (auto & [ofs, deg, v] : vinfo) {
    uint64_t required_space = deg * sizeof(VertexID);
    auto blk = find(required_space);
    if (blk) {
      auto [bid, free_space] = blk.value();
      ofs = (bid + 1) * BLOCK_SIZE - free_space;
      update(bid, free_space - required_space);
    } else {
      auto required_blk_num = (required_space + BLOCK_SIZE - 1) / BLOCK_SIZE;
      ofs = blk_num * BLOCK_SIZE;
      blk_num += required_blk_num;
      update(blk_num - 1, required_blk_num * BLOCK_SIZE - required_space);
    }
  }
  return blk_num;
}


struct SimplePartitioner {
  uint64_t operator()(std::span<VertexInfo> vinfo) {
    BlockID cur_blk = 0;
    BlockID free_space = 0;
    auto find = [&](uint32_t required_space) -> std::optional<std::pair<BlockID, uint32_t>> {
      if (required_space > free_space) return std::nullopt;
      return std::make_optional(std::make_pair(cur_blk, free_space)); 
    };
    auto update = [&](BlockID bid, uint32_t val) {
      cur_blk = bid;
      free_space = val;
    };
    return _partition(find, update, vinfo);
  }
};

struct LPLFPartitioner {
  LPLFPartitioner(uint32_t k = 16) : k(k) { }

  uint64_t operator()(std::span<VertexInfo> vinfo) {
    std::map<BlockID, uint32_t> blks;
    BlockID cur_blk = 0;
    
    auto find = [&blks](uint32_t required_space) -> std::optional<std::pair<BlockID, uint32_t>> {
      for (auto it = blks.rbegin(); it != blks.rend(); ++it) {
        if (it->second >= required_space) {
          return std::make_optional(std::make_pair(it->first, it->second)); } }
      return std::nullopt;
    };
    auto update = [this, &blks, &cur_blk](BlockID bid, uint32_t val) {    
      cur_blk = std::max(cur_blk, bid);
      if (val == 0) { blks.erase(bid); } 
      else {
        blks[bid] = val;
        while (!blks.empty() && blks.begin()->first + k < cur_blk) {
          blks.erase(blks.begin()); } }
    };
    return _partition(find, update, vinfo);
  }

  uint32_t k;
};


struct BestFitPartitioner {
  uint64_t operator()(std::span<VertexInfo> vinfo) {
    std::map<uint32_t, BlockID> blks;
    auto find = [&blks](uint32_t required_space) -> std::optional<std::pair<BlockID, uint32_t>> {
      auto it = blks.lower_bound(required_space);
      if (it != blks.end()) {
        auto ret = std::optional(std::make_pair(it->second, it->first));
        blks.erase(it);
        return ret; }
      return std::nullopt; 
    };
    auto update = [&blks](BlockID bid, uint32_t val) {
      blks[val] = bid;
    };
    return _partition(find, update, vinfo);
  }
};


template <typename T>
struct DegreePackingPartitioner : public T {
  template<typename... Args>
  explicit DegreePackingPartitioner(Args&&... args) : T(std::forward<Args>(args)...) {}

  BlockID operator()(std::span<VertexInfo> vinfo) {
    std::ranges::sort(vinfo, [](const VertexInfo &a, const VertexInfo &b) {
      if (a.deg == b.deg) return a.v < b.v;
      return a.deg > b.deg;
    });
    return T::operator()(vinfo);
  }
};

template <typename P>
struct ParallelPartitioner {
  template<typename... Args>
  explicit ParallelPartitioner(Args&&... args) : partitioner(std::forward<Args>(args)...) {}

  uint64_t operator()(std::span<VertexInfo> vinfo) {
    int num_threads = omp_get_num_threads();
    std::vector<EdgeID> global_offset(num_threads);
    #pragma omp parallel shared(vinfo, num_threads, global_offset)
    {
      uint64_t batch_size = (vinfo.size() + num_threads - 1) / num_threads;
      int tid = omp_get_thread_num();

      uint64_t op = tid * batch_size;
      uint64_t ed = std::min((tid + 1) * batch_size, vinfo.size());
      auto sub_vinfo= vinfo.subspan(op, ed - op);
      global_offset[tid] = partitioner(sub_vinfo);
      #pragma omp barrier

      #pragma omp single
      {
        for (int i = 1; i < num_threads; i++) {
          global_offset[i] += global_offset[i - 1]; }
      }
      #pragma omp barrier

      if (tid != 0) {
        uint64_t oft = global_offset[tid-1] * BLOCK_SIZE;
        std::ranges::for_each(sub_vinfo, [oft](VertexInfo &vi) {
          vi.offset += oft; }); }
    }
    #pragma omp barrier
    return global_offset[num_threads-1];
  }

  P partitioner;
};