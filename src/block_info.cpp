#include "block_info.hpp"

namespace acg {

template struct BlockInfo<uint32_t, INCREASING, 1>;
template struct BlockInfo<uint32_t, DECREASING, 1>;
template struct BlockInfo<uint32_t, INCREASING, 2>;
template struct BlockInfo<uint32_t, DECREASING, 2>;

}  // namespace acg
