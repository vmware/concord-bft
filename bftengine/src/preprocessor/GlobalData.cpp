#include "GlobalData.hpp"

namespace preprocessor {

std::atomic_uint64_t GlobalData::current_block_id = 0;
std::atomic_uint64_t GlobalData::block_delta = 0;
}  // namespace preprocessor