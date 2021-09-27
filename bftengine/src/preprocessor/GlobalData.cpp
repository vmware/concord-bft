#include "GlobalData.hpp"

namespace preprocessor {

std::atomic_uint64_t GlobalData::current_block_id = 0;
}  // namespace preprocessor