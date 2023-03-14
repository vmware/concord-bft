#include "GlobalData.hpp"

namespace preprocessor {

std::atomic_uint64_t GlobalData::current_block_id = 0;
std::atomic_uint64_t GlobalData::block_delta = 0;
std::atomic_uint64_t GlobalData::delta_factor = 60;
std::atomic_bool GlobalData::increment_step = false;
std::atomic_bool GlobalData::decrement_step = false;
}  // namespace preprocessor