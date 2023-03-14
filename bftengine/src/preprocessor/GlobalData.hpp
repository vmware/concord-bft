#include <atomic>

namespace preprocessor {
struct GlobalData {
  // Holds the block id that was last added to storage,
  // Used for the conflict detection optimization.
  static std::atomic_uint64_t current_block_id;
  static std::atomic_uint64_t block_delta;
  static std::atomic_uint64_t delta_factor;
  static std::atomic_bool increment_step;
  static std::atomic_bool decrement_step;
  const static uint8_t step = 10;
};
}  // namespace preprocessor