#include <atomic>

namespace preprocessor {
struct GlobalData {
  // Holds the block id that was last added to storage,
  // Used for the conflict detection optimization.
  static std::atomic_uint64_t current_block_id;
  static std::atomic_uint64_t block_delta;
  const static uint8_t step = 10;
};
}  // namespace preprocessor