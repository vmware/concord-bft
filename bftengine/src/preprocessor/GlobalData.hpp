#include <atomic>

namespace preprocessor {
struct GlobalData {
  // Holds the block id that was last added to storage,
  // Used for the conflict detection optimization.
  static std::atomic_uint64_t current_block_id;
};
}  // namespace preprocessor