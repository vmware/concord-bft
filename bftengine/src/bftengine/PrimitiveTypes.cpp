#include "PrimitiveTypes.hpp"
#include <stdexcept>

namespace bftEngine {
namespace impl {

std::string CommitPathToStr(CommitPath path) {
  switch (path) {
    case CommitPath::NA:
      return "NA";
    case CommitPath::OPTIMISTIC_FAST:
      return "OPTIMISTIC_FAST";
    case CommitPath::FAST_WITH_THRESHOLD:
      return "FAST_WITH_THRESHOLD";
    case CommitPath::SLOW:
      return "SLOW";
    default:
      throw std::runtime_error("Unsupported CommitPath specified.");
  }
}

std::string CommitPathToMDCString(CommitPath path) {
  switch (path) {
    case CommitPath::NA:
      return "NA";
    case CommitPath::OPTIMISTIC_FAST:
      return "fast-optimistic";
    case CommitPath::FAST_WITH_THRESHOLD:
      return "fast";
    case CommitPath::SLOW:
      return "slow";
    default:
      return "Unsupported CommitPath";
  }
}

}  // namespace impl
}  // namespace bftEngine
