#include "PrimitiveTypes.hpp"

namespace bftEngine {
namespace impl {

std::string CommitPathToStr(CommitPath path) {
  switch(path) {
    case CommitPath::NA:
      return "NA";
    case CommitPath::OPTIMISTIC_FAST:
      return "OPTIMISTIC_FAST";
    case CommitPath::FAST_WITH_THRESHOLD:
      return "FAST_WITH_THRESHOLD";
    case CommitPath::SLOW:
      return "SLOW";
  }
}

} // namespace impl
} // namespace bftEngine
