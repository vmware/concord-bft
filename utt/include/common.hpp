#pragma once

#include "commitment.hpp"
#include "details.hpp"
#include "types.hpp"
#include <memory>
#include <vector>
#include <map>

namespace libutt {
class CommKey;
}
namespace libutt::api {
class Utils {
 public:
  static CommKey& getCommitmentKey(Details& d, Commitment::Type t);
  static types::Signature aggregateSigShares(Details& d,
                                             Commitment::Type t,
                                             uint32_t n,
                                             const std::map<uint32_t, types::Signature>& rsigs,
                                             const std::vector<types::CurvePoint>& randomness);
};
}  // namespace libutt::api