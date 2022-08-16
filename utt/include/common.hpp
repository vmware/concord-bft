#pragma once

#include "commitment.hpp"
#include "globalParams.hpp"
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
  static types::Signature aggregateSigShares(uint32_t n, const std::map<uint32_t, types::Signature>& rsigs);
  static types::Signature unblindSignature(const GlobalParams& p,
                                           Commitment::Type,
                                           const std::vector<types::CurvePoint>& randomness,
                                           const types::Signature& sig);
};
}  // namespace libutt::api