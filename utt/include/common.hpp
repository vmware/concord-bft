#pragma once

#include "commitment.hpp"
#include "details.hpp"
#include "types.hpp"
#include <memory>
#include <vector>
#include <map>

namespace libutt::api {
class Utils {
 public:
  static types::Signature aggregateSigShares(Details& d,
                                             Commitment::Type t,
                                             uint32_t n,
                                             const std::map<uint32_t, types::Signature>& rsigs,
                                             std::vector<types::CurvePoint> randomness);
};
}  // namespace libutt::api