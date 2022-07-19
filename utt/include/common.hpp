#pragma once

#include "commitment.hpp"
#include "details.hpp"
#include <memory>
#include <vector>
#include <map>

namespace libutt::api {
struct RegistrationDetails {
  Commitment rcm_;
  std::vector<uint8_t> rcm_sig_;
  std::vector<uint8_t> dsk_pub_;
  std::vector<uint8_t> dsk_priv_;
};

class Utils {
 public:
  static std::vector<uint8_t> aggregateSigShares(Details& d,
                                                 Commitment::Type t,
                                                 uint32_t n,
                                                 const std::map<uint32_t, std::vector<uint8_t>>& rsigs,
                                                 std::vector<std::vector<uint64_t>> randomness);
};
}  // namespace libutt::api