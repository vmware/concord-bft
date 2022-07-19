#pragma once
#include "details.hpp"
#include <vector>

namespace libutt {
class Nullifier;
}
namespace libutt::api {
class Nullifier {
 public:
  Nullifier(Details& d,
            const std::vector<uint64_t>& prf_secret_key,
            const std::vector<uint64_t>& sn,
            const std::vector<uint64_t>& randomization);
  bool validate(Details& d) const;
  std::string toString() const;

 private:
  std::unique_ptr<libutt::Nullifier> nullifier_;
};
}  // namespace libutt::api
