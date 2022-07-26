#pragma once
#include "details.hpp"
#include "types.hpp"
#include <vector>

namespace libutt {
class Nullifier;
}
namespace libutt::api {
class Nullifier {
 public:
  Nullifier(Details& d,
            const types::CurvePoint& prf_secret_key,
            const types::CurvePoint& sn,
            const types::CurvePoint& randomization);
  bool validate(Details& d) const;
  std::string toString() const;

 private:
  std::unique_ptr<libutt::Nullifier> nullifier_;
};
}  // namespace libutt::api
