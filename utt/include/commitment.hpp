#pragma once
#include "globalParams.hpp"
#include "types.hpp"
#include <vector>
#include <cstdint>
#include <optional>

namespace libutt {
class Comm;
class CommKey;
}  // namespace libutt
namespace libutt::api {
class Commitment;
}
libutt::api::Commitment operator+(libutt::api::Commitment lhs, const libutt::api::Commitment& rhs);
namespace libutt::api {
class Registrator;
class Client;
namespace operations {
class Burn;
class Transaction;
}  // namespace operations
class Commitment {
 public:
  enum Type { REGISTRATION = 0, COIN };
  static const libutt::CommKey& getCommitmentKey(const GlobalParams& d, Type t);
  Commitment(const GlobalParams& d, Type t, const std::vector<types::CurvePoint>& messages, bool withG2);
  Commitment(const Commitment& comm);
  Commitment();
  Commitment& operator=(const Commitment&);
  Commitment& operator+=(const Commitment&);
  types::CurvePoint rerandomize(const GlobalParams& d, Type t, std::optional<types::CurvePoint> base_randomness);

 private:
  friend class Registrator;
  friend class Client;
  friend class operations::Burn;
  friend class operations::Transaction;
  std::unique_ptr<libutt::Comm> comm_;
};
}  // namespace libutt::api