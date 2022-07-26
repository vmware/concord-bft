#pragma once
#include "details.hpp"
#include "types.hpp"
#include <vector>
#include <cstdint>
namespace libutt {
class Comm;
class CommKey;
}  // namespace libutt
namespace libutt::api {
class Commitment;
}
libutt::api::Commitment operator+(libutt::api::Commitment lhs, const libutt::api::Commitment& rhs);
namespace libutt::api {
class RegistratorIdentity;
class ClientIdentity;
namespace operations {
class Burn;
class Transaction;
}  // namespace operations
class Commitment {
 public:
  enum Type { REGISTRATION = 0, VALUE, COIN };
  static libutt::CommKey& getCommitmentKey(Details& d, Type t);
  Commitment(Details& d, Type t, const std::vector<types::CurvePoint>& messages, bool withG2);
  Commitment(const std::string& comm);
  Commitment(const Commitment& comm);
  Commitment();
  Commitment& operator=(const Commitment&);
  Commitment& operator+=(const Commitment&);
  types::CurvePoint randomize(Details& d, Type t);
  static size_t getCommitmentSn(const Commitment& comm);
  static std::string getCommitmentHash(const Commitment& comm);

 private:
  friend class RegistratorIdentity;
  friend class ClientIdentity;
  friend class operations::Burn;
  friend class operations::Transaction;
  std::unique_ptr<libutt::Comm> comm_;
};
}  // namespace libutt::api