#pragma once
#include "details.hpp"
#include <vector>
#include <cstdint>
namespace libutt {
class Comm;
}
namespace libutt::api {
class Commitment;
}
libutt::api::Commitment operator+(libutt::api::Commitment lhs, const libutt::api::Commitment& rhs);
namespace libutt::api {
class RegistratorIdentity;
class ClientIdentity;
namespace operations {
class Burn;
}
class Commitment {
 public:
  enum Type { REGISTRATION = 0, VALUE, COIN };
  static std::vector<Commitment> create(Details& d,
                                        Type t,
                                        const std::vector<std::vector<uint64_t>>& messages,
                                        std::vector<std::vector<uint64_t>>& randomizations,
                                        bool withG2);
  Commitment(Details& d, Type t, const std::vector<std::vector<uint64_t>>& messages, bool withG2);
  Commitment(const std::string& comm);
  Commitment(const Commitment& comm);
  Commitment();
  Commitment& operator=(const Commitment&);
  Commitment& operator+=(const Commitment&);
  std::vector<uint64_t> randomize(Details& d, Type t);
  static size_t getCommitmentSn(const Commitment& comm);
  static std::string getCommitmentHash(const Commitment& comm);

 private:
  friend class RegistratorIdentity;
  friend class ClientIdentity;
  friend class operations::Burn;
  std::shared_ptr<libutt::Comm> comm_;
};
}  // namespace libutt::api