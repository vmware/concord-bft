#pragma once
#include <string>
#include <memory>
#include <vector>
#include "coin.hpp"
#include "details.hpp"
namespace libutt {
class MintOp;
}  // namespace libutt
namespace libutt::api {
class BankIdentity;
class ClientIdentity;
}
namespace libutt::api::operations {

class Mint {
 public:
  Mint(const std::string& uniqueHash, size_t value, const std::string& recipPID);
  bool validate(const std::string& uniqueHash, size_t value, const std::string& recipPID) const;

 private:
  friend class libutt::api::BankIdentity;
  friend class libutt::api::ClientIdentity;
  std::unique_ptr<libutt::MintOp> op_;
};
}  // namespace libutt::api::operations