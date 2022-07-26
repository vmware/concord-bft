#pragma once
#include <string>
#include <memory>
#include <vector>
#include "coin.hpp"
#include "clientIdentity.hpp"
#include "details.hpp"
namespace libutt {
class MintOp;
}  // namespace libutt
namespace libutt::api {
class BankIdentity;
}
namespace libutt::api::operations {

class Mint {
 public:
  Mint(const std::string& uniqueHash, size_t value, const std::string& recipPID);
  bool validate(const std::string& uniqueHash, size_t value, const std::string& recipPID) const;
  Coin claimCoin(Details& d, ClientIdentity& cid, uint32_t n, const std::map<uint32_t, types::Signature>& rsigs) const;

 private:
  friend class libutt::api::BankIdentity;
  std::unique_ptr<libutt::MintOp> op_;
};
}  // namespace libutt::api::operations