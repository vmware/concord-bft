#pragma once
#include <string>
#include <memory>
#include <vector>
#include "coin.hpp"
#include "globalParams.hpp"
namespace libutt {
class MintOp;
}  // namespace libutt
namespace libutt::api {
class CoinsSigner;
class Client;
}  // namespace libutt::api
namespace libutt::api::operations {

class Mint {
 public:
  Mint(const std::string& uniqueHash, size_t value, const std::string& recipPID);
  std::string getHash() const;
  uint64_t getVal() const;
  std::string getRecipentID() const;

 private:
  friend class libutt::api::CoinsSigner;
  friend class libutt::api::Client;
  std::unique_ptr<libutt::MintOp> op_;
};
}  // namespace libutt::api::operations