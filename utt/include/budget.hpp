#pragma once
#include "coin.hpp"
#include "client.hpp"
#include "globalParams.hpp"
#include "types.hpp"
#include <string>
namespace libutt::api::operations {
class Budget {
 public:
  Budget(const GlobalParams& d, const libutt::api::Client& cid, uint64_t val, const std::string& exp_date);
  Budget(const GlobalParams& d, const types::CurvePoint& pidHash, uint64_t val, const std::string& exp_date);
  libutt::api::Coin& getCoin();
  const libutt::api::Coin& getCoin() const;
  std::string getHashHex() const;

 private:
  libutt::api::Coin coin_;
};
}  // namespace libutt::api::operations
