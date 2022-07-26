#pragma once
#include "coin.hpp"
#include "client.hpp"
#include "globalParams.hpp"
#include "committer.hpp"
#include <memory>
namespace libutt {
class BurnOp;
}
namespace libutt::api::operations {
class Burn {
 public:
  Burn(const GlobalParams& d, const Client& cid, const Coin& c);
  std::string getNullifier() const;

 public:
  friend class libutt::api::Committer;
  friend class libutt::api::Client;
  std::unique_ptr<libutt::BurnOp> burn_;
};
}  // namespace libutt::api::operations