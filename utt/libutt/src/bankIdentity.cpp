#include "bankIdentity.hpp"
#include "mint.hpp"
#include "details.hpp"
#include "burn.hpp"
#include <utt/Params.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>
#include <utt/Serialization.h>
#include <utt/MintOp.h>
#include <utt/BurnOp.h>
#include <utt/Coin.h>
#include <vector>

namespace libutt::api {
BankIdentity::BankIdentity(const std::string& id, const std::string& bsk, const std::string& bvk, const std::string& rvk) {
  bid_ = id;
  bsk_.reset(new libutt::RandSigShareSK());
  *bsk_ = libutt::deserialize<libutt::RandSigShareSK>(bsk);
  bvk_.reset(new libutt::RandSigPK());
  *bvk_ = libutt::deserialize<libutt::RandSigPK>(bvk);
  rvk_.reset(new libutt::RegAuthPK());
  *rvk_ = libutt::deserialize<libutt::RegAuthPK>(rvk);
}

const std::string& BankIdentity::getId() const { return bid_; }
template <>
std::vector<uint8_t> BankIdentity::sign<operations::Mint>(operations::Mint& mint) const {
  auto res = mint.op_->shareSignCoin(*bsk_);
  auto res_str = libutt::serialize<libutt::RandSigShare>(res);
  return std::vector<uint8_t>(res_str.begin(), res_str.end());
}

template <>
bool BankIdentity::validate<operations::Burn>(const operations::Burn& burn, std::vector<uint8_t> sig) const {
  (void) sig;
  return burn.burn_->validate(Details::instance().getParams(), *(bvk_), *(rvk_));
}
}  // namespace libutt::api