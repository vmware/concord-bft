#include "bankIdentity.hpp"
#include "mint.hpp"
#include <utt/Params.h>
#include <utt/RandSig.h>
#include <utt/Serialization.h>
#include <utt/MintOp.h>
#include <vector>

namespace libutt::api {
BankIdentity::BankIdentity(const std::string& id, const std::string& bsk) {
  bid_ = id;
  bsk_.reset(new libutt::RandSigShareSK());
  *bsk_ = libutt::deserialize<libutt::RandSigShareSK>(bsk);
}

const std::string& BankIdentity::getId() const { return bid_; }
template <>
std::vector<uint8_t> BankIdentity::sign<operations::Mint>(operations::Mint& mint) const {
  auto res = mint.op_->shareSignCoin(*bsk_);
  auto res_str = libutt::serialize<libutt::RandSigShare>(res);
  return std::vector<uint8_t>(res_str.begin(), res_str.end());
}
}  // namespace libutt::api