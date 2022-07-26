#include "bankIdentity.hpp"
#include "mint.hpp"
#include "details.hpp"
#include "burn.hpp"
#include "transaction.hpp"
#include <utt/Params.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>
#include <utt/Serialization.h>
#include <utt/MintOp.h>
#include <utt/BurnOp.h>
#include <utt/Coin.h>
#include <utt/Tx.h>
#include <vector>

namespace libutt::api {
BankIdentity::BankIdentity(const std::string& id,
                           const std::string& bsk,
                           const std::string& bvk,
                           const std::string& rvk) {
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
std::vector<types::Signature> BankIdentity::sign<operations::Mint>(operations::Mint& mint) const {
  auto res = mint.op_->shareSignCoin(*bsk_);
  auto res_str = libutt::serialize<libutt::RandSigShare>(res);
  return {types::Signature(res_str.begin(), res_str.end())};
}
template <>
std::vector<types::Signature> BankIdentity::sign<operations::Transaction>(operations::Transaction& tx) const {
  std::vector<types::Signature> sigs;
  auto res = tx.tx_->shareSignCoins(*bsk_);
  for (const auto& [_, sig] : res) {
      auto sig_str = libutt::serialize<libutt::RandSigShare>(sig);
      sigs.push_back(types::Signature(sig_str.begin(), sig_str.end()));
  }
  return sigs;
}

template <>
bool BankIdentity::validate<operations::Burn>(const operations::Burn& burn, const types::Signature& sig) const {
  (void)sig;
  return burn.burn_->validate(Details::instance().getParams(), *(bvk_), *(rvk_));
}
template <>
bool BankIdentity::validate<operations::Transaction>(const operations::Transaction& tx, const types::Signature& sig) const {
  (void)sig;
  return tx.tx_->validate(Details::instance().getParams(), *(bvk_), *(rvk_));
}
}  // namespace libutt::api