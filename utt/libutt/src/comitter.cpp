#include "committer.hpp"
#include "mint.hpp"
#include "globalParams.hpp"
#include "burn.hpp"
#include "transaction.hpp"
#include "budget.hpp"
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
Committer::Committer(const std::string& id, const std::string& bsk, const std::string& bvk, const std::string& rvk) {
  bid_ = id;
  bsk_.reset(new libutt::RandSigShareSK());
  *bsk_ = libutt::deserialize<libutt::RandSigShareSK>(bsk);
  bvk_.reset(new libutt::RandSigPK());
  *bvk_ = libutt::deserialize<libutt::RandSigPK>(bvk);
  rvk_.reset(new libutt::RegAuthPK());
  *rvk_ = libutt::deserialize<libutt::RegAuthPK>(rvk);
}

const std::string& Committer::getId() const { return bid_; }
template <>
std::vector<types::Signature> Committer::sign<operations::Mint>(operations::Mint& mint) const {
  auto res = mint.op_->shareSignCoin(*bsk_);
  auto res_str = libutt::serialize<libutt::RandSigShare>(res);
  return {types::Signature(res_str.begin(), res_str.end())};
}
template <>
std::vector<types::Signature> Committer::sign<operations::Transaction>(operations::Transaction& tx) const {
  std::vector<types::Signature> sigs;
  auto res = tx.tx_->shareSignCoins(*bsk_);
  for (const auto& [_, sig] : res) {
    auto sig_str = libutt::serialize<libutt::RandSigShare>(sig);
    sigs.push_back(types::Signature(sig_str.begin(), sig_str.end()));
  }
  return sigs;
}

template <>
std::vector<types::Signature> Committer::sign<operations::Budget>(operations::Budget& budget) const {
  std::vector<types::Signature> sigs;
  std::string h1 = budget.getHashHex();
  G1 H = hashToGroup<G1>("ps16base|" + h1);
  Fr pidHash;
  Fr sn;
  Fr val;
  Fr type = libutt::Coin::BudgetType();
  Fr exp_date;
  auto& bcoin = budget.getCoin();
  pidHash.from_words(bcoin.getPidHash());
  sn.from_words(bcoin.getSN());
  val.set_ulong(bcoin.getVal());
  exp_date.from_words(bcoin.getExpDateAsCurvePoint());
  Comm icm = (pidHash * H);  // H^pidHash g^0
  Comm scm(sn * H);          // H^sn g^0
  Comm vcm = (val * H);      // H^value g^0
  Comm tcm(type * H);        // H^type g^0
  Comm dcm(exp_date * H);    // H^exp_date g^0

  auto sig = bsk_->shareSign({icm, scm, vcm, tcm, dcm}, H);
  auto sig_str = libutt::serialize<libutt::RandSigShare>(sig);
  return {types::Signature(sig_str.begin(), sig_str.end())};
}

template <>
bool Committer::validate<operations::Burn>(const operations::Burn& burn, const types::Signature& sig) const {
  (void)sig;
  return burn.burn_->validate(GlobalParams::instance().getParams(), *(bvk_), *(rvk_));
}
template <>
bool Committer::validate<operations::Transaction>(const operations::Transaction& tx,
                                                  const types::Signature& sig) const {
  (void)sig;
  return tx.tx_->validate(GlobalParams::instance().getParams(), *(bvk_), *(rvk_));
}
}  // namespace libutt::api