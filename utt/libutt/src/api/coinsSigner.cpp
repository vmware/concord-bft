#include "coinsSigner.hpp"
#include "mint.hpp"
#include "UTTParams.hpp"
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
CoinsSigner::CoinsSigner(uint16_t id,
                         const std::string& bsk,
                         const std::string& bvk,
                         const std::map<uint16_t, std::string>& shares_verification_keys,
                         const std::string& rvk) {
  bid_ = id;
  bsk_.reset(new libutt::RandSigShareSK());
  *bsk_ = libutt::deserialize<libutt::RandSigShareSK>(bsk);
  bvk_.reset(new libutt::RandSigPK());
  *bvk_ = libutt::deserialize<libutt::RandSigPK>(bvk);
  rvk_.reset(new libutt::RegAuthPK());
  *rvk_ = libutt::deserialize<libutt::RegAuthPK>(rvk);
  for (const auto& [id, svk] : shares_verification_keys) {
    shares_verification_keys_[id].reset(new libutt::RandSigSharePK());
    *(shares_verification_keys_[id]) = libutt::deserialize<libutt::RandSigSharePK>(svk);
  }
}

uint16_t CoinsSigner::getId() const { return bid_; }
template <>
std::vector<types::Signature> CoinsSigner::sign<libutt::MintOp>(const libutt::MintOp& mint) const {
  auto res = mint.shareSignCoin(*bsk_);
  auto res_str = libutt::serialize<libutt::RandSigShare>(res);
  return {types::Signature(res_str.begin(), res_str.end())};
}

template <>
std::vector<types::Signature> CoinsSigner::sign<operations::Mint>(const operations::Mint& mint) const {
  return {mint.shareSign(*this)};
}

template <>
std::vector<types::Signature> CoinsSigner::sign<operations::Transaction>(const operations::Transaction& tx) const {
  std::vector<types::Signature> sigs;
  auto res = tx.tx_->shareSignCoins(*bsk_);
  for (const auto& [_, sig] : res) {
    (void)_;
    auto sig_str = libutt::serialize<libutt::RandSigShare>(sig);
    sigs.push_back(types::Signature(sig_str.begin(), sig_str.end()));
  }
  return sigs;
}

template <>
std::vector<types::Signature> CoinsSigner::sign<operations::Budget>(const operations::Budget& budget) const {
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
bool CoinsSigner::validate<libutt::BurnOp>(const UTTParams& p, const libutt::BurnOp& burn) const {
  return burn.validate(p.getParams(), *(bvk_), *(rvk_));
}

template <>
bool CoinsSigner::validate<operations::Burn>(const UTTParams& p, const operations::Burn& burn) const {
  return burn.validate(p, *this);
}
template <>
bool CoinsSigner::validate<operations::Transaction>(const UTTParams& p, const operations::Transaction& tx) const {
  return tx.tx_->validate(p.getParams(), *(bvk_), *(rvk_));
}

template <>
bool CoinsSigner::validatePartialSignature<libutt::MintOp>(uint16_t id,
                                                           const types::Signature& sig,
                                                           uint64_t,
                                                           const libutt::MintOp& mint) const {
  libutt::RandSigShare rsig = libutt::deserialize<libutt::RandSigShare>(sig);
  return mint.verifySigShare(rsig, *(shares_verification_keys_.at(id)));
}

template <>
bool CoinsSigner::validatePartialSignature<operations::Mint>(uint16_t id,
                                                             const types::Signature& sig,
                                                             uint64_t,
                                                             const operations::Mint& mint) const {
  return mint.validatePartialSig(*this, id, sig);
}

template <>
bool CoinsSigner::validatePartialSignature<operations::Budget>(uint16_t id,
                                                               const types::Signature& sig,
                                                               uint64_t,
                                                               const operations::Budget& budget) const {
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

  libutt::RandSigShare rsig = libutt::deserialize<libutt::RandSigShare>(sig);
  return rsig.verify({icm, scm, vcm, tcm, dcm}, *(shares_verification_keys_.at(id)));
}

template <>
bool CoinsSigner::validatePartialSignature<operations::Transaction>(uint16_t id,
                                                                    const types::Signature& sig,
                                                                    uint64_t txId,
                                                                    const operations::Transaction& tx) const {
  libutt::RandSigShare rsig = libutt::deserialize<libutt::RandSigShare>(sig);
  return tx.tx_->verifySigShare(txId, rsig, *(shares_verification_keys_.at(id)));
}
}  // namespace libutt::api