#include "coinsSigner.hpp"
#include "mint.hpp"
#include "UTTParams.hpp"
#include "burn.hpp"
#include "transaction.hpp"
#include "budget.hpp"
#include "include/coin.impl.hpp"
#include "include/burn.impl.hpp"
#include "include/mint.impl.hpp"
#include "include/transaction.impl.hpp"
#include "include/coinsSigner.impl.hpp"
#include "include/params.impl.hpp"
#include <utt/Serialization.h>
#include <vector>

namespace libutt::api {
CoinsSigner::CoinsSigner(uint16_t id,
                         const std::string& bsk,
                         const std::string& bvk,
                         const std::map<uint16_t, std::string>& shares_verification_keys,
                         const std::string& rvk) {
  bid_ = id;
  pImpl_ = new CoinsSigner::Impl();
  pImpl_->bsk_ = libutt::deserialize<libutt::RandSigShareSK>(bsk);
  pImpl_->bvk_ = libutt::deserialize<libutt::RandSigPK>(bvk);
  pImpl_->rvk_ = libutt::deserialize<libutt::RegAuthPK>(rvk);
  for (const auto& [id, svk] : shares_verification_keys) {
    pImpl_->shares_verification_keys_[id] = libutt::deserialize<libutt::RandSigSharePK>(svk);
  }
}

CoinsSigner::CoinsSigner() { pImpl_ = new CoinsSigner::Impl(); }
CoinsSigner::~CoinsSigner() { delete pImpl_; }
CoinsSigner::CoinsSigner(const CoinsSigner& other) {
  pImpl_ = new CoinsSigner::Impl();
  *this = other;
}
CoinsSigner& CoinsSigner::operator=(const CoinsSigner& other) {
  if (this == &other) return *this;
  pImpl_->bsk_ = other.pImpl_->bsk_;
  pImpl_->bvk_ = other.pImpl_->bvk_;
  pImpl_->rvk_ = other.pImpl_->rvk_;
  pImpl_->shares_verification_keys_ = other.pImpl_->shares_verification_keys_;
  return *this;
}
CoinsSigner::CoinsSigner(CoinsSigner&& other) {
  pImpl_ = new CoinsSigner::Impl();
  *this = std::move(other);
}
CoinsSigner& CoinsSigner::operator=(CoinsSigner&& other) {
  pImpl_->bsk_ = std::move(other.pImpl_->bsk_);
  pImpl_->bvk_ = std::move(other.pImpl_->bvk_);
  pImpl_->rvk_ = std::move(other.pImpl_->rvk_);
  pImpl_->shares_verification_keys_ = std::move(other.pImpl_->shares_verification_keys_);
  return *this;
}

uint16_t CoinsSigner::getId() const { return bid_; }
template <>
std::vector<types::Signature> CoinsSigner::sign<operations::Mint>(const operations::Mint& mint) const {
  auto res = mint.pImpl_->mint_.shareSignCoin(pImpl_->bsk_);
  auto res_str = libutt::serialize<libutt::RandSigShare>(res);
  return {types::Signature(res_str.begin(), res_str.end())};
}
template <>
std::vector<types::Signature> CoinsSigner::sign<operations::Transaction>(const operations::Transaction& tx) const {
  std::vector<types::Signature> sigs;
  auto res = tx.pImpl_->tx_.shareSignCoins(pImpl_->bsk_);
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

  auto sig = pImpl_->bsk_.shareSign({icm, scm, vcm, tcm, dcm}, H);
  auto sig_str = libutt::serialize<libutt::RandSigShare>(sig);
  return {types::Signature(sig_str.begin(), sig_str.end())};
}

template <>
bool CoinsSigner::validate<operations::Burn>(const UTTParams& p, const operations::Burn& burn) const {
  return burn.pImpl_->burn_.validate(p.pImpl_->p, pImpl_->bvk_, pImpl_->rvk_);
}
template <>
bool CoinsSigner::validate<operations::Transaction>(const UTTParams& p, const operations::Transaction& tx) const {
  return tx.pImpl_->tx_.validate(p.pImpl_->p, pImpl_->bvk_, pImpl_->rvk_);
}

template <>
bool CoinsSigner::validatePartialSignature<operations::Mint>(uint16_t id,
                                                             const types::Signature& sig,
                                                             uint64_t,
                                                             const operations::Mint& mint) const {
  libutt::RandSigShare rsig = libutt::deserialize<libutt::RandSigShare>(sig);
  return mint.pImpl_->mint_.verifySigShare(rsig, pImpl_->shares_verification_keys_.at(id));
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
  return rsig.verify({icm, scm, vcm, tcm, dcm}, pImpl_->shares_verification_keys_.at(id));
}

template <>
bool CoinsSigner::validatePartialSignature<operations::Transaction>(uint16_t id,
                                                                    const types::Signature& sig,
                                                                    uint64_t txId,
                                                                    const operations::Transaction& tx) const {
  libutt::RandSigShare rsig = libutt::deserialize<libutt::RandSigShare>(sig);
  return tx.pImpl_->tx_.verifySigShare(txId, rsig, pImpl_->shares_verification_keys_.at(id));
}
}  // namespace libutt::api