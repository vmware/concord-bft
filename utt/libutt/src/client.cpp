#include "client.hpp"
#include "coin.hpp"
#include "burn.hpp"
#include "mint.hpp"
#include "transaction.hpp"
#include "budget.hpp"
#include <utt/IBE.h>
#include <utt/Address.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/Coin.h>
#include <utt/BurnOp.h>
#include <utt/MintOp.h>
#include <utt/Tx.h>
#include <vector>
#include <sstream>
namespace libutt::api {
Client::Client(const std::string& pid,
               const std::string& bpk,
               const std::string& rvk,
               const std::string& csk,
               const std::string& mpk) {
  ask_.reset(new libutt::AddrSK());
  ask_->pid = pid;
  ask_->s = Fr::random_element();
  ask_->pid_hash = AddrSK::pidHash(pid);
  bpk_.reset(new libutt::RandSigPK());
  *bpk_ = libutt::deserialize<libutt::RandSigPK>(bpk);
  rpk_.reset(new libutt::RegAuthPK());
  *rpk_ = libutt::deserialize<libutt::RegAuthPK>(rvk);
  ask_->e = libutt::deserialize<libutt::IBE::EncSK>(csk);
  ask_->mpk_ = libutt::deserialize<libutt::IBE::MPK>(mpk);
}

Commitment Client::generateInputRCM() {
  Commitment comm;
  auto h1 = hashToHex(getPidHash());
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  *(comm.comm_) = (ask_->s * H);
  return comm;
}

void Client::setPRFKey(const types::CurvePoint& s2) {
  Fr fr_s2;
  fr_s2.from_words(s2);
  ask_->s += fr_s2;
}
const std::string& Client::getPid() const { return ask_->pid; }
types::CurvePoint Client::getPidHash() const { return ask_->getPidHash().to_words(); }
types::CurvePoint Client::getPRFSecretKey() const { return ask_->s.to_words(); }

void Client::setRCMSig(const GlobalParams& d, const types::Signature& sig) {
  // Compute the complete rcm including s2
  std::vector<types::CurvePoint> m = {ask_->pid_hash.to_words(), ask_->s.to_words(), Fr::zero().to_words()};
  rcm_ = Commitment(d, Commitment::Type::REGISTRATION, m, true);
  rcm_sig_ = sig;
  ask_->rs = libutt::deserialize<libutt::RandSig>(sig);
}

std::pair<Commitment, types::Signature> Client::getRcm() const {
  auto tmp = libutt::serialize<libutt::RandSig>(ask_->rs);
  return {rcm_, types::Signature(tmp.begin(), tmp.end())};
}

template <>
std::vector<libutt::api::Coin> Client::claimCoins<operations::Mint>(
    const operations::Mint& mint,
    const GlobalParams& d,
    uint32_t n,
    const std::vector<std::map<uint32_t, types::Signature>>& rsigs) const {
  Fr r_pid = Fr::zero(), r_sn = Fr::zero(), r_val = Fr::zero(), r_type = Fr::zero(), r_expdate = Fr::zero();
  std::vector<types::CurvePoint> r = {
      r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
  auto sig = Utils::aggregateSigShares(d, Commitment::Type::COIN, n, rsigs.front(), r);
  libutt::api::Coin c(d,
                      getPRFSecretKey(),
                      mint.op_->getSN().to_words(),
                      mint.op_->getVal().to_words(),
                      getPidHash(),
                      Coin::Type::Normal,
                      libutt::Coin::DoesNotExpire().to_words());
  c.setSig(sig);
  c.rerandomize();
  return {c};
}

template <>
std::vector<libutt::api::Coin> Client::claimCoins<operations::Budget>(
    const operations::Budget& budget,
    const GlobalParams& d,
    uint32_t n,
    const std::vector<std::map<uint32_t, types::Signature>>& rsigs) const {
  Fr r_pid = Fr::zero(), r_sn = Fr::zero(), r_val = Fr::zero(), r_type = Fr::zero(), r_expdate = Fr::zero();
  std::vector<types::CurvePoint> r = {
      r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
  auto sig = Utils::aggregateSigShares(d, Commitment::Type::COIN, n, rsigs.front(), r);
  libutt::api::Coin c = budget.getCoin();
  c.setSig(sig);
  c.rerandomize();
  return {c};
}
template <>
std::vector<libutt::api::Coin> Client::claimCoins<operations::Transaction>(
    const operations::Transaction& tx,
    const GlobalParams& d,
    uint32_t n,
    const std::vector<std::map<uint32_t, types::Signature>>& rsigs) const {
  std::vector<libutt::api::Coin> ret;
  auto mineTransactions = tx.tx_->getMineTransactions(*ask_);
  for (const auto& [txoIdx, txo] : mineTransactions) {
    Fr r_pid = txo.t, r_sn = Fr::zero(), r_val = txo.d, r_type = Fr::zero(), r_expdate = Fr::zero();
    std::vector<types::CurvePoint> r = {
        r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
    auto sig = Utils::aggregateSigShares(d, Commitment::Type::COIN, n, rsigs[txoIdx], r);
    libutt::api::Coin c(d,
                        getPRFSecretKey(),
                        tx.tx_->getSN(txoIdx).to_words(),
                        txo.val.to_words(),
                        getPidHash(),
                        txo.coin_type == libutt::Coin::NormalType() ? Coin::Type::Normal : Coin::Type::Budget,
                        txo.exp_date.to_words());
    c.setSig(sig);
    c.rerandomize();
    ret.emplace_back(std::move(c));
  }
  return ret;
}

template <>
bool Client::validate<Coin>(const Coin& c) const {
  return c.coin_->hasValidSig(*bpk_);
}

}  // namespace libutt::api