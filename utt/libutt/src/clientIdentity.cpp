#include "clientIdentity.hpp"
#include "coin.hpp"
#include "burn.hpp"
#include "mint.hpp"
#include "transaction.hpp"
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
ClientIdentity::ClientIdentity(const std::string& pid,
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
Commitment ClientIdentity::generateFullRCM(Details& d) {
  std::vector<types::CurvePoint> m = {getPidHash(), ask_->s.to_words(), Fr::zero().to_words()};
  auto comm = Commitment(d, Commitment::Type::REGISTRATION, m, true);
  return comm;
}
Commitment ClientIdentity::generatePartialRCM(Details& d) {
  std::vector<types::CurvePoint> m = {Fr::zero().to_words(), ask_->s.to_words(), Fr::zero().to_words()};
  auto comm = Commitment(d, Commitment::Type::REGISTRATION, m, true);

  auto& reg_ck = d.getParams().ck_reg;
  auto h1 = hashToHex(getPidHash());
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  CommKey ck_extra({H, reg_ck.getGen1()});
  *(comm.comm_) = libutt::Comm::create(ck_extra, {ask_->s, Fr::zero()}, false);
  return comm;
}

const std::string& ClientIdentity::getPid() const { return ask_->pid; }
types::CurvePoint ClientIdentity::getPidHash() const { return ask_->getPidHash().to_words(); }
types::CurvePoint ClientIdentity::getPRFSecretKey() const { return ask_->s.to_words(); }

void ClientIdentity::setRCM(const Commitment& comm, const types::Signature& sig) {
  rcm_ = comm;
  rcm_sig_ = sig;
  ask_->rcm = *(comm.comm_);
  ask_->rs = libutt::deserialize<libutt::RandSig>(sig);
}

std::pair<Commitment, types::Signature> ClientIdentity::getRcm() const {
  auto tmp = libutt::serialize<libutt::RandSig>(ask_->rs);
  return {rcm_, types::Signature(tmp.begin(), tmp.end())};
}

template <>
std::vector<libutt::api::Coin> ClientIdentity::claimCoins<operations::Mint>(const operations::Mint& mint, Details& d, uint32_t n,
                     const std::vector<std::map<uint32_t, types::Signature>>& rsigs) const {
   Fr r_pid = Fr::zero(), r_sn = Fr::zero(), r_val = Fr::zero(), r_type = Fr::zero(), r_expdate = Fr::zero();
  std::vector<types::CurvePoint> r = {
      r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
  auto sig = Utils::aggregateSigShares(d, Commitment::Type::COIN, n, rsigs.front(), r);
  libutt::api::Coin c(d,
                      getPRFSecretKey(),
                      mint.op_->getSN().to_words(),
                      mint.op_->getVal().to_words(),
                      Coin::Type::Normal,
                      libutt::Coin::DoesNotExpire().to_words(),
                      *this);
  c.setSig(sig);
  c.randomize();
  return {c};
}

template <>
std::vector<libutt::api::Coin> ClientIdentity::claimCoins<operations::Transaction>(const operations::Transaction& tx, Details& d, uint32_t n,
                     const std::vector<std::map<uint32_t, types::Signature>>& rsigs) const {
                      std::vector<libutt::api::Coin> ret;
    auto mineTransactions = tx.tx_->getMineTransactions(*ask_);
    size_t i = 0;
    for (const auto& [txoIdx, txo] : mineTransactions) {
      Fr r_pid = txo.t, r_sn = Fr::zero(), r_val = txo.d, r_type = Fr::zero(), r_expdate = Fr::zero();
      std::vector<types::CurvePoint> r = {r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
      auto sig = Utils::aggregateSigShares(d, Commitment::Type::COIN, n, rsigs[i], r);
      libutt::api::Coin c(d,
                      getPRFSecretKey(),
                      tx.tx_->getSN(txoIdx).to_words(),
                      txo.val.to_words(),
                      txo.coin_type == libutt::Coin::NormalType() ? Coin::Type::Normal : Coin::Type::Budget, 
                      txo.exp_date.to_words(),
                      *this);
      c.setSig(sig);
      c.randomize();
      ret.emplace_back(std::move(c));
      i++;
    }
    return ret;
}


//   for (size_t i = 0 ; i < tx.tx_->outs.size() ; i++) {
//     auto txo = tx.tx_->outs.at(i);
//     auto internal_coin = tx.tx_->tryClaimCoin(d.getParams(), i, ask, n, rsigs.at(i))
//   }
//    Fr r_pid = Fr::zero(), r_sn = Fr::zero(), r_val = Fr::zero(), r_type = Fr::zero(), r_expdate = Fr::zero();
//   std::vector<types::CurvePoint> r = {
//       r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
//   auto sig = Utils::aggregateSigShares(d, Commitment::Type::COIN, n, rsigs, r);
//   libutt::api::Coin c(d,
//                       getPRFSecretKey(),
//                       mint.op_->getSN().to_words(),
//                       mint.op_->getVal().to_words(),
//                       Coin::Type::Normal,
//                       libutt::Coin::DoesNotExpire().to_words(),
//                       *this);
//   c.setSig(sig);
//   c.randomize();
//   return {c};
// }
template <>
bool ClientIdentity::validate<Coin>(const Coin& c) const {
  return c.coin_->hasValidSig(*bpk_);
}

}  // namespace libutt::api