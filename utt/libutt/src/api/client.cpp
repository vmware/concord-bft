#include "client.hpp"
#include "coin.hpp"
#include "burn.hpp"
#include "mint.hpp"
#include "transaction.hpp"
#include "budget.hpp"
#include "include/coin.impl.hpp"
#include "include/commitment.impl.hpp"
#include "include/mint.impl.hpp"
#include "include/transaction.impl.hpp"
#include "include/client.impl.hpp"
#include <utt/Serialization.h>
#include <utt/DataUtils.hpp>
#include <vector>
#include <sstream>
namespace libutt::api {
Client::Client(const std::string& pid, const std::string& bpk, const std::string& rvk, const std::string& rsaSk) {
  if (pid.empty() || bpk.empty() || rvk.empty() || rsaSk.empty())
    throw std::runtime_error("Invalid parameters for building the client");
  pImpl_ = new Client::Impl();
  pImpl_->ask_.pid = pid;
  pImpl_->ask_.s = Fr::random_element();
  pImpl_->ask_.pid_hash = AddrSK::pidHash(pid);
  pImpl_->bpk_ = libutt::deserialize<libutt::RandSigPK>(bpk);
  pImpl_->rpk_ = libutt::deserialize<libutt::RegAuthPK>(rvk);
  pImpl_->decryptor_ = std::make_shared<libutt::RSADecryptor>(rsaSk);
}
Client::Client(const std::string& pid,
               const std::string& bpk,
               const std::string& rvk,
               const std::string& csk,
               const std::string& mpk) {
  if (pid.empty() || bpk.empty() || rvk.empty() || csk.empty() || mpk.empty())
    throw std::runtime_error("Invalid parameters for building the client");
  pImpl_ = new Client::Impl();
  pImpl_->ask_ = libutt::AddrSK();
  pImpl_->ask_.pid = pid;
  pImpl_->ask_.s = Fr::random_element();
  pImpl_->ask_.pid_hash = AddrSK::pidHash(pid);
  pImpl_->ask_.e = libutt::deserialize<libutt::IBE::EncSK>(csk);
  pImpl_->ask_.mpk_ = libutt::deserialize<libutt::IBE::MPK>(mpk);
  pImpl_->bpk_ = libutt::deserialize<libutt::RandSigPK>(bpk);
  pImpl_->rpk_ = libutt::deserialize<libutt::RegAuthPK>(rvk);
  pImpl_->decryptor_ = std::make_shared<libutt::IBEDecryptor>(pImpl_->ask_.e);
}
Client::~Client() { delete pImpl_; }
Client::Client() { pImpl_ = new Client::Impl(); }
Client::Client(const Client& other) {
  pImpl_ = new Client::Impl();
  *this = other;
}
Client& Client::operator=(const Client& other) {
  if (this == &other) return *this;
  pImpl_->ask_ = other.pImpl_->ask_;
  pImpl_->bpk_ = other.pImpl_->bpk_;
  pImpl_->rpk_ = other.pImpl_->rpk_;
  pImpl_->decryptor_ = other.pImpl_->decryptor_;
  return *this;
}
Client::Client(Client&& other) {
  pImpl_ = new Client::Impl();
  *this = std::move(other);
}
Client& Client::operator=(Client&& other) {
  pImpl_->ask_ = std::move(other.pImpl_->ask_);
  pImpl_->bpk_ = std::move(other.pImpl_->bpk_);
  pImpl_->rpk_ = std::move(other.pImpl_->rpk_);
  pImpl_->decryptor_ = std::move(other.pImpl_->decryptor_);
  return *this;
}
Commitment Client::generateInputRCM() {
  Commitment comm;
  auto h1 = hashToHex(getPidHash());
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  comm.pImpl_->comm_ = (pImpl_->ask_.s * H);
  return comm;
}

void Client::setPRFKey(const types::CurvePoint& s2) {
  if (complete_s) return;
  Fr fr_s2;
  fr_s2.from_words(s2);
  pImpl_->ask_.s += fr_s2;
  complete_s = true;
}
const std::string& Client::getPid() const { return pImpl_->ask_.pid; }
types::CurvePoint Client::getPidHash() const { return pImpl_->ask_.getPidHash().to_words(); }
types::CurvePoint Client::getPRFSecretKey() const { return pImpl_->ask_.s.to_words(); }

void Client::setRCMSig(const UTTParams& d, const types::CurvePoint& s2, const types::Signature& sig) {
  setPRFKey(s2);
  // Compute the complete rcm including s2
  std::vector<types::CurvePoint> m = {
      pImpl_->ask_.pid_hash.to_words(), pImpl_->ask_.s.to_words(), Fr::zero().to_words()};
  rcm_ = Commitment(d, Commitment::Type::REGISTRATION, m, true);
  rcm_sig_ = sig;
  pImpl_->ask_.rs = libutt::deserialize<libutt::RandSig>(sig);
  if (!pImpl_->ask_.rs.verify(rcm_.pImpl_->comm_, pImpl_->rpk_.vk))
    throw std::runtime_error("setRCMSig - failed to verify rcm against signature!");
}

std::pair<Commitment, types::Signature> Client::getRcm() const {
  auto tmp = libutt::serialize<libutt::RandSig>(pImpl_->ask_.rs);
  return {rcm_, types::Signature(tmp.begin(), tmp.end())};
}

std::pair<Commitment, types::Signature> Client::rerandomizeRcm(const UTTParams& d) const {
  auto rcm_copy = rcm_;
  auto r = rcm_copy.rerandomize(d, Commitment::Type::REGISTRATION, std::nullopt);
  Fr r_rand;
  r_rand.from_words(r);
  auto sig_obj = pImpl_->ask_.rs;
  sig_obj.rerandomize(r_rand, Fr::random_element());
  auto tmp = libutt::serialize<libutt::RandSig>(sig_obj);
  return {rcm_copy, types::Signature(tmp.begin(), tmp.end())};
}
template <>
std::vector<libutt::api::Coin> Client::claimCoins<operations::Mint>(
    const operations::Mint& mint, const UTTParams& d, const std::vector<types::Signature>& blindedSigs) const {
  if (blindedSigs.size() != 1) throw std::runtime_error("Mint suppose to contain a single coin only");
  Fr r_pid = Fr::zero(), r_sn = Fr::zero(), r_val = Fr::zero(), r_type = Fr::zero(), r_expdate = Fr::zero();
  std::vector<types::CurvePoint> r = {
      r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
  auto sig = Utils::unblindSignature(d, Commitment::Type::COIN, r, blindedSigs.front());
  libutt::api::Coin c(d,
                      getPRFSecretKey(),
                      mint.pImpl_->mint_.getSN().to_words(),
                      mint.pImpl_->mint_.getVal().to_words(),
                      getPidHash(),
                      Coin::Type::Normal,
                      libutt::Coin::DoesNotExpire().to_words());
  c.setSig(sig);
  c.rerandomize(std::nullopt);
  return {c};
}

template <>
std::vector<libutt::api::Coin> Client::claimCoins<operations::Budget>(
    const operations::Budget& budget, const UTTParams& d, const std::vector<types::Signature>& blindedSigs) const {
  if (blindedSigs.size() != 1) throw std::runtime_error("Mint suppose to contain a single coin only");
  Fr r_pid = Fr::zero(), r_sn = Fr::zero(), r_val = Fr::zero(), r_type = Fr::zero(), r_expdate = Fr::zero();
  std::vector<types::CurvePoint> r = {
      r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
  auto sig = Utils::unblindSignature(d, Commitment::Type::COIN, r, blindedSigs.front());
  libutt::api::Coin c = budget.getCoin();
  c.createNullifier(d, getPRFSecretKey());
  c.setSig(sig);
  c.rerandomize(std::nullopt);
  return {c};
}
template <>
std::vector<libutt::api::Coin> Client::claimCoins<operations::Transaction>(
    const operations::Transaction& tx, const UTTParams& d, const std::vector<types::Signature>& blindedSigs) const {
  std::vector<libutt::api::Coin> ret;
  auto mineTransactions = tx.pImpl_->tx_.getMyTransactions(*(pImpl_->decryptor_));
  for (const auto& [txoIdx, txo] : mineTransactions) {
    if (blindedSigs.size() <= txoIdx) throw std::runtime_error("Invalid number of blinded signatures");
    Fr r_pid = txo.t, r_sn = Fr::zero(), r_val = txo.d, r_type = Fr::zero(), r_expdate = Fr::zero();
    std::vector<types::CurvePoint> r = {
        r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
    auto sig = Utils::unblindSignature(d, Commitment::Type::COIN, r, blindedSigs[txoIdx]);
    libutt::api::Coin c(d,
                        getPRFSecretKey(),
                        tx.pImpl_->tx_.getSN(txoIdx).to_words(),
                        txo.val.to_words(),
                        getPidHash(),
                        txo.coin_type == libutt::Coin::NormalType() ? Coin::Type::Normal : Coin::Type::Budget,
                        txo.exp_date.to_words());
    c.setSig(sig);
    c.rerandomize(std::nullopt);
    ret.emplace_back(std::move(c));
  }
  return ret;
}

template <>
bool Client::validate<Coin>(const Coin& c) const {
  return c.pImpl_->c.hasValidSig(pImpl_->bpk_);
}

}  // namespace libutt::api