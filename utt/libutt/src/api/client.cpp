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
#include <utt/DataUtils.hpp>
#include <vector>
#include <sstream>
namespace libutt::api {
struct Client::Impl {
  Impl(const std::string& pid, const libutt::RandSigPK& bpk, const libutt::RegAuthPK& rpk, const std::string& rsaSk)
      : bpk_{bpk}, rpk_{rpk}, decryptor_{std::make_shared<libutt::RSADecryptor>(rsaSk)} {
    ask_.pid = pid;
    ask_.s = Fr::random_element();
    ask_.pid_hash = AddrSK::pidHash(pid);
  }
  Impl(const std::string& pid,
       const libutt::RandSigPK& bpk,
       const libutt::RegAuthPK& rpk,
       const libutt::IBE::EncSK& esk,
       const libutt::IBE::MPK& mpk)
      : bpk_{bpk}, rpk_{rpk}, decryptor_{std::make_shared<libutt::IBEDecryptor>(esk)} {
    ask_.pid = pid;
    ask_.s = Fr::random_element();
    ask_.pid_hash = AddrSK::pidHash(pid);
    ask_.e = esk;
    ask_.mpk_ = mpk;
  }
  libutt::RandSigPK bpk_;
  libutt::RegAuthPK rpk_;
  std::shared_ptr<libutt::IDecryptor> decryptor_;
  libutt::AddrSK ask_;
};
Client::Client(const std::string& pid, const std::string& bpk, const std::string& rvk, const std::string& rsaSk) {
  if (pid.empty() || bpk.empty() || rvk.empty() || rsaSk.empty())
    throw std::runtime_error("Invalid parameters for building the client");
  impl_.reset(new Client::Impl(
      pid, libutt::deserialize<libutt::RandSigPK>(bpk), libutt::deserialize<libutt::RegAuthPK>(rvk), rsaSk));
}
Client::Client(const std::string& pid,
               const std::string& bpk,
               const std::string& rvk,
               const std::string& csk,
               const std::string& mpk) {
  if (pid.empty() || bpk.empty() || rvk.empty() || csk.empty() || mpk.empty())
    throw std::runtime_error("Invalid parameters for building the client");
  impl_.reset(new Client::Impl(pid,
                               libutt::deserialize<libutt::RandSigPK>(bpk),
                               libutt::deserialize<libutt::RegAuthPK>(rvk),
                               libutt::deserialize<libutt::IBE::EncSK>(csk),
                               libutt::deserialize<libutt::IBE::MPK>(mpk)));
}

Commitment Client::generateInputRCM() {
  Commitment comm;
  auto h1 = hashToHex(getPidHash());
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  *((libutt::Comm*)comm.getInternals()) = (impl_->ask_.s * H);
  return comm;
}

void Client::setPRFKey(const types::CurvePoint& s2) {
  if (complete_s) return;
  Fr fr_s2;
  fr_s2.from_words(s2);
  impl_->ask_.s += fr_s2;
  complete_s = true;
}
const std::string& Client::getPid() const { return impl_->ask_.pid; }
types::CurvePoint Client::getPidHash() const { return impl_->ask_.getPidHash().to_words(); }
types::CurvePoint Client::getPRFSecretKey() const { return impl_->ask_.s.to_words(); }

void Client::setRCMSig(const UTTParams& d, const types::CurvePoint& s2, const types::Signature& sig) {
  setPRFKey(s2);
  // Compute the complete rcm including s2
  std::vector<types::CurvePoint> m = {impl_->ask_.pid_hash.to_words(), impl_->ask_.s.to_words(), Fr::zero().to_words()};
  rcm_ = Commitment(d, Commitment::Type::REGISTRATION, m, true);
  rcm_sig_ = sig;
  impl_->ask_.rs = libutt::deserialize<libutt::RandSig>(sig);
}

std::pair<Commitment, types::Signature> Client::getRcm() const {
  auto tmp = libutt::serialize<libutt::RandSig>(impl_->ask_.rs);
  return {rcm_, types::Signature(tmp.begin(), tmp.end())};
}

std::pair<Commitment, types::Signature> Client::rerandomizeRcm(const UTTParams& d) const {
  auto rcm_copy = rcm_;
  auto r = rcm_copy.rerandomize(d, Commitment::Type::REGISTRATION, std::nullopt);
  Fr r_rand;
  r_rand.from_words(r);
  auto sig_obj = impl_->ask_.rs;
  sig_obj.rerandomize(r_rand, Fr::random_element());
  auto tmp = libutt::serialize<libutt::RandSig>(sig_obj);
  return {rcm_copy, types::Signature(tmp.begin(), tmp.end())};
}

template <>
std::vector<libutt::api::Coin> Client::claimCoins<libutt::MintOp>(
    const libutt::MintOp& mint, const UTTParams& d, const std::vector<types::Signature>& blindedSigs) const {
  if (blindedSigs.size() != 1) throw std::runtime_error("Mint suppose to contain a single coin only");
  Fr r_pid = Fr::zero(), r_sn = Fr::zero(), r_val = Fr::zero(), r_type = Fr::zero(), r_expdate = Fr::zero();
  std::vector<types::CurvePoint> r = {
      r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
  auto sig = Utils::unblindSignature(d, Commitment::Type::COIN, r, blindedSigs.front());
  libutt::api::Coin c(d,
                      getPRFSecretKey(),
                      mint.getSN().to_words(),
                      mint.getVal().to_words(),
                      getPidHash(),
                      Coin::Type::Normal,
                      libutt::Coin::DoesNotExpire().to_words());
  c.setSig(sig);
  c.rerandomize(std::nullopt);
  return {c};
}

template <>
std::vector<libutt::api::Coin> Client::claimCoins<operations::Mint>(
    const operations::Mint& mint, const UTTParams& d, const std::vector<types::Signature>& blindedSigs) const {
  return {mint.claimCoin(*this, d, blindedSigs.front())};
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
std::vector<libutt::api::Coin> Client::claimCoins<libutt::Tx>(const libutt::Tx& tx,
                                                              const UTTParams& d,
                                                              const std::vector<types::Signature>& blindedSigs) const {
  std::vector<libutt::api::Coin> ret;
  auto mineTransactions = tx.getMyTransactions(*(impl_->decryptor_));
  for (const auto& [txoIdx, txo] : mineTransactions) {
    if (blindedSigs.size() <= txoIdx) throw std::runtime_error("Invalid number of blinded signatures");
    Fr r_pid = txo.t, r_sn = Fr::zero(), r_val = txo.d, r_type = Fr::zero(), r_expdate = Fr::zero();
    std::vector<types::CurvePoint> r = {
        r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
    auto sig = Utils::unblindSignature(d, Commitment::Type::COIN, r, blindedSigs[txoIdx]);
    libutt::api::Coin c(d,
                        getPRFSecretKey(),
                        tx.getSN(txoIdx).to_words(),
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
std::vector<libutt::api::Coin> Client::claimCoins<operations::Transaction>(
    const operations::Transaction& tx, const UTTParams& d, const std::vector<types::Signature>& blindedSigs) const {
  return tx.claimCoins(*this, d, blindedSigs);
}

template <>
bool Client::validate<libutt::Coin>(const libutt::Coin& c) const {
  return c.hasValidSig(impl_->bpk_);
}

template <>
bool Client::validate<Coin>(const Coin& c) const {
  return c.validate(*this);
}

std::vector<void*> Client::getInternals() const { return {&(impl_->bpk_), &(impl_->rpk_)}; }
}  // namespace libutt::api