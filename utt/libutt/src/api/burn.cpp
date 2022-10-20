#include "burn.hpp"
#include <utt/BurnOp.h>
#include <utt/RandSig.h>
#include <utt/Coin.h>
#include <utt/Address.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/RegAuth.h>
#include <ostream>

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Burn& burn) {
  out << *(burn.burn_) << std::endl;
  out << burn.c_ << std::endl;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Burn& burn) {
  in >> *(burn.burn_);
  libff::consume_OUTPUT_NEWLINE(in);
  in >> burn.c_;
  libff::consume_OUTPUT_NEWLINE(in);
  return in;
}
namespace libutt::api::operations {
Burn::Burn(const UTTParams& d, const Client& cid, const Coin& coin) : c_{coin} {
  Fr fr_pidhash;
  fr_pidhash.from_words(cid.getPidHash());
  Fr prf;
  prf.from_words(cid.getPRFSecretKey());
  const auto rcm = cid.getRcm();
  const auto rcm_str_sig = std::string(rcm.second.begin(), rcm.second.end());
  const auto rcm_sig = libutt::deserialize<libutt::RandSig>(rcm_str_sig);
  auto& rpk = *(cid.rpk_);
  burn_.reset(new libutt::BurnOp(
      d.getParams(), fr_pidhash, cid.getPid(), *(rcm.first.comm_), rcm_sig, prf, *(coin.coin_), *(cid.bpk_), rpk));
}
Burn::Burn() { burn_.reset(new libutt::BurnOp()); }
Burn::Burn(const Burn& other) {
  burn_.reset(new libutt::BurnOp());
  *this = other;
}
Burn& Burn::operator=(const Burn& other) {
  if (&other == this) return *this;
  *(burn_) = *(other.burn_);
  return *this;
}
std::string Burn::getNullifier() const { return burn_->getNullifier(); }
const Coin& Burn::getCoin() const { return c_; }
const std::string& Burn::getOwnerPid() const { return burn_->getOwnerPid(); }
}  // namespace libutt::api::operations