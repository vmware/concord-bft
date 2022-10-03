#include "burn.hpp"
#include <utt/BurnOp.h>
#include <utt/RandSig.h>
#include <utt/Coin.h>
#include <utt/Address.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/RegAuth.h>
#include <utt/Params.h>
#include <ostream>

using namespace libutt;
namespace libutt::api::operations {
struct Burn::Impl {
  Impl(const Params& p,
       Fr pid_hash,
       const std::string& pid,
       const Comm& rcm,
       const RandSig& rcm_sig,
       Fr prf,
       const libutt::Coin& c,
       const RandSigPK& bpk,
       const RegAuthPK& rpk)
      : burn_op_{p, pid_hash, pid, rcm, rcm_sig, prf, c, bpk, rpk} {}
  Impl() = default;
  libutt::BurnOp burn_op_;
};

Burn::Burn(const UTTParams& d, const Client& cid, const Coin& coin) : c_{coin} {
  Fr fr_pidhash;
  fr_pidhash.from_words(cid.getPidHash());
  Fr prf;
  prf.from_words(cid.getPRFSecretKey());
  const auto rcm = cid.getRcm();
  const auto rcm_str_sig = std::string(rcm.second.begin(), rcm.second.end());
  const auto rcm_sig = libutt::deserialize<libutt::RandSig>(rcm_str_sig);
  auto& rpk = *(cid.rpk_);
  impl_.reset(new Burn::Impl(
      d.getParams(), fr_pidhash, cid.getPid(), *(rcm.first.comm_), rcm_sig, prf, *(coin.coin_), *(cid.bpk_), rpk));
}
Burn::Burn() { impl_.reset(new Burn::Impl()); }
Burn::Burn(const Burn& other) {
  impl_.reset(new Burn::Impl());
  impl_->burn_op_ = other.impl_->burn_op_;
}
Burn& Burn::operator=(const Burn& other) {
  if (&other == this) return *this;
  impl_->burn_op_ = other.impl_->burn_op_;
  return *this;
}
std::string Burn::getNullifier() const { return impl_->burn_op_.getNullifier(); }
const Coin& Burn::getCoin() const { return c_; }

bool Burn::validate(const UTTParams& p, const libutt::api::CoinsSigner& cs) const {
  return cs.validate(p, impl_->burn_op_);
}
}  // namespace libutt::api::operations

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Burn& burn) {
  out << burn.impl_->burn_op_ << std::endl;
  out << burn.c_ << std::endl;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Burn& burn) {
  in >> burn.impl_->burn_op_;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> burn.c_;
  libff::consume_OUTPUT_NEWLINE(in);
  return in;
}