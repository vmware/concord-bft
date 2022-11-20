#include "burn.hpp"
#include "include/coin.impl.hpp"
#include "include/burn.impl.hpp"
#include "include/commitment.impl.hpp"
#include "include/client.impl.hpp"
#include "include/params.impl.hpp"
#include <utt/Serialization.h>
#include <ostream>

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Burn& burn) {
  out << burn.pImpl_->burn_ << std::endl;
  out << burn.c_ << std::endl;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Burn& burn) {
  in >> burn.pImpl_->burn_;
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
  pImpl_ = new Burn::Impl();
  pImpl_->burn_ = libutt::BurnOp(d.pImpl_->p,
                                 fr_pidhash,
                                 cid.getPid(),
                                 rcm.first.pImpl_->comm_,
                                 rcm_sig,
                                 prf,
                                 coin.pImpl_->c,
                                 cid.pImpl_->bpk_,
                                 cid.pImpl_->rpk_);
}
Burn::Burn() { pImpl_ = new Burn::Impl(); }
Burn::Burn(const Burn& other) {
  pImpl_ = new Burn::Impl();
  *this = other;
}
Burn& Burn::operator=(const Burn& other) {
  if (&other == this) return *this;
  pImpl_->burn_ = other.pImpl_->burn_;
  c_ = other.c_;
  return *this;
}
Burn::~Burn() { delete pImpl_; }
Burn::Burn(Burn&& other) {
  pImpl_ = new Burn::Impl();
  *this = std::move(other);
}
Burn& Burn::operator=(Burn&& other) {
  *pImpl_ = std::move(*other.pImpl_);
  c_ = std::move(other.c_);
  return *this;
}

std::string Burn::getNullifier() const { return pImpl_->burn_.getNullifier(); }
const Coin& Burn::getCoin() const { return c_; }
const std::string& Burn::getOwnerPid() const { return pImpl_->burn_.getOwnerPid(); }
}  // namespace libutt::api::operations