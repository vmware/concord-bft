#include "mint.hpp"
#include "coin.hpp"
#include "common.hpp"
#include <utt/MintOp.h>
#include <utt/Params.h>
#include <utt/Coin.h>
#include <utt/Serialization.h>

namespace libutt::api::operations {
struct Mint::Impl {
  Impl(const std::string& uniqueHash, size_t value, const std::string& recipPID) : op_{uniqueHash, value, recipPID} {}
  Impl() = default;
  libutt::MintOp op_;
};
Mint::Mint(const std::string& uniqueHash, size_t value, const std::string& recipPID) {
  impl_.reset(new Mint::Impl(uniqueHash, value, recipPID));
}
Mint::Mint() { impl_.reset(new Mint::Impl()); }
Mint::Mint(const Mint& other) {
  impl_.reset(new Mint::Impl());
  *(this->impl_) = *(other.impl_);
}

Mint& Mint::operator=(const Mint& other) {
  if (this == &other) return *this;
  *(this->impl_) = *(other.impl_);
  return *this;
}
std::string Mint::getHash() const { return impl_->op_.getHashHex(); }
uint64_t Mint::getVal() const { return impl_->op_.getVal().as_ulong(); }
std::string Mint::getRecipentID() const { return impl_->op_.getClientId(); }
bool Mint::validatePartialSig(const CoinsSigner& cs, uint16_t signer_id, const types::Signature& sig) const {
  return cs.validatePartialSignature(signer_id, sig, 0, impl_->op_);
}

types::Signature Mint::shareSign(const CoinsSigner& cs) const { return cs.sign(impl_->op_).front(); }

libutt::api::Coin Mint::claimCoin(const Client& c, const UTTParams& d, const types::Signature& blindedSig) const {
  return c.claimCoins(impl_->op_, d, {blindedSig}).front();
}
}  // namespace libutt::api::operations

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Mint& mint) {
  out << mint.impl_->op_;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Mint& mint) {
  in >> mint.impl_->op_;
  return in;
}