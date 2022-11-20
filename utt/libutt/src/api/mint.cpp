#include "mint.hpp"
#include "coin.hpp"
#include "common.hpp"
#include "include/mint.impl.hpp"
#include <utt/Serialization.h>

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Mint& mint) {
  out << mint.pImpl_->mint_;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Mint& mint) {
  in >> mint.pImpl_->mint_;
  return in;
}

namespace libutt::api::operations {
Mint::Mint(const std::string& uniqueHash, size_t value, const std::string& recipPID) {
  pImpl_ = new Mint::Impl();
  pImpl_->mint_ = libutt::MintOp(uniqueHash, value, recipPID);
}
Mint::Mint() { pImpl_ = new Mint::Impl(); }
Mint::Mint(const Mint& other) {
  pImpl_ = new Mint::Impl();
  *this = other;
}

Mint& Mint::operator=(const Mint& other) {
  if (this == &other) return *this;
  pImpl_->mint_ = other.pImpl_->mint_;
  return *this;
}

Mint::~Mint() { delete pImpl_; }
Mint::Mint(Mint&& other) {
  pImpl_ = new Mint::Impl();
  *this = std::move(other);
}
Mint& Mint::operator=(Mint&& other) {
  pImpl_->mint_ = std::move(other.pImpl_->mint_);
  return *this;
}
std::string Mint::getHash() const { return pImpl_->mint_.getHashHex(); }
uint64_t Mint::getVal() const { return pImpl_->mint_.getVal().as_ulong(); }
std::string Mint::getRecipentID() const { return pImpl_->mint_.getClientId(); }

}  // namespace libutt::api::operations