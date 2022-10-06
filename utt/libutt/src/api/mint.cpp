#include "mint.hpp"
#include "coin.hpp"
#include "common.hpp"
#include <utt/MintOp.h>
#include <utt/Params.h>
#include <utt/Coin.h>
#include <utt/Serialization.h>

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Mint& mint) {
  out << *(mint.op_);
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Mint& mint) {
  in >> *(mint.op_);
  return in;
}

namespace libutt::api::operations {
Mint::Mint(const std::string& uniqueHash, size_t value, const std::string& recipPID) {
  op_.reset(new libutt::MintOp(uniqueHash, value, recipPID));
}
Mint::Mint() { op_.reset(new libutt::MintOp()); }
Mint::Mint(const Mint& other) {
  op_.reset(new libutt::MintOp());
  *this = other;
}

Mint& Mint::operator=(const Mint& other) {
  if (this == &other) return *this;
  *(op_) = *(other.op_);
  return *this;
}
std::string Mint::getHash() const { return op_->getHashHex(); }
uint64_t Mint::getVal() const { return op_->getVal().as_ulong(); }
std::string Mint::getRecipentID() const { return op_->getClientId(); }

}  // namespace libutt::api::operations