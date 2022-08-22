#include "mint.hpp"
#include "coin.hpp"
#include "common.hpp"
#include <utt/MintOp.h>
#include <utt/Params.h>
#include <utt/Coin.h>
#include <utt/Serialization.h>

namespace libutt::api::operations {
Mint::Mint(const std::string& uniqueHash, size_t value, const std::string& recipPID) {
  op_.reset(new libutt::MintOp(uniqueHash, value, recipPID));
}

std::string Mint::getHash() const { return op_->getHashHex(); }
uint64_t Mint::getVal() const { return op_->getVal().as_ulong(); }
std::string Mint::getRecipentID() const { return op_->getClientId(); }

}  // namespace libutt::api::operations