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
bool Mint::validate(const std::string& uniqueHash, size_t value, const std::string& recipPID) const {
  return op_->validate(uniqueHash, value, recipPID);
}
}  // namespace libutt::api::operations