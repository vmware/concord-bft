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
Coin Mint::claimCoin(Details& d,
                     ClientIdentity& cid,
                     uint32_t n,
                     const std::map<uint32_t, std::vector<uint8_t>>& rsigs) const {
  Fr r_pid = Fr::zero(), r_sn = Fr::zero(), r_val = Fr::zero(), r_type = Fr::zero(), r_expdate = Fr::zero();
  std::vector<types::CurvePoint> r = {
      r_pid.to_words(), r_sn.to_words(), r_val.to_words(), r_type.to_words(), r_expdate.to_words()};
  auto sig = Utils::aggregateSigShares(d, Commitment::Type::COIN, n, rsigs, r);
  libutt::api::Coin c(d,
                      cid.getPRFSecretKey(),
                      op_->getSN().to_words(),
                      op_->getVal().to_words(),
                      Coin::Type::Normal,
                      libutt::Coin::DoesNotExpire().to_words(),
                      cid);
  c.setSig(sig);
  c.randomize();
  return c;
}
}  // namespace libutt::api::operations