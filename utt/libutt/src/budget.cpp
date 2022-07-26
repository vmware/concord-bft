#include "budget.hpp"
#include <utt/Params.h>
#include <utt/Coin.h>
#include <utt/PolyCrypto.h>
#include <utt/Serialization.h>
namespace libutt::api::operations {
Budget::Budget(const GlobalParams& d, const libutt::api::Client& cid, uint64_t val, uint64_t exp_date) {
  Fr fr_val;
  fr_val.set_ulong(val);
  Fr fr_expdate;
  fr_expdate.set_ulong(exp_date);
  coin_ = libutt::api::Coin(d,
                            cid.getPRFSecretKey(),
                            Fr::random_element().to_words(),
                            fr_val.to_words(),
                            cid.getPidHash(),
                            libutt::api::Coin::Type::Budget,
                            fr_expdate.to_words());
}
libutt::api::Coin& Budget::getCoin() { return coin_; }
const libutt::api::Coin& Budget::getCoin() const { return coin_; }
std::string Budget::getHashHex() const {
  return hashToHex(("Budget|" + libutt::serialize<libutt::Coin>((*coin_.coin_))));
}
types::CurvePoint Budget::getPidHash() const { return coin_.coin_->pid_hash.to_words(); }
types::CurvePoint Budget::getSN() const { return coin_.coin_->sn.to_words(); }
types::CurvePoint Budget::getVal() const { return coin_.coin_->val.to_words(); }
types::CurvePoint Budget::getExpDate() const { return coin_.coin_->exp_date.to_words(); }
}  // namespace libutt::api::operations