#include "budget.hpp"
#include "common.hpp"
#include "include/coin.impl.hpp"
#include <utt/PolyCrypto.h>
#include <utt/Serialization.h>

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Budget& budget) {
  out << budget.coin_;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Budget& budget) {
  in >> budget.coin_;
  return in;
}

namespace libutt::api::operations {
Budget::Budget(const UTTParams& d, const libutt::api::Client& cid, uint64_t val, uint64_t exp_date) {
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
Budget::Budget(const UTTParams& d,
               const types::CurvePoint& snHash,
               const types::CurvePoint& pidHash,
               uint64_t val,
               uint64_t exp_date,
               bool finalize) {
  Fr fr_val;
  fr_val.set_ulong(val);
  Fr fr_expdate;
  fr_expdate.set_ulong(exp_date);
  coin_ = libutt::api::Coin(
      d, snHash, fr_val.to_words(), pidHash, libutt::api::Coin::Type::Budget, fr_expdate.to_words(), finalize);
}
libutt::api::Coin& Budget::getCoin() { return coin_; }
const libutt::api::Coin& Budget::getCoin() const { return coin_; }
std::string Budget::getHashHex() const {
  return hashToHex("Budget|" + libutt::serialize<libutt::Coin>(coin_.pImpl_->c));
}
}  // namespace libutt::api::operations