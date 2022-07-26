#include "coin.hpp"
#include <utt/Coin.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/RandSig.h>

namespace libutt::api {
Coin::Coin(Details& d,
           const types::CurvePoint& prf,
           const types::CurvePoint& sn,
           const types::CurvePoint& val,
           Type t,
           const types::CurvePoint& exp_date,
           const ClientIdentity& cid) {
  Fr fr_sn;
  fr_sn.from_words(sn);
  Fr fr_val;
  fr_val.from_words(val);
  Fr fr_type = t == Type::Normal ? libutt::Coin::NormalType() : libutt::Coin::BudgetType();
  Fr fr_exp_date;
  fr_exp_date.from_words(exp_date);
  Fr pid_hash;
  pid_hash.from_words(cid.getPidHash());
  Fr fr_prf;
  fr_prf.from_words(prf);
  coin_.reset(new libutt::Coin(d.getParams().ck_coin,
                               d.getParams().null,
                               fr_prf,
                               fr_sn,
                               fr_val,
                               fr_type,
                               libutt::Coin::DoesNotExpire(),
                               pid_hash));
  type_ = t;
}
Coin::Coin(const Coin& c) {
  coin_.reset(new libutt::Coin());
  *(coin_) = *(c.coin_);
  has_sig_ = c.has_sig_;
  type_ = c.type_;
}
Coin& Coin::operator=(const Coin& c) {
  *(coin_) = *(c.coin_);
  has_sig_ = c.has_sig_;
  type_ = c.type_;
  return *this;
}
Coin::Coin() { coin_.reset(new libutt::Coin()); }
const std::string Coin::getNullifier() const { return coin_->null.toUniqueString(); }
bool Coin::hasSig() const { return has_sig_; }
void Coin::setSig(const types::Signature& sig) {
  coin_->sig = libutt::deserialize<libutt::RandSig>(std::string(sig.begin(), sig.end()));
  has_sig_ = true;
}
Coin::Type Coin::getType() const { return type_; }
types::Signature Coin::getSig() const {
  auto str_sig = libutt::serialize<libutt::RandSig>(coin_->sig);
  return types::Signature(str_sig.begin(), str_sig.end());
}
void Coin::randomize() {
  Fr u_delta = Fr::random_element();
  coin_->sig.rerandomize(coin_->r, u_delta);
}
}  // namespace libutt::api