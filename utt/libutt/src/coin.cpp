#include "coin.hpp"
#include <utt/Coin.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/RandSig.h>

namespace libutt::api {
Coin::Coin(const GlobalParams& d,
           const types::CurvePoint& prf,
           const types::CurvePoint& sn,
           const types::CurvePoint& val,
           const types::CurvePoint& pidhash,
           Type t,
           const types::CurvePoint& exp_date) {
  Fr fr_sn;
  fr_sn.from_words(sn);
  Fr fr_val;
  fr_val.from_words(val);
  Fr fr_type = t == Type::Normal ? libutt::Coin::NormalType() : libutt::Coin::BudgetType();
  Fr fr_exp_date;
  fr_exp_date.from_words(exp_date);
  Fr pid_hash;
  pid_hash.from_words(pidhash);
  Fr fr_prf;
  fr_prf.from_words(prf);
  coin_.reset(new libutt::Coin(
      d.getParams().ck_coin, d.getParams().null, fr_prf, fr_sn, fr_val, fr_type, fr_exp_date, pid_hash));
  type_ = t;
}

Coin::Coin(const GlobalParams& d,
           const types::CurvePoint& sn,
           const types::CurvePoint& val,
           const types::CurvePoint& pidhash,
           Type t,
           const types::CurvePoint& exp_date) {
  Fr fr_sn;
  fr_sn.from_words(sn);
  Fr fr_val;
  fr_val.from_words(val);
  Fr fr_type = t == Type::Normal ? libutt::Coin::NormalType() : libutt::Coin::BudgetType();
  Fr fr_exp_date;
  fr_exp_date.from_words(exp_date);
  Fr pid_hash;
  pid_hash.from_words(pidhash);

  coin_.reset(new libutt::Coin(d.getParams().ck_coin, fr_sn, fr_val, fr_type, fr_exp_date, pid_hash));
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
  if (sig.empty()) return;
  coin_->sig = libutt::deserialize<libutt::RandSig>(std::string(sig.begin(), sig.end()));
  has_sig_ = true;
}
Coin::Type Coin::getType() const { return type_; }

uint64_t Coin::getVal() const { return coin_->val.as_ulong(); }
types::Signature Coin::getSig() const {
  auto str_sig = libutt::serialize<libutt::RandSig>(coin_->sig);
  return types::Signature(str_sig.begin(), str_sig.end());
}
void Coin::rerandomize() {
  Fr u_delta = Fr::random_element();
  coin_->sig.rerandomize(coin_->r, u_delta);
}

types::CurvePoint Coin::getPidHash() const { return coin_->pid_hash.to_words(); }
types::CurvePoint Coin::getSN() const { return coin_->sn.to_words(); }
std::string Coin::getExpDate() const { return exp_date_str_; }
types::CurvePoint Coin::getExpDateAsCurvePoint() const { return coin_->exp_date.to_words(); }
}  // namespace libutt::api