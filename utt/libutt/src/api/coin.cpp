#include "include/coin.impl.hpp"
#include "include/params.impl.hpp"
#include <utt/Serialization.h>
#include <ostream>

std::ostream& operator<<(std::ostream& out, const libutt::api::Coin& coin) {
  out << coin.has_sig_ << std::endl;
  out << coin.pImpl_->c << std::endl;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::Coin& coin) {
  in >> coin.has_sig_;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> coin.pImpl_->c;
  libff::consume_OUTPUT_NEWLINE(in);
  coin.type_ = coin.pImpl_->c.isNormal() ? libutt::api::Coin::Type::Normal : libutt::api::Coin::Type::Budget;
  return in;
}

namespace libutt::api {
Coin::Coin(const UTTParams& d,
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
  pImpl_ = new Coin::Impl();
  pImpl_->c =
      libutt::Coin(d.pImpl_->p.ck_coin, d.pImpl_->p.null, fr_prf, fr_sn, fr_val, fr_type, fr_exp_date, pid_hash);
  type_ = t;
}
Coin::~Coin() { delete pImpl_; }
Coin::Coin(const UTTParams& d,
           const types::CurvePoint& sn,
           const types::CurvePoint& val,
           const types::CurvePoint& pidhash,
           Type t,
           const types::CurvePoint& exp_date,
           bool finalize) {
  Fr fr_sn;
  fr_sn.from_words(sn);
  Fr fr_val;
  fr_val.from_words(val);
  Fr fr_type = t == Type::Normal ? libutt::Coin::NormalType() : libutt::Coin::BudgetType();
  Fr fr_exp_date;
  fr_exp_date.from_words(exp_date);
  Fr pid_hash;
  pid_hash.from_words(pidhash);
  pImpl_ = new Coin::Impl();
  pImpl_->c = libutt::Coin(d.pImpl_->p.ck_coin, fr_sn, fr_val, fr_type, fr_exp_date, pid_hash, finalize);
  type_ = t;
}
Coin::Coin(const Coin& c) {
  pImpl_ = new Coin::Impl();
  *this = c;
}
Coin& Coin::operator=(const Coin& c) {
  if (&c == this) return *this;
  pImpl_->c = c.pImpl_->c;
  has_sig_ = c.has_sig_;
  type_ = c.type_;
  return *this;
}
Coin::Coin(Coin&& c) {
  pImpl_ = new Coin::Impl();
  *this = std::move(c);
}

Coin& Coin::operator=(Coin&& c) {
  pImpl_->c = std::move(c.pImpl_->c);
  has_sig_ = c.has_sig_;
  type_ = c.type_;
  return *this;
}
Coin::Coin() { pImpl_ = new Coin::Impl(); }

std::string Coin::getNullifier() const { return pImpl_->c.null.toUniqueString(); }
bool Coin::hasSig() const { return has_sig_; }
void Coin::setSig(const types::Signature& sig) {
  if (sig.empty()) return;
  pImpl_->c.sig = libutt::deserialize<libutt::RandSig>(std::string(sig.begin(), sig.end()));
  has_sig_ = true;
}
Coin::Type Coin::getType() const { return type_; }

uint64_t Coin::getVal() const { return pImpl_->c.val.as_ulong(); }
types::Signature Coin::getSig() const {
  auto str_sig = libutt::serialize<libutt::RandSig>(pImpl_->c.sig);
  return types::Signature(str_sig.begin(), str_sig.end());
}
void Coin::rerandomize(std::optional<types::CurvePoint> base_randomness) {
  Fr u_delta = Fr::random_element();
  if (base_randomness.has_value()) u_delta.from_words(*base_randomness);
  pImpl_->c.sig.rerandomize(pImpl_->c.r, u_delta);
}
void Coin::finalize(const UTTParams& p, const types::CurvePoint& prf) {
  Fr fr_prf;
  fr_prf.from_words(prf);
  pImpl_->c.createNullifier(p.pImpl_->p.null, fr_prf);
  pImpl_->c.commit();
}

void Coin::createNullifier(const UTTParams& d, const types::CurvePoint& prf) {
  Fr fr_prf;
  fr_prf.from_words(prf);
  pImpl_->c.createNullifier(d.pImpl_->p.null, fr_prf);
}
types::CurvePoint Coin::getPidHash() const { return pImpl_->c.pid_hash.to_words(); }
types::CurvePoint Coin::getSN() const { return pImpl_->c.sn.to_words(); }
types::CurvePoint Coin::getExpDateAsCurvePoint() const { return pImpl_->c.exp_date.to_words(); }
uint64_t Coin::getExpDate() const { return pImpl_->c.exp_date.as_ulong(); }
}  // namespace libutt::api