#include "coin.hpp"
#include <utt/Coin.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/RandSig.h>

namespace libutt::api {
Coin::Coin(Details& d,
           const std::vector<uint64_t>& sn,
           const std::vector<uint64_t>& val,
           Type t,
           const std::vector<uint64_t>& exp_date,
           ClientIdentity& cid) {
  Fr fr_sn;
  fr_sn.from_words(sn);
  Fr fr_val;
  fr_val.from_words(val);
  Fr fr_type = t == Type::Normal ? libutt::Coin::NormalType() : libutt::Coin::BudgetType();
  Fr fr_exp_date;
  fr_exp_date.from_words(exp_date);
  Fr pid_hash;
  pid_hash.from_words(cid.getPidHash());
  coin_.reset(new libutt::Coin(d.getParams().ck_coin, fr_sn, fr_val, fr_type, fr_exp_date, pid_hash));
  nullifier_.reset(new Nullifier(d, cid.getPRFSecretKey(), sn, Fr::random_element().to_words()));
  type_ = t;
}
const Nullifier& Coin::getNullifier() const { return *nullifier_; }
bool Coin::hasSig() const { return has_sig_; }
void Coin::setSig(const std::vector<uint8_t>& sig) {
  coin_->sig = libutt::deserialize<libutt::RandSig>(std::string(sig.begin(), sig.end()));
  has_sig_ = true;
}
Coin::Type Coin::getType() const { return type_; }
std::vector<uint8_t> Coin::getSig() const {
  auto str_sig = libutt::serialize<libutt::RandSig>(coin_->sig);
  return std::vector<uint8_t>(str_sig.begin(), str_sig.end());
}
void Coin::randomize() {
  Fr u_delta = Fr::random_element();
  coin_->sig.rerandomize(coin_->r, u_delta);
}
}  // namespace libutt::api