#include "transaction.hpp"
#include <utt/Tx.h>
#include <utt/Serialization.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>
#include <utt/Coin.h>
#include <utt/Address.h>
#include <utt/DataUtils.hpp>

namespace libutt::api::operations {
struct Transaction::Impl {
  Impl(const libutt::Params& p,
       Fr pid_hash,
       const std::string& pid,
       const libutt::Comm& rcm,
       const libutt::RandSig& rcm_sig,
       Fr prf,
       const std::vector<libutt::Coin>& input_coins,
       const std::optional<libutt::Coin>& budget_coin,
       const std::vector<std::tuple<std::string, Fr>>& recipients,
       const libutt::RandSigPK& rvk,
       const libutt::IEncryptor& encryptor)
      : tx_{p, pid_hash, pid, rcm, rcm_sig, prf, input_coins, budget_coin, recipients, std::nullopt, rvk, encryptor} {}
  Impl() = default;
  libutt::Tx tx_;
};
Transaction::Transaction(const UTTParams& d,
                         const Client& cid,
                         const std::vector<Coin>& coins,
                         const std::optional<Coin>& bc,
                         const std::vector<std::tuple<std::string, uint64_t>>& recipients,
                         const IEncryptor& encryptor) {
  input_coins_ = coins;
  budget_coin_ = bc;
  Fr fr_pidhash;
  fr_pidhash.from_words(cid.getPidHash());
  Fr prf;
  prf.from_words(cid.getPRFSecretKey());
  auto rcm = cid.getRcm();
  auto rcm_str_sig = std::string(rcm.second.begin(), rcm.second.end());
  auto rcm_sig = libutt::deserialize<libutt::RandSig>(rcm_str_sig);
  std::vector<libutt::Coin> input_coins(coins.size());
  for (size_t i = 0; i < coins.size(); i++) {
    auto& c = coins[i];
    input_coins[i] = *((libutt::Coin*)(c.getInternals()));
  }
  std::optional<libutt::Coin> budget_coin = std::nullopt;
  if (bc.has_value()) budget_coin.emplace(*((libutt::Coin*)(bc->getInternals())));
  std::vector<std::tuple<std::string, Fr>> fr_recipients(recipients.size());
  for (size_t i = 0; i < recipients.size(); i++) {
    // initiate the Fr types with the values given in the recipients vector (becasue the interanl Tx object gets
    // vector<Fr> as an input)
    const auto& [r_str, r_id] = recipients[i];
    auto& [id, fr] = fr_recipients[i];
    id = r_str;
    fr.set_ulong(r_id);
  }
  auto& rpk = *(cid.rpk_);
  impl_.reset(new Transaction::Impl(d.getParams(),
                                    fr_pidhash,
                                    cid.getPid(),
                                    *(rcm.first.comm_),
                                    rcm_sig,
                                    prf,
                                    input_coins,
                                    budget_coin,
                                    fr_recipients,
                                    rpk.vk,
                                    encryptor));
}
Transaction::Transaction() { impl_.reset(new Transaction::Impl()); }
Transaction::Transaction(const Transaction& other) {
  impl_.reset(new Transaction::Impl());
  *(this->impl_) = *(other.impl_);
  input_coins_ = other.input_coins_;
  budget_coin_ = other.budget_coin_;
}
Transaction& Transaction::operator=(const Transaction& other) {
  if (this == &other) return *this;
  *(this->impl_) = *(other.impl_);
  input_coins_ = other.input_coins_;
  budget_coin_ = other.budget_coin_;
  return *this;
}
std::vector<std::string> Transaction::getNullifiers() const { return impl_->tx_.getNullifiers(); }
const std::vector<Coin>& Transaction::getInputCoins() const { return input_coins_; }
std::optional<Coin> Transaction::getBudgetCoin() const { return budget_coin_; }

uint32_t Transaction::getNumOfOutputCoins() const { return (uint32_t)impl_->tx_.outs.size(); }
std::vector<types::Signature> Transaction::shareSign(const CoinsSigner& cs) const { return cs.sign(impl_->tx_); }

std::vector<libutt::api::Coin> Transaction::claimCoins(const Client& c,
                                                       const UTTParams& d,
                                                       const std::vector<types::Signature>& blindedSigs) const {
  return c.claimCoins(impl_->tx_, d, blindedSigs);
}

bool Transaction::validate(const CoinsSigner& cs, const UTTParams& p) const { return cs.validate(p, impl_->tx_); }
bool Transaction::validatePartialSignature(const CoinsSigner& cs,
                                           uint16_t id,
                                           const types::Signature& sig,
                                           uint64_t txId) const {
  return cs.validatePartialSignature(id, sig, txId, impl_->tx_);
}
}  // namespace libutt::api::operations

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Transaction& tx) {
  out << tx.impl_->tx_ << std::endl;
  libutt::serializeVector(out, tx.input_coins_);
  out << tx.budget_coin_;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Transaction& tx) {
  in >> tx.impl_->tx_;
  libff::consume_OUTPUT_NEWLINE(in);
  libutt::deserializeVector(in, tx.input_coins_);
  in >> tx.budget_coin_;
  return in;
}