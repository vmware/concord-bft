#include "transaction.hpp"
#include <utt/Tx.h>
#include <utt/Serialization.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>
#include <utt/Coin.h>
#include <utt/Address.h>
#include <utt/DataUtils.hpp>

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Transaction& tx) {
  out << *(tx.tx_) << std::endl;
  libutt::serializeVector(out, tx.input_coins_);
  out << tx.budget_coin_;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Transaction& tx) {
  in >> *(tx.tx_);
  libff::consume_OUTPUT_NEWLINE(in);
  libutt::deserializeVector(in, tx.input_coins_);
  in >> tx.budget_coin_;
  return in;
}
namespace libutt::api::operations {
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
    const auto& c = coins[i];
    input_coins[i] = *(c.coin_);
  }
  std::optional<libutt::Coin> budget_coin = std::nullopt;
  if (bc.has_value()) budget_coin.emplace(*(bc->coin_));
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
  tx_.reset(new libutt::Tx(d.getParams(),
                           fr_pidhash,
                           cid.getPid(),
                           *(rcm.first.comm_),
                           rcm_sig,
                           prf,
                           input_coins,
                           budget_coin,
                           fr_recipients,
                           std::nullopt,
                           rpk.vk,
                           encryptor));
}
Transaction::Transaction() { tx_.reset(new libutt::Tx()); }
Transaction::Transaction(const Transaction& other) {
  tx_.reset(new libutt::Tx());
  *this = other;
}
Transaction::Transaction(Transaction&& other) { *this = std::move(other); }
Transaction& Transaction::operator=(const Transaction& other) {
  if (this == &other) return *this;
  *tx_ = *(other.tx_);
  input_coins_ = other.input_coins_;
  budget_coin_ = other.budget_coin_;
  return *this;
}
Transaction& Transaction::operator=(Transaction&& other) {
  if (this == &other) return *this;
  tx_ = std::move(other.tx_);
  input_coins_ = std::move(other.input_coins_);
  budget_coin_ = std::move(other.budget_coin_);
  return *this;
}
std::vector<std::string> Transaction::getNullifiers() const { return tx_->getNullifiers(); }
const std::vector<Coin>& Transaction::getInputCoins() const { return input_coins_; }
std::optional<Coin> Transaction::getBudgetCoin() const { return budget_coin_; }
}  // namespace libutt::api::operations