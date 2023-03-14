#include "transaction.hpp"
#include "include/coin.impl.hpp"
#include "include/commitment.impl.hpp"
#include "include/transaction.impl.hpp"
#include "include/client.impl.hpp"
#include "include/params.impl.hpp"
#include <utt/Serialization.h>
#include <utt/DataUtils.hpp>

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Transaction& tx) {
  out << tx.pImpl_->tx_ << std::endl;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::operations::Transaction& tx) {
  in >> tx.pImpl_->tx_;
  return in;
}
namespace libutt::api::operations {
Transaction::Transaction(const UTTParams& d,
                         const Client& cid,
                         const std::vector<Coin>& coins,
                         const std::optional<Coin>& bc,
                         const std::vector<std::tuple<std::string, uint64_t>>& recipients,
                         const IEncryptor& encryptor) {
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
    input_coins[i] = c.pImpl_->c;
  }
  std::optional<libutt::Coin> budget_coin = std::nullopt;
  if (bc.has_value()) budget_coin.emplace(bc->pImpl_->c);
  std::vector<std::tuple<std::string, Fr>> fr_recipients(recipients.size());
  for (size_t i = 0; i < recipients.size(); i++) {
    // initiate the Fr types with the values given in the recipients vector (becasue the interanl Tx object gets
    // vector<Fr> as an input)
    const auto& [r_str, r_id] = recipients[i];
    auto& [id, fr] = fr_recipients[i];
    id = r_str;
    fr.set_ulong(r_id);
  }
  pImpl_ = new Transaction::Impl();
  pImpl_->tx_ = libutt::Tx(d.pImpl_->p,
                           fr_pidhash,
                           cid.getPid(),
                           rcm.first.pImpl_->comm_,
                           rcm_sig,
                           prf,
                           input_coins,
                           budget_coin,
                           fr_recipients,
                           std::nullopt,
                           cid.pImpl_->rpk_.vk,
                           encryptor);
}
Transaction::Transaction() { pImpl_ = new Transaction::Impl(); }
Transaction::Transaction(const Transaction& other) {
  pImpl_ = new Transaction::Impl();
  *this = other;
}
Transaction& Transaction::operator=(const Transaction& other) {
  if (this == &other) return *this;
  pImpl_->tx_ = other.pImpl_->tx_;
  return *this;
}
Transaction::~Transaction() { delete pImpl_; }

Transaction::Transaction(Transaction&& other) {
  pImpl_ = new Transaction::Impl();
  *this = std::move(other);
}
Transaction& Transaction::operator=(Transaction&& other) {
  pImpl_->tx_ = std::move(other.pImpl_->tx_);
  return *this;
}
std::vector<std::string> Transaction::getNullifiers() const { return pImpl_->tx_.getNullifiers(); }

uint32_t Transaction::getNumOfOutputCoins() const { return (uint32_t)pImpl_->tx_.outs.size(); }

bool Transaction::hasBudgetCoin() const { return pImpl_->tx_.ins.back().coin_type == libutt::Coin::BudgetType(); }
uint64_t Transaction::getBudgetExpirationDate() const {
  if (!hasBudgetCoin()) return 0;
  return pImpl_->tx_.ins.back().exp_date.as_ulong();
}
}  // namespace libutt::api::operations