#pragma once

#include <utt/RegAuth.h>
#include <utt/Params.h>
#include <utt/Wallet.h>
#include <utt/Tx.h>

#include <functional>

namespace libutt::Client {

size_t calcBalance(const Wallet& w);
size_t calcBudget(const Wallet& w);

struct CreateTxEvent {
  std::string txType_ = "undefined";
  std::vector<size_t> inputNormalCoinValues_;
  std::optional<size_t> inputBudgetCoinValue_;
  std::map<std::string, size_t> recipients_;  // [pid : coinValue]
};

struct ClaimEvent {
  bool isBudgetCoin_ = false;
  size_t value_;
};

struct PruneCoinsResult {
  std::vector<size_t> spentCoins_;
  std::optional<size_t> spentBudgetCoin_;
};

using CoinStrategy = std::function<Tx(const Wallet&, const std::string&, size_t, CreateTxEvent&)>;
extern CoinStrategy k_CoinStrategyPreferExactChange;

Tx createTxForPayment(const Wallet& w,
                      const std::string& pid,
                      size_t payment,
                      CreateTxEvent& outEvent,
                      const CoinStrategy& strategy = k_CoinStrategyPreferExactChange);

PruneCoinsResult pruneSpentCoins(Wallet& w, const std::set<std::string>& nullset);

void tryClaimCoin(Wallet& w,
                  const Tx& tx,
                  size_t txoIdx,
                  const std::vector<RandSigShare>& sigShares,
                  const std::vector<size_t>& signerIds,
                  size_t n,
                  std::optional<ClaimEvent>& outEvent);

}  // namespace libutt::Client