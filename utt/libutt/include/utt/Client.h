#pragma once

#include <utt/RegAuth.h>
#include <utt/Params.h>
#include <utt/Wallet.h>
#include <utt/Tx.h>

#include <functional>

namespace libutt::Client {

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
size_t calcBalance(const Wallet& w);
size_t calcBudget(const Wallet& w);

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct CreateTxResult {
  std::string txType_ = "undefined";
  std::vector<size_t> inputNormalCoinValues_;
  std::optional<size_t> inputBudgetCoinValue_;
  std::map<std::string, size_t> recipients_;  // [pid : coinValue]
  Tx tx;
};

using CoinStrategy = std::function<CreateTxResult(const Wallet&, const std::string&, size_t)>;
extern CoinStrategy k_CoinStrategyPreferExactChange;

CreateTxResult createTxForPayment(const Wallet& w,
                                  const std::string& pid,
                                  size_t payment,
                                  const CoinStrategy& strategy = k_CoinStrategyPreferExactChange);

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct PruneCoinsResult {
  std::vector<size_t> spentCoins_;
  std::optional<size_t> spentBudgetCoin_;
};

PruneCoinsResult pruneSpentCoins(Wallet& w, const std::set<std::string>& nullset);

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct ClaimCoinResult {
  bool isBudgetCoin_ = false;
  size_t value_;
};

std::optional<ClaimCoinResult> tryClaimCoin(Wallet& w,
                                            const Tx& tx,
                                            size_t txoIdx,
                                            const std::vector<RandSigShare>& sigShares,
                                            const std::vector<size_t>& signerIds,
                                            size_t n);

}  // namespace libutt::Client