#pragma once

#include <utt/RegAuth.h>
#include <utt/Params.h>
#include <utt/Wallet.h>
#include <utt/Tx.h>

#include <functional>

namespace libutt::Client {

size_t calcBalance(const Wallet& w);
size_t calcBudget(const Wallet& w);

using CoinStrategy = std::function<Tx(const Wallet&, const std::string&, size_t)>;
extern CoinStrategy k_CoinStrategyPreferExactChange;

Tx createTxForPayment(const Wallet& w,
                      const std::string& pid,
                      size_t payment,
                      const CoinStrategy& strategy = k_CoinStrategyPreferExactChange);

}  // namespace libutt::Client