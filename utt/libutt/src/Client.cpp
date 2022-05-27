
#include <utt/Client.h>

namespace libutt::Client {

//////////////////////////////////////////////////////////////////////////////////////////////
CreateTxResult createTx_1t1(const Wallet& w, size_t coinIdx, const std::string& pid) {
  std::vector<Coin> inputCoins = std::vector<Coin>{w.coins.at(coinIdx)};
  auto budgetCoin = *w.budgetCoin;  // copy

  Fr payment = inputCoins[0].val;

  std::vector<std::tuple<std::string, Fr>> recip;
  recip.emplace_back(pid, payment);

  CreateTxResult result;
  result.txType_ = "1-to-1 transfer";
  result.inputNormalCoinValues_.emplace_back(inputCoins[0].getValue());
  result.inputBudgetCoinValue_ = budgetCoin.getValue();
  result.outputBudgetCoinValue_ = budgetCoin.getValue() - payment.as_ulong();
  result.recipients_.emplace(pid, payment.as_ulong());
  result.tx = Tx(w.p, w.ask, inputCoins, budgetCoin, recip, w.bpk, w.rpk);

  return result;
}

//////////////////////////////////////////////////////////////////////////////////////////////
CreateTxResult createTx_1t2(const Wallet& w, size_t coinIdx, size_t payment, const std::string& pid) {
  std::vector<Coin> inputCoins = std::vector<Coin>{w.coins.at(coinIdx)};
  auto budgetCoin = *w.budgetCoin;  // copy

  Fr value1;
  value1.set_ulong(payment);

  Fr value2 = inputCoins[0].val - value1;

  std::vector<std::tuple<std::string, Fr>> recip;
  recip.emplace_back(pid, value1);
  recip.emplace_back(w.ask.getPid(), value2);

  CreateTxResult result;
  result.txType_ = "1-to-2 transfer";
  result.inputNormalCoinValues_.emplace_back(inputCoins[0].getValue());
  result.inputBudgetCoinValue_ = budgetCoin.getValue();
  result.outputBudgetCoinValue_ = budgetCoin.getValue() - payment;
  result.recipients_.emplace(pid, value1.as_ulong());
  result.recipients_.emplace(w.getUserPid(), value2.as_ulong());
  result.tx = Tx(w.p, w.ask, inputCoins, budgetCoin, recip, w.bpk, w.rpk);

  return result;
}

//////////////////////////////////////////////////////////////////////////////////////////////
CreateTxResult createTx_2t1(const Wallet& w, size_t coinIdx1, size_t coinIdx2, const std::string& pid) {
  std::vector<Coin> inputCoins = std::vector<Coin>{w.coins.at(coinIdx1), w.coins.at(coinIdx2)};
  auto budgetCoin = *w.budgetCoin;  // copy

  Fr totalValue = inputCoins[0].val + inputCoins[1].val;

  std::vector<std::tuple<std::string, Fr>> recip;
  recip.emplace_back(pid, totalValue);

  CreateTxResult result;
  result.txType_ = "2-to-1 transfer";
  result.inputNormalCoinValues_.emplace_back(inputCoins[0].getValue());
  result.inputNormalCoinValues_.emplace_back(inputCoins[1].getValue());
  result.inputBudgetCoinValue_ = budgetCoin.getValue();
  result.outputBudgetCoinValue_ = budgetCoin.getValue() - totalValue.as_ulong();
  result.recipients_.emplace(pid, totalValue.as_ulong());
  result.tx = Tx(w.p, w.ask, inputCoins, budgetCoin, recip, w.bpk, w.rpk);

  return result;
}

//////////////////////////////////////////////////////////////////////////////////////////////
CreateTxResult createTx_2t2(const Wallet& w, size_t coinIdx1, size_t coinIdx2, size_t payment, const std::string& pid) {
  std::vector<Coin> inputCoins = std::vector<Coin>{w.coins.at(coinIdx1), w.coins.at(coinIdx2)};
  auto budgetCoin = *w.budgetCoin;  // copy

  Fr value1;
  value1.set_ulong(payment);

  Fr value2 = (inputCoins[0].val + inputCoins[1].val) - value1;

  std::vector<std::tuple<std::string, Fr>> recip;
  recip.emplace_back(pid, value1);
  recip.emplace_back(w.ask.getPid(), value2);

  CreateTxResult result;
  result.txType_ = "2-to-2 transfer";
  result.inputNormalCoinValues_.emplace_back(inputCoins[0].getValue());
  result.inputNormalCoinValues_.emplace_back(inputCoins[1].getValue());
  result.inputBudgetCoinValue_ = budgetCoin.getValue();
  result.outputBudgetCoinValue_ = budgetCoin.getValue() - payment;
  result.recipients_.emplace(pid, value1.as_ulong());
  result.recipients_.emplace(w.getUserPid(), value2.as_ulong());
  result.tx = Tx(w.p, w.ask, inputCoins, budgetCoin, recip, w.bpk, w.rpk);

  return result;
}

//////////////////////////////////////////////////////////////////////////////////////////////
CreateTxResult createTx_Self2t2(const Wallet& w, size_t coinIdx1, size_t coinIdx2, size_t amount) {
  std::vector<Coin> inputCoins = std::vector<Coin>{w.coins.at(coinIdx1), w.coins.at(coinIdx2)};

  Fr value1;
  value1.set_ulong(amount);

  Fr value2 = (inputCoins[0].val + inputCoins[1].val) - value1;

  std::vector<std::tuple<std::string, Fr>> recip;
  recip.emplace_back(w.ask.getPid(), value1);
  recip.emplace_back(w.ask.getPid(), value2);

  CreateTxResult result;
  result.txType_ = "coin-split";
  result.inputNormalCoinValues_.emplace_back(inputCoins[0].getValue());
  result.inputNormalCoinValues_.emplace_back(inputCoins[1].getValue());
  result.recipients_.emplace(w.getUserPid(), value1.as_ulong());
  result.recipients_.emplace(w.getUserPid(), value2.as_ulong());
  result.tx = Tx(w.p, w.ask, inputCoins, std::nullopt, recip, w.bpk, w.rpk);

  return result;
}

//////////////////////////////////////////////////////////////////////////////////////////////
CreateTxResult createTx_Self2t1(const Wallet& w, size_t coinIdx1, size_t coinIdx2) {
  std::vector<Coin> inputCoins = std::vector<Coin>{w.coins.at(coinIdx1), w.coins.at(coinIdx2)};

  Fr totalValue = inputCoins[0].val + inputCoins[1].val;

  std::vector<std::tuple<std::string, Fr>> recip;
  recip.emplace_back(w.ask.getPid(), totalValue);

  CreateTxResult result;
  result.txType_ = "coin-merge";
  result.inputNormalCoinValues_.emplace_back(inputCoins[0].getValue());
  result.inputNormalCoinValues_.emplace_back(inputCoins[1].getValue());
  result.recipients_.emplace(w.getUserPid(), totalValue.as_ulong());
  result.tx = Tx(w.p, w.ask, inputCoins, std::nullopt, recip, w.bpk, w.rpk);

  return result;
}

//////////////////////////////////////////////////////////////////////////////////////////////
CreateTxResult createTx_Self1t2(const Wallet& w, size_t coinIdx, size_t splitAmount) {
  std::vector<Coin> inputCoins = std::vector<Coin>{w.coins.at(coinIdx)};

  Fr value1;
  value1.set_ulong(splitAmount);

  Fr value2 = inputCoins[0].val - value1;

  std::vector<std::tuple<std::string, Fr>> recip;
  recip.emplace_back(w.ask.getPid(), value1);
  recip.emplace_back(w.ask.getPid(), value2);

  CreateTxResult result;
  result.txType_ = "coin-split";
  result.inputNormalCoinValues_.emplace_back(inputCoins[0].getValue());
  result.recipients_.emplace(w.getUserPid(), value1.as_ulong());
  result.recipients_.emplace(w.getUserPid(), value2.as_ulong());
  result.tx = Tx(w.p, w.ask, inputCoins, std::nullopt, recip, w.bpk, w.rpk);

  return result;
}

//////////////////////////////////////////////////////////////////////////////////////////////
size_t calcBalance(const Wallet& w) {
  size_t balance = 0;
  for (const auto& c : w.coins) balance += c.getValue();
  return balance;
}

//////////////////////////////////////////////////////////////////////////////////////////////
size_t calcBudget(const Wallet& w) { return w.budgetCoin ? w.budgetCoin->getValue() : 0; }

//////////////////////////////////////////////////////////////////////////////////////////////
auto k_CoinStrategyPreferExactMatch = [](const std::vector<Coin>& coins, size_t targetAmount) -> std::vector<size_t> {
  // Precondition: 0 < payment <= budget <= balance

  // Variant 1: Prefer exactly matching coins (using sorted coins)
  //
  // (1) look for a single coin where value >= k, an exact coin will be preferred
  // (2) look for two coins with total value >= k, an exact sum will be preferred
  // (3) no two coins sum up to k, do a merge on the largest two coins

  // Example 1 (1 coin match):
  // Target Amount: 5
  // Wallet: [2, 3, 4, 4, 7, 8]
  //
  // (1) find lower bound (LB) of 5: [2, 3, 4, 4, 7, 8] --> found a coin >= 5
  //                                              ^
  // (2) Use a single coin of 7
  // Note: If we prefer to use two exact coins (if possible) we can go to example 2
  // by considering the subrange [2, 3, 4, 4] and skip step (1) of example 2.

  // Example 2 (2 coin matches):
  // Target Amount: 5
  // Wallet: [2, 3, 4, 4]
  //
  // (1) find lower bound (LB) of 5: [2, 3, 4, 4, end] -- we don't have a single coin candidate
  //                                               ^
  // (2) search for pairs >= 5
  // [2, 3, 4, 4]
  //  l        h      l+h = 6 found a match -- save and continue iterating
  //  l     h         l+h = 6 found a match -- ignore, already has a match
  //  l  h            l+h = 5 found exact match -- save and break (we prefer the exact match)
  //  Termination: l == h
  //
  // (3) Use exact match
  // Note: We use exact match over an inexact match and if neither exists we merge the
  // top two coins and try again.

  using CoinRef = std::pair<size_t, size_t>;  // [coinValue, coinIdx]

  std::vector<CoinRef> aux;

  for (size_t i = 0; i < coins.size(); ++i) {
    aux.emplace_back(coins[i].getValue(), i);
  }

  auto cmpCoinValue = [](const CoinRef& lhs, const CoinRef& rhs) { return lhs.first < rhs.first; };
  std::sort(aux.begin(), aux.end(), cmpCoinValue);

  auto lb = std::lower_bound(aux.begin(), aux.end(), CoinRef{targetAmount, -1}, cmpCoinValue);
  if (lb != aux.end()) {
    // We can pay with one coin (>= targetAmount)
    return std::vector<size_t>{lb->second};
  } else {  // Try to pay with two coins
    // We know that our balance is enough and no coin is >= payment (because lower_bound == end)
    // Then we may have two coins that satisfy the payment
    // If we don't have two coins we must merge
    size_t low = 0;
    size_t high = aux.size() - 1;
    std::optional<std::pair<size_t, size_t>> match;
    std::optional<std::pair<size_t, size_t>> exactMatch;

    while (low < high) {
      const auto sum = aux[low].first + aux[high].first;
      if (sum == targetAmount) {
        exactMatch = std::make_pair(aux[low].second, aux[high].second);
        break;  // exact found
      } else if (sum > targetAmount) {
        // found a pair (but we want to look for an exact match)
        if (!match) match = std::make_pair(aux[low].second, aux[high].second);
        --high;
      } else {
        ++low;
      }
    }

    // Prefer an exact match over an inexact
    if (exactMatch) {
      return std::vector<size_t>{exactMatch->first, exactMatch->second};
    } else if (match) {
      return std::vector<size_t>{match->first, match->second};
    }
  }

  // At this point no one or two coins are sufficient for the target amount
  // so we merge the top two coins
  const auto lastIdx = aux.size() - 1;
  return std::vector<size_t>{aux[lastIdx - 1].second, aux[lastIdx].second};
};

//////////////////////////////////////////////////////////////////////////////////////////////
CreateTxResult createTxForPayment(const Wallet& w, const std::string& pid, size_t payment) {
  if (w.coins.empty()) throw std::runtime_error("Wallet has no coins!");
  if (pid.empty()) throw std::runtime_error("Empty pid!");
  if (payment == 0) throw std::runtime_error("Payment must be positive!");

  const size_t balance = calcBalance(w);
  if (balance < payment) throw std::runtime_error("Wallet has insufficient balance!");
  const size_t budget = calcBudget(w);
  if (budget < payment) throw std::runtime_error("Wallet has insufficient anonymous budget!");

  auto coins = k_CoinStrategyPreferExactMatch(w.coins, payment);
  if (coins.empty()) throw std::runtime_error("Coin strategy didn't pick any coins!");

  if (coins.size() == 1) {
    const auto value = w.coins.at(coins[0]).getValue();
    if (value < payment) throw std::runtime_error("Coin strategy picked a single insufficient coin!");
    if (value == payment) return createTx_1t1(w, coins[0], pid);
    return createTx_1t2(w, coins[0], payment, pid);  // value > payment
  } else if (coins.size() == 2) {
    const auto value = w.coins.at(coins[0]).getValue() + w.coins.at(coins[1]).getValue();
    if (value < payment) return createTx_Self2t1(w, coins[0], coins[1]);  // Coin merge
    if (value == payment) return createTx_2t1(w, coins[0], coins[1], pid);
    return createTx_2t2(w, coins[0], coins[1], payment, pid);  // value > payment
  } else {
    throw std::runtime_error("Coin strategy picked more than two coins!");
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////
CreateBurnTxResult createTxForBurn(const Wallet& w, size_t amount) {
  if (w.coins.empty()) throw std::runtime_error("Wallet has no coins!");
  if (amount == 0) throw std::runtime_error("Payment must be positive!");

  const size_t balance = calcBalance(w);
  if (balance < amount) throw std::runtime_error("Wallet has insufficient balance!");

  auto coins = k_CoinStrategyPreferExactMatch(w.coins, amount);
  if (coins.empty()) throw std::runtime_error("Coin strategy didn't pick any coins!");

  if (coins.size() == 1) {
    const auto value = w.coins.at(coins[0]).getValue();
    if (value < amount) throw std::runtime_error("Coin strategy picked a single insufficient coin!");
    if (value == amount) return BurnOp(w.p, w.ask, w.coins.at(coins[0]), w.bpk, w.rpk);
    return createTx_Self1t2(w, coins[0], amount);  // value > payment
  } else if (coins.size() == 2) {
    const auto value = w.coins.at(coins[0]).getValue() + w.coins.at(coins[1]).getValue();
    if (value <= amount) return createTx_Self2t1(w, coins[0], coins[1]);  // Coin merge
    return createTx_Self2t2(w, coins[0], coins[1], amount);               // value > payment
  } else {
    throw std::runtime_error("Coin strategy picked more than two coins!");
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////
PruneCoinsResult pruneSpentCoins(Wallet& w, const std::set<std::string>& nullset) {
  PruneCoinsResult result;

  for (auto& c : w.coins) {
    if (nullset.count(c.null.toUniqueString()) > 0) {
      result.spentCoins_.emplace_back(c.getValue());
      c.val = 0;
    }
  }

  w.coins.erase(std::remove_if(w.coins.begin(), w.coins.end(), [](const libutt::Coin& c) { return c.val == 0; }),
                w.coins.end());

  if (w.budgetCoin && nullset.count(w.budgetCoin->null.toUniqueString()) > 0) {
    result.spentBudgetCoin_ = w.budgetCoin->getValue();
    w.budgetCoin.reset();
  }

  return result;
}

//////////////////////////////////////////////////////////////////////////////////////////////
std::optional<ClaimCoinResult> tryClaimCoin(Wallet& w,
                                            const Tx& tx,
                                            size_t txoIdx,
                                            const std::vector<RandSigShare>& sigShares,
                                            const std::vector<size_t>& signerIds,
                                            size_t n) {
  auto& txo = tx.outs.at(txoIdx);

  Fr val;  // coin value
  Fr d;    // vcm_2 value commitment randomness
  Fr t;    // identity commitment randomness

  // decrypt the ciphertext
  bool forMe;
  AutoBuf<unsigned char> ptxt;
  std::tie(forMe, ptxt) = w.ask.e.decrypt(txo.ctxt);

  if (!forMe) {
    logtrace << "TXO #" << txoIdx << " is NOT for pid '" << w.ask.pid << "'!" << endl;
    return std::nullopt;
  } else {
    logtrace << "TXO #" << txoIdx << " is for pid '" << w.ask.pid << "'!" << endl;
  }

  // parse the plaintext as (value, vcm_2 randomness, icm randomness)
  auto vdt = bytesToFrs(ptxt);
  assertEqual(vdt.size(), 3);
  val = vdt[0];
  d = vdt[1];
  t = vdt[2];

  logtrace << "val: " << val << endl;
  logtrace << "d: " << d << endl;
  logtrace << "t: " << t << endl;

  // prepare to aggregate & unblind signature and store into Coin object

  // assemble randomness vector for unblinding the coin sig
  Fr r_pid = t, r_sn = Fr::zero(), r_val = d, r_type = Fr::zero(), r_expdate = Fr::zero();

  std::vector<Fr> r = {r_pid, r_sn, r_val, r_type, r_expdate};

  // aggregate & unblind the signature
  testAssertFalse(sigShares.empty());
  testAssertFalse(signerIds.empty());
  testAssertEqual(sigShares.size(), signerIds.size());

  RandSig sig = RandSigShare::aggregate(n, sigShares, signerIds, w.p.getCoinCK(), r);

#ifndef NDEBUG
  {
    auto sn = tx.getSN(txoIdx);
    logtrace << "sn: " << sn << endl;
    Comm ccm = Comm::create(w.p.getCoinCK(),
                            {
                                w.ask.getPidHash(),
                                sn,
                                val,
                                txo.coin_type,
                                txo.exp_date,
                                Fr::zero()  // the recovered signature will be on a commitment w/ randomness 0
                            },
                            true);

    assertTrue(sig.verify(ccm, w.bpk));
  }
#endif

  // TODO(Perf): small optimization here would re-use vcm_1 (g_3^v g^z) instead of recommitting
  // ...but then we'd need to encrypt z too
  //
  // the signature from above is on commitment with r=0, but the Coin constructor
  // will re-randomize the ccm and also compute coin commitment, value commitment, nullifier
  Coin c(w.p.getCoinCK(), w.p.null, tx.getSN(txoIdx), val, txo.coin_type, txo.exp_date, w.ask);

  // re-randomize the coin signature
  Fr u_delta = Fr::random_element();
  c.sig = sig;
  c.sig.rerandomize(c.r, u_delta);

  assertTrue(c.hasValidSig(w.bpk));
  assertNotEqual(c.r, Fr::zero());  // should output a re-randomized coin always

  w.addCoin(c);  // Adds either a normal or budget coin

  return ClaimCoinResult{c.isBudget(), c.getValue()};
}

}  // namespace libutt::Client