#include <utt/Configuration.h>

#include <utt/Simulation.h>

#include <functional>
#include <queue>

void pickCoinsVaraint1(const Simulation::Context& ctx,
                       Wallet& w1,
                       Wallet& w2,
                       size_t payment,
                       std::set<std::string>& nullset,
                       std::vector<Wallet>& wallets) {
  // Precondition: 0 < payment <= budget <= balance

  // Variant 1: Prefer exact payments (using sorted coins)
  //
  // (1) look for a single coin where value >= k, an exact coin will be preferred
  // (2) look for two coins with total value >= k, an exact sum will be preferred
  // (3) no two coins sum up to k, do a merge on the largest two coins

  // Example 1 (1 coin match):
  // Target Payment: 5
  // Wallet: [2, 3, 4, 4, 7, 8]
  //
  // (1) find lower bound (LB) of 5: [2, 3, 4, 4, 7, 8] --> found a coin >= 5
  //                                              ^
  // (2) Make a one coin payment with 7
  // Note: If we prefer to pay with two exact coins (if possible) we can go to example 2
  // by considering the subrange [2, 3, 4, 4] and skip step (1) of example 2.

  // Example 2 (2 coin matches):
  // Target Payment: 5
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
  // Note: We use exact match over an inexact match and if neither exist we merge the
  // top two coins and try again.

  while (true) {
    using CoinRef = std::pair<size_t, size_t>;  // [coinValue, coinIdx]

    std::vector<CoinRef> aux;

    for (size_t i = 0; i < w1.coins.size(); ++i) {
      aux.emplace_back(w1.coins[i].getValue(), i);
    }

    auto cmpCoinValue = [](const CoinRef& lhs, const CoinRef& rhs) { return lhs.first < rhs.first; };
    std::sort(aux.begin(), aux.end(), cmpCoinValue);

    std::stringstream ss;
    ss << "'" << w1.ask.pid << "' Wallet: [";
    for (const auto& c : aux) {
      ss << "(" << c.first << ", " << c.second << ")";
    }
    ss << "]\n";
    loginfo << ss.str();

    auto lb = std::lower_bound(aux.begin(), aux.end(), CoinRef{payment, -1}, cmpCoinValue);
    if (lb != aux.end()) {
      // We can pay with one coin
      if (lb->first > payment) {
        Simulation::doPayment_1t2(ctx, w1, w2, lb->second, payment, nullset, wallets);
      } else {
        assertEqual(lb->first, payment);
        Simulation::doPayment_1t1(ctx, w1, w2, lb->second, nullset, wallets);
      }
      break;  // Done
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
        if (sum == payment) {
          exactMatch = std::make_pair(aux[low].second, aux[high].second);
          break;  // exact found
        } else if (sum > payment) {
          // found a pair (but we want to look for an exact match)
          if (!match) match = std::make_pair(aux[low].second, aux[high].second);
          --high;
        } else {
          ++low;
        }
      }

      if (exactMatch) {
        Simulation::doPayment_2t1(ctx, w1, w2, exactMatch->first, exactMatch->second, nullset, wallets);
        break;  // Done
      } else if (match) {
        Simulation::doPayment_2t2(ctx, w1, w2, match->first, match->second, payment, nullset, wallets);
        break;  // Done
      } else {
        // At this point no two distinct coins sum up to payment
        // We must merge the top two coins
        const auto lastIdx = aux.size() - 1;
        Simulation::doCoinMerge(ctx, w1, aux[lastIdx - 1].second, aux[lastIdx].second, nullset, wallets);
      }
    }
  }
}

void pickCoinsVaraint2(const Simulation::Context& ctx,
                       Wallet& w1,
                       Wallet& w2,
                       size_t payment,
                       std::set<std::string>& nullset,
                       std::vector<Wallet>& wallets) {
  // Precondition: 0 < payment <= budget <= balance

  // Variant 2: Consider only the highest two coins
  // (1) Use either of the two coins if it's an exact one-coin payment
  // (2) Use both coins if the sum satisfies payment
  // (3) Merge the two coins otherwise (sum < payment)

  while (true) {
    std::stringstream ss;
    ss << "'" << w1.ask.pid << "' Wallet: [";
    for (const auto& c : w1.coins) {
      ss << c.getValue() << ',';
    }
    ss << "]\n";
    loginfo << ss.str();

    // Special case
    if (w1.coins.size() == 1) {
      const auto coinValue = w1.coins[0].getValue();
      testAssertGreaterThanOrEqual(coinValue, payment);
      if (coinValue > payment) {
        Simulation::doPayment_1t2(ctx, w1, w2, 0, payment, nullset, wallets);
      } else {
        Simulation::doPayment_1t1(ctx, w1, w2, 0, nullset, wallets);
      }
      break;  // Done
    }

    // Precondition: |coins| >= 2
    using CoinRef = std::pair<size_t, size_t>;  // [coinValue, coinIdx]
    // Setup a min heap
    auto cmpCoinValue = [](const CoinRef& lhs, const CoinRef& rhs) { return lhs.first > rhs.first; };
    std::priority_queue<CoinRef, std::vector<CoinRef>, decltype(cmpCoinValue)> aux(cmpCoinValue);

    for (size_t i = 0; i < w1.coins.size(); ++i) {
      aux.emplace(CoinRef{w1.coins[i].getValue(), i});
      if (aux.size() > 2) aux.pop();
    }

    // Get top two coins
    // c1 <= c2
    auto c1 = aux.top();
    aux.pop();
    auto c2 = aux.top();
    aux.pop();
    assertTrue(aux.empty());

    loginfo << "c1=(" << c1.first << ',' << c1.second << ") c2=(" << c2.first << ',' << c2.second << ")\n";

    if (c1.first + c2.first >= payment) {  // We can do the payment
      if (c1.first == payment) {
        Simulation::doPayment_1t1(ctx, w1, w2, c1.second, nullset, wallets);
      } else if (c2.first == payment) {
        Simulation::doPayment_1t1(ctx, w1, w2, c2.second, nullset, wallets);
      } else if (c2.first > payment) {
        Simulation::doPayment_1t2(ctx, w1, w2, c2.second, payment, nullset, wallets);
      } else if (c1.first + c2.first == payment) {
        Simulation::doPayment_2t1(ctx, w1, w2, c1.second, c2.second, nullset, wallets);
      } else {  // > payment
        Simulation::doPayment_2t2(ctx, w1, w2, c1.second, c2.second, payment, nullset, wallets);
      }
      break;  // Done

    } else {  // We need to merge the coins
      Simulation::doCoinMerge(ctx, w1, c1.second, c2.second, nullset, wallets);
    }
  }
}

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  //////////////////////////////////////////////////////////////////////////////////////////////
  // CREATE DECENTRALIZED UTT SYSTEM
  //////////////////////////////////////////////////////////////////////////////////////////////

  Simulation::initialize();

  size_t n = 21;
  size_t thresh = 12;

  Simulation::Context ctx = Simulation::createContext(n, thresh);

  loginfo << "Created decentralized UTT system" << endl;

  //////////////////////////////////////////////////////////////////////////////////////////////
  // CREATE ACCOUNT WALLETS
  //////////////////////////////////////////////////////////////////////////////////////////////

  // Creates users and their wallets
  size_t numWallets = 10;
  // Each wallet is initialized with the following normal coins
  std::vector<size_t> normal_coin_values = {1, 2, 3, 4, 5, 10};
  size_t budget = 10000;  // Only one budget coin can be stored in the wallet currently

  std::vector<Wallet> wallets;
  for (size_t i = 0; i < numWallets; ++i)
    wallets.emplace_back(Simulation::createWallet(ctx, "user_" + std::to_string(i + 1), normal_coin_values, budget));

  loginfo << "Created random wallets" << endl;

  // nullifier list
  std::set<std::string> nullset;

  //////////////////////////////////////////////////////////////////////////////////////////////
  // SIMULATE
  //////////////////////////////////////////////////////////////////////////////////////////////

  assertGreaterThanOrEqual(wallets.size(), 2);

  size_t numCycles = 20;

  for (size_t cycle = 0; cycle < numCycles; cycle++) {
    loginfo << endl << endl;
    loginfo << "================ Cycle #" << (cycle + 1) << " ================" << endl;

    // Pick two distinct random wallets
    size_t i = static_cast<size_t>(rand()) % wallets.size();
    size_t rand_offset = 1 + (static_cast<size_t>(rand()) % (wallets.size() - 1));
    size_t j = (i + rand_offset) % wallets.size();
    assertNotEqual(i, j);

    auto& w1 = wallets[i];
    auto& w2 = wallets[j];
    Simulation::assertSerialization(w1);
    Simulation::assertSerialization(w2);

    const size_t balance = Simulation::calcBalance(w1);
    const size_t budget = Simulation::calcBudget(w1);
    const size_t maxPayment = std::min<size_t>(balance, budget);

    if (maxPayment == 0) {
      loginfo << "No payment possible. [balance=" << balance << "] [budget=" << budget << "] Skipping.\n";
      continue;
    }

    const size_t payment = static_cast<size_t>(rand()) % maxPayment + 1;  // [1 .. maxPayment]

    loginfo << "Target payment $" << payment << " from '" << w1.ask.pid << "' to '" << w2.ask.pid << "'\n";

    // Precondition: 0 < payment <= budget <= balance

    pickCoinsVaraint1(ctx, w1, w2, payment, nullset, wallets);

  }  // end for all cycles

  // Check total value in the system
  size_t wallet_value = 0;
  for (const auto& v : normal_coin_values) wallet_value += v;
  size_t total_value_expected = wallets.size() * wallet_value;

  size_t total_value_actual = 0;
  for (const auto& wallet : wallets) {
    for (const auto& c : wallet.coins) {
      total_value_actual += c.getValue();
    }
  }
  testAssertEqual(total_value_expected, total_value_actual);

  loginfo << "All is well." << endl;

  return 0;
}