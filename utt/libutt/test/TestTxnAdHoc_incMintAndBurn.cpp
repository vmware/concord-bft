#include <utt/Configuration.h>

#include <utt/Simulation.h>
#include <utt/Client.h>
#include <utt/Replica.h>

#include <utt/MintOp.h>
#include <utt/BurnOp.h>

#include <functional>
#include <queue>

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
  size_t numWallets = 30;
  // Each wallet is initialized with the following normal coins
  std::vector<size_t> normal_coin_values = {};  //  {1, 2, 3, 4, 5, 10};
  size_t budget = 1800;                         // Only one budget coin can be stored in the wallet currently

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

  size_t numCycles = 500;

  size_t newMoney = 0;
  size_t burnedMoney = 0;

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

    size_t balance = Simulation::calcBalance(w1);

    if (balance > 0 && ((rand() % 100 + 1) <= 30)) {
      const size_t numOfCoins = w1.coins.size();

      assertTrue(numOfCoins > 0);

      size_t coinsToBurn = (size_t)(((size_t)rand() % numOfCoins) + 1);

      while (coinsToBurn > 0) {
        coinsToBurn--;

        assertTrue(w1.coins.size() > 0);
        assert(w1.coins[0].isNormal());

        loginfo << "Burning coin with " << w1.coins[0].getValue() << "$ for '" << w1.ask.pid << "'\n";

        BurnOp temp_BurnTx(ctx.p_, w1.ask, w1.coins[0], ctx.bpk_, ctx.rpk_);

        std::string oldHash = temp_BurnTx.getHashHex();
        std::stringstream ss;
        ss << temp_BurnTx;

        BurnOp burnTx(ss);
        auto newHash = burnTx.getHashHex();

        //          testAssertEqual(temp_BurnTx, burnTx);
        testAssertEqual(oldHash, newHash);

        loginfo << "Hash of burn operation is '" << newHash << "'\n";

        if (!burnTx.validate(ctx.p_, ctx.bpk_, ctx.rpk_) || burnTx.getOwnerPid() != w1.ask.getPid() ||
            burnTx.getValue() != w1.coins[0].getValue()) {
          testAssertFail("Burn op should have verified");
        }

        std::string nullif = burnTx.getNullifier();

        assertTrue(nullset.count(nullif) == 0);

        nullset.insert(nullif);

        const auto result = Client::pruneSpentCoins(w1, {nullif});

        assertTrue(result.spentCoins_.size() == 1);

        loginfo << "'" << w1.ask.pid << "' removing $" << result.spentCoins_[0] << " normal coin" << endl;
        burnedMoney += result.spentCoins_[0];
      }

      balance = Simulation::calcBalance(w1);
    }

    if (balance == 0) {
      const size_t newCoinValue = static_cast<size_t>(rand()) % 180 + 1;

      loginfo << "Creating new " << newCoinValue << "$ coin for '" << w1.ask.pid << "'\n";

      std::string simulatonOfUniqueTxHash =
          std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));

      MintOp temp_mintTx(simulatonOfUniqueTxHash, newCoinValue, w1.ask.pid);

      std::string oldHash = temp_mintTx.getHashHex();
      std::stringstream ss;
      ss << temp_mintTx;

      MintOp mintTx(ss);
      auto newHash = mintTx.getHashHex();

      testAssertEqual(temp_mintTx, mintTx);
      testAssertEqual(oldHash, newHash);

      loginfo << "Hash of mint operation is '" << newHash << "'\n";

      if (!mintTx.validate(simulatonOfUniqueTxHash, newCoinValue, w1.ask.pid)) {
        testAssertFail("Mint op should have verified");
      }

      std::vector<RandSigShare> replicaSignShares;

      // Each replica signs the output coins with its SK share
      for (RandSigShareSK& bskShare : ctx.bskShares_) {
        // RandSigShare shareSignCoin(const Params& p, const RandSigShareSK& bskShare) const;

        RandSigShare rss = mintTx.shareSignCoin(bskShare);

        if (!mintTx.verifySigShare(rss, bskShare.toPK())) {
          testAssertFail("....");
        }

        replicaSignShares.emplace_back(rss);
      }

      // Sample a subset of sigshares to aggregate
      std::vector<RandSigShare> sigShareSubset;
      std::vector<size_t> signerIdSubset = random_subset(ctx.thresh_, ctx.n_);
      for (auto id : signerIdSubset) {
        sigShareSubset.push_back(replicaSignShares.at(id));
      }

      Coin newCoin = mintTx.claimCoin(ctx.p_, w1.ask, ctx.n_, sigShareSubset, signerIdSubset, ctx.bpk_);

      w1.addNormalCoin(newCoin);

      newMoney += newCoinValue;

      balance = Simulation::calcBalance(w1);

      assertTrue(balance > 0);
    }

    const size_t budget = Simulation::calcBudget(w1);
    const size_t maxPayment = std::min<size_t>(balance, budget);

    if (maxPayment == 0) {
      loginfo << "No payment possible. [balance=" << balance << "] [budget=" << budget << "] Skipping.\n";
      continue;
    }

    const size_t payment = static_cast<size_t>(rand()) % maxPayment + 1;  // [1 .. maxPayment]

    loginfo << "Target payment $" << payment << " from '" << w1.ask.pid << "' to '" << w2.ask.pid << "'\n";

    // Precondition: 0 < payment <= budget <= balance

    while (true) {
      auto result = Client::createTxForPayment(w1, w2.getUserPid(), payment);
      loginfo << "Create " << result.txType_ << " tx\n";
      for (const auto& [pid, value] : result.recipients_) {
        if (pid != w1.ask.pid) loginfo << "'" << w1.ask.pid << "' sends $" << value << " to '" << pid << "'" << endl;
      }

      const auto& clientTx = result.tx;

      // Replicas receive, validate and sign the proposed transaction
      std::vector<std::vector<RandSigShare>> replicaSignShares;
      {
        // Simulate client sending a valid transaction to replicas
        auto replicaTx = Simulation::sendValidTxOnNetwork(ctx, clientTx);

        // Mark input coins as spent
        Simulation::addNullifiers(replicaTx, nullset);

        // Each replica signs the output coins with its SK share
        for (auto& bskShare : ctx.bskShares_) {
          replicaSignShares.emplace_back(Replica::signShareOutputCoins(replicaTx, bskShare));
        }
      }

      // Prune spent coins
      for (auto& w : wallets) {
        const auto result = Client::pruneSpentCoins(w, nullset);
        for (const size_t value : result.spentCoins_)
          loginfo << "'" << w.ask.pid << "' removing $" << value << " normal coin" << endl;
        if (result.spentBudgetCoin_)
          loginfo << "'" << w.ask.pid << "' removing $" << *result.spentBudgetCoin_ << " budget coin" << endl;
      }

      // Claim output coins
      for (size_t txoIdx = 0; txoIdx < clientTx.outs.size(); txoIdx++) {
        // Sample a subset of sigshares to aggregate
        std::vector<RandSigShare> sigShareSubset;
        std::vector<size_t> signerIdSubset = random_subset(ctx.thresh_, ctx.n_);
        for (auto id : signerIdSubset) {
          sigShareSubset.push_back(replicaSignShares.at(id).at(txoIdx));
        }

        // Ensure only one of the wallets identifies this output as theirs and claims it
        logtrace << "Trying to claim output #" << txoIdx << " on each wallet" << endl;
        bool claimed = false;
        for (size_t j = 0; j < wallets.size(); j++) {
          auto result = Client::tryClaimCoin(wallets.at(j), clientTx, txoIdx, sigShareSubset, signerIdSubset, ctx.n_);

          if (result) {
            assertFalse(claimed);
            loginfo << "Adding a $" << result->value_ << " '" << (result->isBudgetCoin_ ? "budget" : "normal")
                    << "' coin to wallet #" << j + 1 << " for '" << wallets[j].ask.pid << "'" << endl;

            claimed = true;
          }
        }
        assertTrue(claimed);
      }

      const bool isPayment = clientTx.isBudgeted();
      if (isPayment) break;  // Done
    }
  }  // end for all cycles

  // Check total value in the system
  size_t wallet_value = 0;
  for (const auto& v : normal_coin_values) wallet_value += v;
  size_t total_value_expected = wallets.size() * wallet_value + newMoney - burnedMoney;

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