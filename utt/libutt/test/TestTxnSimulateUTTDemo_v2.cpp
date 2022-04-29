#include <utt/Configuration.h>

#include <utt/Simulation.h>
#include <utt/Client.h>
#include <utt/Replica.h>

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
      loginfo << "No payment possible. [balance=" << balance << "] [budget=" << budget << "] Skipping.";
      continue;
    }

    const size_t payment = static_cast<size_t>(rand()) % maxPayment + 1;  // [1 .. maxPayment]

    loginfo << "Target payment $" << payment << " from '" << w1.ask.pid << "' to '" << w2.ask.pid << "'\n";

    // Precondition: 0 < payment <= budget <= balance

    Client::CreateTxEvent createTxEvent;
    auto clientTx = Client::createTxForPayment(w1, w2.getUserPid(), payment, createTxEvent);

    for (const size_t value : createTxEvent.inputNormalCoinValues_)
      loginfo << "'" << w1.ask.pid << "' removing $" << value << " coin" << endl;

    for (const auto& [pid, value] : createTxEvent.recipients_) {
      if (pid != w1.ask.pid) loginfo << "'" << w1.ask.pid << "' sends $" << value << " to '" << pid << "'" << endl;
    }

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
      auto result = Client::pruneSpentCoins(w, nullset);
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
        std::optional<Client::ClaimEvent> claimEvent;
        Client::tryClaimCoin(wallets.at(j), clientTx, txoIdx, sigShareSubset, signerIdSubset, ctx.n_, claimEvent);

        if (claimEvent) {
          assertFalse(claimed);
          loginfo << "Adding a $" << claimEvent->value_ << " '" << (claimEvent->isBudgetCoin_ ? "budget" : "normal")
                  << "' coin to wallet #" << j + 1 << " for '" << wallets[j].ask.pid << "'" << endl;

          claimed = true;
        }
      }
      assertTrue(claimed);
    }

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