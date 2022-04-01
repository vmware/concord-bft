#include <utt/Configuration.h>

#include <utt/Address.h>
#include <utt/Coin.h>
#include <utt/Factory.h>
#include <utt/Params.h>
#include <utt/RandSig.h>
#include <utt/RandSigDKG.h>
#include <utt/RegAuth.h>
#include <utt/Tx.h>
#include <utt/Utils.h>
#include <utt/Wallet.h>

#include <cmath>
#include <ctime>
#include <iostream>
#include <fstream>
#include <random>
#include <stdexcept>
#include <vector>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>
#include <xutils/Timer.h>
#include <xutils/Utils.h>

using namespace std;
using namespace libutt;

void assertWalletDeserializes(Wallet& w1) {
  std::stringstream ss1;
  ss1 << w1 << std::endl;
  auto s1 = ss1.str();
  ss1 << w1 << std::endl;

  Wallet w2, w3;
  ss1 >> w2;
  libff::consume_OUTPUT_NEWLINE(ss1);
  ss1 >> w3;
  libff::consume_OUTPUT_NEWLINE(ss1);

  std::stringstream ss2, ss3;
  ss2 << w2 << std::endl;
  auto s2 = ss2.str();
  ss3 << w3 << std::endl;
  auto s3 = ss3.str();

  testAssertEqual(w1, w2);
  testAssertEqual(w1, w3);

  // actually return and try to use the deserialized wallet
  w1 = w2;
}

/**
 * This function tests a payment "cycle":
 *  - There are 3 users
 *  - Each has a wallet with two random coins and (a lot of budget)
 *  - 'user1' takes his two coins and creates a TX paying 'user2' one coin and getting back a coin as change
 *  - 'user2' takes his two coins and creates a TX  paying 'user3' one coin and getting back a coin as change
 *  - 'user3' takes his two coins and creates a TX  paying 'user1' one coin and getting back a coin as change
 *  - At end of this "cycle" all users have two coins in the wallet and this can be repeated!
 */
void testBudgeted2to2Txn(size_t thresh, size_t n, size_t numCycles, bool isBudgeted, bool isQuickPay) {
  // TODO: test no budgets too
  testAssertTrue(isBudgeted);

  // NOTE: Already tested (de)serialization of: Params, RandSigPK,
  // RandSigShareSK, RandSigSharePK.
  //
  // We do NOT need to serialize RandSigSK and RegAuthSK for our benchmarking purposes.
  Factory f(thresh, n);

  Params p;

  // ..but still wanna make sure deserialized params work for validating TXNs
  std::stringstream ssp;
  ssp << f.getParams();
  ssp >> p;

  RegAuthPK rpk = f.getRegAuthPK();
  RandSigPK bpk = f.getBankPK();
  std::vector<RandSigShareSK> bskShares = f.getBankShareSKs();

  logdbg << "Created decentralized UTT system" << endl;

  // Creates users and their wallets
  size_t numWallets = 3, numCoins = 2;
  size_t maxDenom = 100;
  size_t budget = maxDenom * numCycles;
  std::vector<Wallet> w = f.randomWallets(numWallets, numCoins, maxDenom, budget);
  logdbg << "Created random wallets" << endl;

  // nullifer list
  std::set<std::string> nullset;

  for (size_t cycle = 0; cycle < numCycles; cycle++) {
    logdbg << endl;
    logdbg << "Payment cycle #" << cycle << endl;
    logdbg << endl;
    for (size_t i = 0; i < w.size(); i++) {
      assertWalletDeserializes(w[i]);

      auto& wallet_recip = w[(i + 1) % w.size()];
      auto& pid_recip = wallet_recip.getUserPid();

      // NOTE: We remove spent coins from wallet and will add newly-minted coins to wallet
      // (BFT clients will use a cycle of two wallets to simulate Alice paying Bob by simply paying himself across these
      // two wallets)
      Tx tx_tmp = w[i].spendTwoRandomCoins(pid_recip, true);

      logdbg << "Created TXN!" << endl;

      // test (de)serialiazation, since Tx's are sent over the network
      auto oldHash = tx_tmp.getHashHex();
      std::stringstream ss;
      ss << tx_tmp;

      Tx tx(ss);
      // ss >> tx;
      auto newHash = tx.getHashHex();

      testAssertEqual(tx_tmp, tx);
      testAssertEqual(oldHash, newHash);

      if (isQuickPay) {
        // check transaction validates
        if (!tx.quickPayValidate(p, bpk, rpk)) {
          testAssertFail("TXN should have QuickPay-verified");
        }
      } else {
        if (!tx.validate(p, bpk, rpk)) {
          testAssertFail("TXN should have verified");
        }
      }

      logdbg << "Validated TXN!" << endl;

      // fetch nullifiers (includes budget coin nullifiers, if budgeted) and add to nullifier set
      // NOTE: we'll share the same nullifer set for both budget and normal coins
      for (auto& nullif : tx.getNullifiers()) {
        // make sure then nullifier is not in the list
        testAssertEqual(nullset.count(nullif), 0);

        // add the nullifier to the list
        nullset.insert(nullif);
      }

      // replicas: go through every TX output and sign it
      for (size_t txoIdx = 0; txoIdx < tx.outs.size(); txoIdx++) {
        // each replica returns its sigshare on this output
        std::vector<RandSigShare> sigShares;
        for (auto& bskShare : bskShares) {
          auto sigShare_tmp = tx.shareSignCoin(txoIdx, bskShare);

          // check this verifies before serialization
          testAssertTrue(tx.verifySigShare(txoIdx, sigShare_tmp, bskShare.toPK()));

          // test (de)serialiazation, since RandSigShares are sent over the network
          std::stringstream ss;
          ss << sigShare_tmp;

          RandSigShare sigShare;
          ss >> sigShare;

          // check this is the same after deserialization
          testAssertEqual(sigShare, sigShare_tmp);

          sigShares.push_back(sigShare);
        }

        // first, sample a subset of sigshares to aggregate
        std::vector<RandSigShare> sigShareSubset;
        std::vector<size_t> signerIdSubset = random_subset(thresh, n);
        for (auto id : signerIdSubset) {
          sigShareSubset.push_back(sigShares.at(id));
        }

        // next, ensure only one of the wallets identifies this output as theirs
        size_t foundIdx = w.size();
        std::optional<Coin> coin;
        logtrace << "Trying to claim output #" << txoIdx << " on each wallet" << endl;
        for (size_t j = 0; j < w.size(); j++) {
          // note that we're doing everything in this one call: checking the TxOut is
          // for me, aggregating the signature shares, unblinding, and returning
          // the final Coin object
          //
          // NOTE: Coin commitment will be re-randomized and ready to spend!
          coin = tx.tryClaimCoin(p, txoIdx, w[j].ask, n, sigShareSubset, signerIdSubset, bpk);

          if (coin.has_value()) {
            foundIdx = j;
            logdbg << "Adding a $" << coin->val << " '" << coin->getType() << "' coin to wallet #" << j + 1 << " for '"
                   << w[j].ask.pid << "'" << endl;
            w[j].addCoin(*coin);  // add the coin back to the wallet
            break;
          }
        }
        testAssertTrue(coin.has_value());
        testAssertNotEqual(foundIdx, w.size());

        // assert all of the other wallets do not think this output is theirs
        for (size_t j = 0; j < w.size(); j++) {
          if (j != foundIdx) {
            auto coin = tx.tryClaimCoin(p, txoIdx, w[j].ask, n, {}, {}, bpk);
            testAssertFalse(coin.has_value());
          }
        }

      }  // end for all txouts
    }    // end for all wallets
  }      // end for all cycles
}

int main(int argc, char* argv[]) {
  libutt::initialize(nullptr, 0);
  // srand(static_cast<unsigned int>(time(NULL)));
  (void)argc;
  (void)argv;

  testBudgeted2to2Txn(12, 21, 3, true, true);
  testBudgeted2to2Txn(12, 21, 3, true, false);

  loginfo << "All is well." << endl;

  return 0;
}
