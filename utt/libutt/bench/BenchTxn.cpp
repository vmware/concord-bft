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

/**
 *
 */
class BenchTxn {
 public:
  constexpr static size_t numWallets = 2, numCoins = 2, maxDenom = 100;

 public:
  size_t thresh, n;
  Factory f;
  Params p;
  RegAuthPK rpk;
  RandSigPK bpk;
  std::vector<RandSigShareSK> bskShares;
  size_t numIters;
  size_t budget;
  std::vector<Wallet> w;  // the two wallets for benchmarking TXNs
  AveragingTimer tc, tqv, tv, tp, tim, tcm;

 public:
  /**
   * i.e., need exactlty 'thresh' signatures to aggregate
   */
  BenchTxn(size_t thresh, size_t n, size_t numIters, bool isBudgeted)
      : thresh(thresh),
        n(n),
        f(thresh, n),
        p(f.getParams()),
        rpk(f.getRegAuthPK()),
        bpk(f.getBankPK()),
        bskShares(f.getBankShareSKs()),
        numIters(numIters),
        budget(2 * maxDenom * numIters),
        w(f.randomWallets(numWallets, numCoins, maxDenom, budget)),
        tc("TXN creation     "),
        tqv("TXN QP-validation"),  // quickpay validation time
        tv("TXN validation   "),
        tp("TXN processing   "),
        tim("TXN 'is mine?'   "),
        tcm("TXN 'claim mine!'") {
    // WARNING: this code assumes two wallets are repeatedly paying each other
    assertEqual(numWallets, 2);

    // TODO: test no budgets too
    testAssertTrue(isBudgeted);
  }

 public:
  void bench() {
    loginfo << "Benchmarking TXNs in a t = " << thresh << " out of n = " << n << " setting" << endl;

    size_t txnSize = 0, oldTxnSize = 0;

    /**
     * w[0] pays w[1]
     * w[1] pays w[0]
     * and so on
     * so each wallet sends numIters / 2 TXNs
     * each TXN spends two normal coins and a budget coin
     */
    for (size_t i = 0; i < numIters; i++) {
      auto senderIdx = i % w.size();
      auto recipIdx = (i + 1) % w.size();
      auto& wallet_sender = w[senderIdx];
      auto& wallet_recip = w[recipIdx];
      auto& pid_recip = wallet_recip.getUserPid();

      //
      // Step 1: Measure TXN creation
      //
      tc.startLap();
      Tx tx = wallet_sender.spendTwoRandomCoins(pid_recip, true);
      tc.endLap();

      txnSize = tx.getSize();
      if (oldTxnSize != 0 && txnSize != oldTxnSize) {
        assertFail("I would've expected all TXNs to have the same size!");
      } else {
        oldTxnSize = txnSize;
      }

      //
      // Step 2: Measure TXN validation
      //
      tqv.startLap();
      if (!tx.quickPayValidate(p, bpk, rpk)) {
        testAssertFail("TXN should have QuickPay-verified");
      }
      tqv.endLap();

      tv.startLap();
      if (!tx.validate(p, bpk, rpk)) {
        testAssertFail("TXN should have verified");
      }
      tv.endLap();

      std::vector<std::vector<RandSigShare>> sigShares(tx.outs.size(), std::vector<RandSigShare>());

      // each replica computes its sigshares on ALL outputs
      for (size_t j = 0; j < thresh; j++) {
        //
        // Step 3: Measure TXN processing (as the time for one replica to sharesign ALL outputs)
        //
        tp.startLap();
        for (size_t txoIdx = 0; txoIdx < tx.outs.size(); txoIdx++) {
          auto sigShare = tx.shareSignCoin(txoIdx, bskShares.at(j));
          sigShares.at(txoIdx).push_back(sigShare);
        }
        tp.endLap();
      }

      for (size_t txoIdx = 0; txoIdx < tx.outs.size(); txoIdx++) {
        // first, sample a subset of sigshares to aggregate
        std::vector<RandSigShare> sigShareSubset = sigShares.at(txoIdx);
        std::vector<size_t> signerIdSubset;
        for (size_t j = 0; j < thresh; j++) {
          signerIdSubset.push_back(j);
        }

        size_t ownerIdx;
        assertStrictlyLessThan(senderIdx, 2);
        assertStrictlyLessThan(recipIdx, 2);
        // WARNING: Asssumes Wallet::spendTwoRandomCoins puts the recipient in the first output
        if (txoIdx == 0) {
          ownerIdx = recipIdx;
        } else if (txoIdx == 1 || txoIdx == 2) {
          ownerIdx = senderIdx;
        } else {
          testAssertFail("This benchmark was written for 2-to-2 budgeted TXNs");
        }
        size_t nonOwnerIdx = 1 - ownerIdx;

        //
        // Step 4: Measure TXN claim mine! time (includes sig aggregation time)
        //
        tcm.startLap();
        auto coin = tx.tryClaimCoin(p, txoIdx, w[ownerIdx].ask, n, sigShareSubset, signerIdSubset, bpk);
        assertTrue(coin.has_value());
        w[ownerIdx].addCoin(*coin);
        tcm.endLap();

        tim.startLap();
        coin = tx.tryClaimCoin(p, txoIdx, w[nonOwnerIdx].ask, n, sigShareSubset, signerIdSubset, bpk);
        assertFalse(coin.has_value());
        tim.endLap();
      }
    }

    logperf << tc << endl;
    logperf << tqv << endl;
    logperf << tv << endl;
    logperf << tp << endl;
    logperf << tim << endl;
    logperf << tcm << endl;
    logperf << "TXN size (bytes) : " << Utils::humanizeBytes(txnSize) << endl;
  }
};

int main(int argc, char* argv[]) {
  libutt::initialize(nullptr, 0);
  // srand(static_cast<unsigned int>(time(NULL)));
  (void)argc;
  (void)argv;

  size_t t = 2, n = 4, numIters = 10;

  if (argc > 1 && (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)) {
    cout << "Usage: " << argv[0] << " <t> <n> [<iters>]" << endl;
    cout << endl;
    cout << "Measures TXN performance on a (t, n) threshold setting by running <iters> TXNs" << endl;
    return 1;
  }

  if (argc > 1) t = static_cast<size_t>(std::stoi(argv[1]));
  if (argc > 2) n = static_cast<size_t>(std::stoi(argv[2]));
  if (argc > 3) numIters = static_cast<size_t>(std::stoi(argv[3]));

  testAssertStrictlyLessThan(t, n);

  BenchTxn b(t, n, numIters, true);

  // TODO: txn quick pay validation time
  b.bench();

  loginfo << "All is well." << endl;

  return 0;
}
