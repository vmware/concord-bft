#include <utt/Configuration.h>

#include <utt/PolyCrypto.h>
#include <utt/PolyOps.h>
#include <utt/Utt.h>

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
using namespace libfqfft;
using namespace libutt;

void testCoinSig() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<long> udi(1, 1024);

  auto p = libutt::Params::Random();  // randomly generated parameters
  CoinSecrets c(p, udi(gen));         // randomly generated coin

  std::cout << "Random coin value: " << c.val << endl;

  CoinComm cc(p, c);  // commit to the coin using the specified parameters

  testAssertTrue(cc.hasCorrectG2(p));

  std::cout << "Pedersen in G1: " << cc.ped1 << endl;
  std::cout << "Pedersen in G2: " << cc.ped2 << endl;

  // sign the coin
  BankSK sk = BankSK::random();
  CoinSig scc = sk.thresholdSignCoin(p, cc);

  auto pk = sk.toPK(p);
  bool isSigValid = scc.verify(p, cc, pk);

  testAssertTrue(isSigValid);
}

void testBudgeted2to2Txn(size_t t, size_t n) {
  BankPK bpk;
  std::vector<BankSK> bsk;
  RegAuthSK rsk;
  RegAuthPK rpk;
  Params p;

  std::tie(bsk, bpk, rsk, rpk, p) = Factory::randomCentralizedUTT();

  // Creates users and their wallets
  size_t numWallets = 3, maxBal = 1000;
  std::vector<Wallet> w = Factory::randomWallets(bsk, rsk, p, numWallets, maxBal);

  // nullifer list
  std::set<std::string> nl;

  for (size_t i = 0; i < w.size(); i++) {
    Fr val = (rand() % w.totalBalance()) + 1;  // never sends zero coins

    auto& w_recip = w[(i + 1) % w.size()];
    auto& pid_recip = w_recip.getUserPid();

    // TODO: should this get rid of those coins from the wallet?
    // can create doubleSpendTxn call too
    Tx tx = w.createTxn(val, pid_recip, rpk);

    // checks inputs
    if (!tx.validate(p, bpk, rpk)) {
      testAssertFail("TXN should have verified");
    }

    // fetch nullifiers and add to set
    // NOTE: we'll share the same nullifer list for both budget and normal coins
    for (auto& nullif : tx.getNullifiers()) {
      testAssertEqual(nl.count(nullif), 0);
      nl.insert(nullif.toString());
    }

    for (auto& txout : tx.outs) {
      // sign TX output and return sig share
      std::vector<RandSigShare> sigShare;
      for (auto& bskShare : bsk) {
        sigShare.push_back(txout.shareSignComm(p, bskShare));
      }

      std::vector<RandSigShare> sigShareSubset;
      std::vector<size_t> signerSubset;

      // TODO: Sample a subset of sigshares and aggregate

      RandSig sigma = RandSig::aggregate(n, sigShareSubset, signerSubset);

      for (auto& ow : w) {
        ow.tryMakeMine(txout.getComm(), sigma);
        // i.e., calls TxOut::isMine() which returns std::optional<CoinSecrets> and, if yours, then calls
        // Wallet::makeMine, which rerandomizes everything
      }
    }
  }

  // TODO: keep track of and assert balances are correct
}

void test2to2Txn() {
  auto p = libutt::Params::Random();

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<long> udi(1, 1024);

  CoinSecrets c1(p, udi(gen)), c2(p, udi(gen));  // randomly generated coins
  CoinComm cc1(p, c1), cc2(p, c2);               // commit to the coins

  // sign the coins
  BankSK bsk = BankSK::random();
  CoinSig scc1 = bsk.thresholdSignCoin(p, cc1);
  CoinSig scc2 = bsk.thresholdSignCoin(p, cc2);

  // together, the coins total this much
  unsigned long totalVal = (c1.val + c2.val).as_ulong();
  std::uniform_int_distribution<unsigned long> udo(0, totalVal);

  // split them up randomly
  unsigned long o1 = udo(gen);
  unsigned long o2 = totalVal - o1;
  Fr out1, out2;
  out1.set_ulong(o1);
  out2.set_ulong(o2);

  AddrSK ask1 = AddrSK::random(), ask2 = AddrSK::random();
  AddrPK apk1 = ask1.toAddrPK(p);
  AddrPK apk2 = ask2.toAddrPK(p);

  Tx tx = Tx::create(p,
                     {
                         std::make_tuple(c1, cc1, scc1),
                         std::make_tuple(c2, cc2, scc2),
                     },
                     {std::make_tuple(apk1, out1), std::make_tuple(apk2, out2)});

  BankPK bpk = bsk.toPK(p);
  tx.verify(p, bpk);
  tx.process(p, bsk);
  auto opt1 = tx.outs[0].isMine(p, ask1);
  testAssertTrue(opt1.has_value());
  auto walletCoin1 = tx.outs[0].makeMine(p, opt1.value(), bpk);
  testAssertFalse(tx.outs[1].isMine(p, ask1).has_value())(void) walletCoin1;

  auto opt2 = tx.outs[1].isMine(p, ask2);
  testAssertTrue(opt2.has_value());
  testAssertFalse(tx.outs[0].isMine(p, ask2).has_value()) auto walletCoin2 = tx.outs[1].makeMine(p, opt2.value(), bpk);
  (void)walletCoin2;
}

int main(int argc, char* argv[]) {
  libutt::initialize(nullptr, 0);
  // srand(static_cast<unsigned int>(time(NULL)));
  (void)argc;
  (void)argv;

  testCoinSig();

  test2to2Txn();

  loginfo << "All is well." << endl;

  return 0;
}
