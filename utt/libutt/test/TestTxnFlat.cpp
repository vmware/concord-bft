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

#include <utt/NtlLib.h>

using namespace std;
using namespace libutt;

using Fr = typename libff::default_ec_pp::Fp_type;

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

int main(int argc, char* argv[]) {
  //////////////////////////////////////////////////////////////////////////////////////////////
  // LIBRARY INITIALIZATION
  //////////////////////////////////////////////////////////////////////////////////////////////
  // libutt::initialize(nullptr, 0);
  /*void initialize(unsigned char * randSeed, int size)*/
  {
    unsigned char* randSeed = nullptr;  // TODO: initialize entropy source
    int size = 0;                       // TODO: initialize entropy source

    // Apparently, libff logs some extra info when computing pairings
    libff::inhibit_profiling_info = true;

    // AB: We _info disables printing of information and _counters prevents tracking of profiling information. If we are
    // using the code in parallel, disable both the logs.
    libff::inhibit_profiling_counters = true;

    // Initializes the default EC curve, so as to avoid "surprises"
    libff::default_ec_pp::init_public_params();

    // Initializes the NTL finite field
    NTL::ZZ p = NTL::conv<ZZ>("21888242871839275222246405745257275088548364400416034343698204186575808495617");
    NTL::ZZ_p::init(p);

    NTL::SetSeed(randSeed, size);

#ifdef USE_MULTITHREADING
    // NOTE: See https://stackoverflow.com/questions/11095309/openmp-set-num-threads-is-not-working
    loginfo << "Using " << getNumCores() << " threads" << endl;
    omp_set_dynamic(0);                                    // Explicitly disable dynamic teams
    omp_set_num_threads(static_cast<int>(getNumCores()));  // Use 4 threads for all consecutive parallel regions
#else
    // loginfo << "NOT using multithreading" << endl;
#endif

    RangeProof::Params::initializeOmegas();
  }

  // srand(static_cast<unsigned int>(time(NULL)));
  (void)argc;
  (void)argv;

  // testBudgeted2to2Txn(12, 21, 3, true, false);
  // void testBudgeted2to2Txn(size_t thresh, size_t n, size_t numCycles, bool isBudgeted, bool isQuickPay)
  {
    size_t thresh = 12;
    size_t n = 21;
    size_t numCycles = 3;
    bool isBudgeted = true;
    bool isQuickPay = false;

    // TODO: test no budgets too
    testAssertTrue(isBudgeted);

    // NOTE: Already tested (de)serialization of: Params, RandSigPK,
    // RandSigShareSK, RandSigSharePK.
    //
    // We do NOT need to serialize RandSigSK and RegAuthSK for our benchmarking purposes.

    //////////////////////////////////////////////////////////////////////////////////////////////
    // PARAMS INITIALIZATION
    //////////////////////////////////////////////////////////////////////////////////////////////
    // Params initialization should be part of the program that does the configuration

    // Factory f(thresh, n);
    // Factory(size_t t, size_t n)
    RandSigDKG dkg = RandSigDKG(thresh, n, Params::NumMessages);
    Params p = Params::random(dkg.getCK());                             // All replicas
    RegAuthSK rsk = RegAuthSK::random(p.getRegCK(), p.getIbeParams());  // eventually not needed

    // Params p;

    // ..but still wanna make sure deserialized params work for validating TXNs
    // std::stringstream ssp;
    // ssp << f.getParams();
    // ssp >> p;

    // RegAuthPK rpk = f.getRegAuthPK();
    RegAuthPK rpk = rsk.toPK();  // Want it in all replicas
    // RandSigPK bpk = f.getBankPK();
    RandSigPK bpk = dkg.getSK().toPK();  // Want in all replicas
    // std::vector<RandSigShareSK> bskShares = f.getBankShareSKs();
    std::vector<RandSigShareSK> bskShares = dkg.getAllShareSKs();  // Each replica stores its share

    logdbg << "Created decentralized UTT system" << endl;

    // Creates users and their wallets
    size_t numWallets = 3, numCoins = 2;
    size_t maxDenom = 100;
    size_t budget = maxDenom * numCycles;

    //////////////////////////////////////////////////////////////////////////////////////////////
    // CREATE ACCOUNT WALLETS
    //////////////////////////////////////////////////////////////////////////////////////////////

    // std::vector<Wallet> w = f.randomWallets(numWallets, numCoins, maxDenom, budget);
    // std::vector<Wallet> randomWallets(size_t numWallets, size_t numCoins, size_t maxDenom, size_t budget)
    std::vector<Wallet> w = [&]() {
      // auto bsk = getBankSK();
      auto bsk = dkg.getSK();
      std::vector<Wallet> wallets;  // Created by the configuration program

      for (size_t i = 0; i < numWallets; i++) {
        // register a randomly-generated user
        AddrSK ask = rsk.registerRandomUser("user" + std::to_string(i + 1));

        // Part of each client
        Wallet w;
        w.p = p;
        w.ask = ask;
        w.bpk = bsk.toPK();
        w.rpk = rsk.toPK();
        // logdbg << "RPK::vk " << w.rpk.vk << endl;

        // issue some random 'normal' coins to this user
        // Part of each client
        Coin prevCoin;
        size_t totalVal = 0;
        for (size_t k = 0; k < numCoins; k++) {
          // ...of value less than or equal to the allowed max coin value
          size_t val = static_cast<size_t>(rand()) % maxDenom + 1;
          totalVal += val;

          //////////////////////////////////////////////////////////////////////////////////////////////
          // ISSUE NORMAL TYPE COIN
          //////////////////////////////////////////////////////////////////////////////////////////////
          // ...and everything else random
          // Coin c = Factory::mintRandomCoin(Coin::NormalType(), val, ask);
          // Coin mintRandomCoin(const Fr& type, size_t val, const AddrSK& ask)
          Coin c = [&]() {
            auto type = Coin::NormalType();

            auto sn = Fr::random_element();
            auto exp_date = (type == Coin::BudgetType() ? Coin::SomeExpirationDate() : Coin::DoesNotExpire());
            auto val_fr = Fr(static_cast<long>(val));
            std::string typeStr = Coin::typeToString(type);

            // logdbg << "Minting '" << typeStr << "' coin of value " << val << " for " << ask.pid << endl;
            Coin c(p.getCoinCK(), p.null, sn, val_fr, type, exp_date, ask);

            // sign *full* coin commitment using bank's SK
            // c.sig = getBankSK().sign(c.augmentComm());
            c.sig = bsk.sign(c.augmentComm());

            return c;
          }();
          w.addNormalCoin(c);

          // test serialization
          Coin currCoin;
          std::stringstream ss;
          ss << c;
          ss >> currCoin;
          assertEqual(currCoin, c) assertNotEqual(prevCoin, c);

          prevCoin = c;
        }

        //////////////////////////////////////////////////////////////////////////////////////////////
        // ISSUE BUDGET TYPE COIN
        //////////////////////////////////////////////////////////////////////////////////////////////
        // issue budget coin
        // Coin b = Factory::mintRandomCoin(Coin::BudgetType(), budget, ask);
        // Coin mintRandomCoin(const Fr& type, size_t val, const AddrSK& ask)
        // Store the budget coin the wallet
        Coin b = [&]() {
          auto type = Coin::BudgetType();
          size_t val = budget;

          auto sn = Fr::random_element();
          auto exp_date = (type == Coin::BudgetType() ? Coin::SomeExpirationDate() : Coin::DoesNotExpire());
          auto val_fr = Fr(static_cast<long>(val));
          std::string typeStr = Coin::typeToString(type);

          // logdbg << "Minting '" << typeStr << "' coin of value " << val << " for " << ask.pid << endl;
          Coin c(p.getCoinCK(), p.null, sn, val_fr, type, exp_date, ask);

          // sign *full* coin commitment using bank's SK
          // c.sig = getBankSK().sign(c.augmentComm());
          c.sig = bsk.sign(c.augmentComm());

          return c;
        }();

        w.setBudgetCoin(b);

        wallets.push_back(w);
      }
      return wallets;
    }();
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

        //////////////////////////////////////////////////////////////////////////////////////////////
        // CREATE TRANSACTION
        //////////////////////////////////////////////////////////////////////////////////////////////

        // NOTE: We remove spent coins from wallet and will add newly-minted coins to wallet
        // (BFT clients will use a cycle of two wallets to simulate Alice paying Bob by simply paying himself across
        // these two wallets)
        // Tx tx_tmp = w[i].spendTwoRandomCoins(pid_recip, true);
        Tx tx_tmp = [&]() {
          bool removeFromWallet = true;
          auto& coins = w[i].coins;
          auto& budgetCoin = w[i].budgetCoin;
          const auto& ask = w[i].ask;
          const auto& pid = pid_recip;

          testAssertGreaterThanOrEqual(coins.size(), 2);

          // Input coins
          std::vector<Coin> c;

          if (removeFromWallet) {
            c.push_back(coins.back());  // remove one coin from the wallet
            coins.pop_back();
            c.push_back(coins.back());  // remove one coin from the wallet
            coins.pop_back();
          } else {
            c.push_back(coins.at(coins.size() - 1));
            c.push_back(coins.at(coins.size() - 2));
          }

          // the recipients and their amounts received
          std::vector<std::tuple<std::string, Fr>> recip;

          // split these two input coins randomly amongst sender and recipient
          Fr totalVal = c.at(0).val + c.at(1).val;
          int totalValInt = static_cast<int>(totalVal.as_ulong());
          Fr val1;
          val1.set_ulong(static_cast<unsigned long>(rand() % (totalValInt - 1) + 1));  // i.e., random in [1, totalVal)
          Fr val2 = totalVal - val1;
          testAssertEqual(val1 + val2, totalVal);

          logdbg << "'" << ask.pid << "' sends $" << val1 << " to '" << pid << "'" << endl;
          logdbg << "'" << ask.pid << "' gets $" << val2 << " change" << endl;

          recip.push_back(std::make_tuple(pid, val1));      // send some money to someone else
          recip.push_back(std::make_tuple(ask.pid, val2));  // give yourself some change

          // ...and Tx::Tx() will take care of the budget change

          // testAssertTrue(hasBudgetCoin());
          testAssertTrue(budgetCoin.has_value());
          Coin b = *budgetCoin;
          if (removeFromWallet) {
            budgetCoin.reset();  // remove from wallet; caller is responsible for adding budget change back
            // testAssertFalse(hasBudgetCoin());
            testAssertFalse(budgetCoin.has_value());
          }

          return Tx(p, ask, c, b, recip, bpk, rpk);
        }();

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

        //////////////////////////////////////////////////////////////////////////////////////////////
        // REPLICAS SIGN OUTPUT COINS IN TRANSACTION
        //////////////////////////////////////////////////////////////////////////////////////////////

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

          //////////////////////////////////////////////////////////////////////////////////////////////
          // OBTAIN A SUBSET OF SIGN SHARES (F + 1) of N
          //////////////////////////////////////////////////////////////////////////////////////////////

          // first, sample a subset of sigshares to aggregate
          std::vector<RandSigShare> sigShareSubset;
          std::vector<size_t> signerIdSubset = random_subset(thresh, n);
          for (auto id : signerIdSubset) {
            sigShareSubset.push_back(sigShares.at(id));
          }

          //////////////////////////////////////////////////////////////////////////////////////////////
          // CLIENT TRIES TO CLAIM COINS
          //////////////////////////////////////////////////////////////////////////////////////////////

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
            // coin = tx.tryClaimCoin(p, txoIdx, w[j].ask, n, sigShareSubset, signerIdSubset, bpk);
            coin = [&]() -> std::optional<Coin> {
              // const Params& p,
              // size_t txoIdx,
              const AddrSK& ask = w[j].ask;
              // size_t n,
              const std::vector<RandSigShare>& sigShares = sigShareSubset;
              const std::vector<size_t>& signerIds = signerIdSubset;
              // const RandSigPK& bpk

              // WARNING: Use std::vector<TxOut>::at(j) so it can throw if out of bounds
              // auto& txo = outs.at(txoIdx);
              auto& txo = tx.outs.at(txoIdx);

              Fr val;  // coin value
              Fr d;    // vcm_2 value commitment randomness
              Fr t;    // identity commitment randomness

              // decrypt the ciphertext
              bool forMe;
              AutoBuf<unsigned char> ptxt;
              std::tie(forMe, ptxt) = ask.e.decrypt(txo.ctxt);

              if (!forMe) {
                logtrace << "TXO #" << txoIdx << " is NOT for pid '" << ask.pid << "'!" << endl;
                return std::nullopt;
              } else {
                logtrace << "TXO #" << txoIdx << " is for pid '" << ask.pid << "'!" << endl;
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

              RandSig sig = RandSigShare::aggregate(n, sigShares, signerIds, p.getCoinCK(), r);

#ifndef NDEBUG
              {
                auto sn = tx.getSN(txoIdx);
                logtrace << "sn: " << sn << endl;
                Comm ccm =
                    Comm::create(p.getCoinCK(),
                                 {
                                     ask.getPidHash(),
                                     sn,
                                     val,
                                     txo.coin_type,
                                     txo.exp_date,
                                     Fr::zero()  // the recovered signature will be on a commitment w/ randomness 0
                                 },
                                 true);

                assertTrue(sig.verify(ccm, bpk));
              }
#else
              (void)bpk;
#endif

              // TODO(Perf): small optimization here would re-use vcm_1 (g_3^v g^z) instead of recommitting
              // ...but then we'd need to encrypt z too
              //
              // the signature from above is on commitment with r=0, but the Coin constructor
              // will re-randomize the ccm and also compute coin commitment, value commitment, nullifier
              Coin c(p.getCoinCK(), p.null, tx.getSN(txoIdx), val, txo.coin_type, txo.exp_date, ask);

              // re-randomize the coin signature
              Fr u_delta = Fr::random_element();
              c.sig = sig;
              c.sig.rerandomize(c.r, u_delta);

              assertTrue(c.hasValidSig(bpk));
              assertNotEqual(c.r, Fr::zero());  // should output a re-randomized coin always

              return c;
            }();

            if (coin.has_value()) {
              foundIdx = j;
              logdbg << "Adding a $" << coin->val << " '" << coin->getType() << "' coin to wallet #" << j + 1
                     << " for '" << w[j].ask.pid << "'" << endl;
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

  loginfo << "All is well." << endl;

  return 0;
}
