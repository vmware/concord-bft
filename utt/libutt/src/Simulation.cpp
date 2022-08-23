
#include <utt/Simulation.h>

#include <utt/Configuration.h>

#include <cmath>
#include <ctime>
#include <iostream>
#include <fstream>
#include <random>
#include <stdexcept>
#include <vector>
#include <functional>
#include <algorithm>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>
#include <xutils/Timer.h>
#include <xutils/Utils.h>

#include <utt/NtlLib.h>

using Fr = typename libff::default_ec_pp::Fp_type;

namespace libutt::Simulation {

//////////////////////////////////////////////////////////////////////////////////////////////
void assertSerialization(Wallet& inOutWallet) {
  std::stringstream ss;
  ss << inOutWallet;

  Wallet tmp;
  ss >> tmp;

  testAssertEqual(tmp, inOutWallet);

  inOutWallet = tmp;
}

//////////////////////////////////////////////////////////////////////////////////////////////
Tx sendValidTxOnNetwork(const Context& ctx, const Tx& inTx) {
  auto oldHash = inTx.getHashHex();
  std::stringstream ss;
  ss << inTx;

  Tx outTx(ss);
  // ss >> tx;
  auto newHash = outTx.getHashHex();

  testAssertEqual(inTx, outTx);
  testAssertEqual(oldHash, newHash);

  if (!outTx.validate(ctx.p_, ctx.bpk_, ctx.rpk_)) {
    testAssertFail("TXN should have verified");
  }

  loginfo << "Validated TXN!" << endl;
  return outTx;
}

//////////////////////////////////////////////////////////////////////////////////////////////
void addNullifiers(const Tx& tx, std::set<std::string>& nullset) {
  // fetch nullifiers (includes budget coin nullifiers, if budgeted) and add to nullifier set
  // NOTE: we'll share the same nullifer set for both budget and normal coins
  for (auto& nullif : tx.getNullifiers()) {
    // make sure then nullifier is not in the list
    testAssertEqual(nullset.count(nullif), 0);

    // add the nullifier to the list
    nullset.insert(nullif);
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////
Coin createNormalCoin(const Context& ctx, size_t val, AddrSK& ask) {
  auto type = Coin::NormalType();
  auto sn = Fr::random_element();
  auto exp_date = Coin::DoesNotExpire();
  auto val_fr = Fr(static_cast<long>(val));
  std::string typeStr = Coin::typeToString(type);

  // loginfo << "Minting '" << typeStr << "' coin of value " << val << " for " << ask.pid << endl;
  Coin c(ctx.p_.getCoinCK(), ctx.p_.null, sn, val_fr, type, exp_date, ask);

  // sign *full* coin commitment using bank's SK
  // c.sig = getBankSK().sign(c.augmentComm());
  c.sig = ctx.bsk_.sign(c.augmentComm());

  return c;
}

//////////////////////////////////////////////////////////////////////////////////////////////
Coin createBudgetCoin(const Context& ctx, size_t val, AddrSK& ask) {
  auto type = Coin::BudgetType();
  auto sn = Fr::random_element();
  auto exp_date = Coin::SomeExpirationDate();
  auto val_fr = Fr(static_cast<long>(val));
  std::string typeStr = Coin::typeToString(type);

  // loginfo << "Minting '" << typeStr << "' coin of value " << val << " for " << ask.pid << endl;
  Coin c(ctx.p_.getCoinCK(), ctx.p_.null, sn, val_fr, type, exp_date, ask);

  // sign *full* coin commitment using bank's SK
  // c.sig = getBankSK().sign(c.augmentComm());
  c.sig = ctx.bsk_.sign(c.augmentComm());

  return c;
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
void replicasSignOutputCoins(const Context& ctx, Tx& tx, size_t txoIdx, std::vector<RandSigShare>& sigShares) {
  for (auto& bskShare : ctx.bskShares_) {
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
}

//////////////////////////////////////////////////////////////////////////////////////////////
std::optional<Coin> tryClaimCoin(const Context& ctx,
                                 Tx& tx,
                                 size_t txoIdx,
                                 const std::vector<RandSigShare>& sigShares,
                                 const std::vector<size_t>& signerIds,
                                 const AddrSK& ask) {
  // WARNING: Use std::vector<TxOut>::at(j) so it can throw if out of bounds
  // auto& txo = outs.at(txoIdx);
  auto& txo = tx.outs.at(txoIdx);

  Fr val;  // coin value
  Fr d;    // vcm_2 value commitment randomness
  Fr t;    // identity commitment randomness

  // decrypt the ciphertext
  IBEDecryptor decryptor(ask.e);
  auto ptxt = decryptor.decrypt(txo.ctxt);
  bool forMe = !ptxt.empty();
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

  RandSig sig = RandSigShare::aggregate(ctx.n_, sigShares, signerIds, ctx.p_.getCoinCK(), r);

#ifndef NDEBUG
  {
    auto sn = tx.getSN(txoIdx);
    logtrace << "sn: " << sn << endl;
    Comm ccm = Comm::create(ctx.p_.getCoinCK(),
                            {
                                ask.getPidHash(),
                                sn,
                                val,
                                txo.coin_type,
                                txo.exp_date,
                                Fr::zero()  // the recovered signature will be on a commitment w/ randomness 0
                            },
                            true);

    assertTrue(sig.verify(ccm, ctx.bpk_));
  }
#endif

  // TODO(Perf): small optimization here would re-use vcm_1 (g_3^v g^z) instead of recommitting
  // ...but then we'd need to encrypt z too
  //
  // the signature from above is on commitment with r=0, but the Coin constructor
  // will re-randomize the ccm and also compute coin commitment, value commitment, nullifier
  Coin c(ctx.p_.getCoinCK(), ctx.p_.null, tx.getSN(txoIdx), val, txo.coin_type, txo.exp_date, ask);

  // re-randomize the coin signature
  Fr u_delta = Fr::random_element();
  c.sig = sig;
  c.sig.rerandomize(c.r, u_delta);

  assertTrue(c.hasValidSig(ctx.bpk_));
  assertNotEqual(c.r, Fr::zero());  // should output a re-randomized coin always

  return c;
}

void signAndClaimOutputCoins(const Context& ctx, Tx& tx, std::vector<Wallet>& w) {
  // replicas: go through every TX output and sign it
  for (size_t txoIdx = 0; txoIdx < tx.outs.size(); txoIdx++) {
    // each replica returns its sigshare on this output
    std::vector<RandSigShare> sigShares;
    replicasSignOutputCoins(ctx, tx, txoIdx, sigShares);

    // first, sample a subset of sigshares to aggregate
    std::vector<RandSigShare> sigShareSubset;
    std::vector<size_t> signerIdSubset = random_subset(ctx.thresh_, ctx.n_);
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
      // coin = tx.tryClaimCoin(p, txoIdx, w[j].ask, n, sigShareSubset, signerIdSubset, bpk);
      coin = tryClaimCoin(ctx, tx, txoIdx, sigShareSubset, signerIdSubset, w[j].ask);

      if (coin.has_value()) {
        foundIdx = j;
        loginfo << "Adding a $" << coin->val << " '" << coin->getType() << "' coin to wallet #" << j + 1 << " for '"
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
        auto coin = tryClaimCoin(ctx, tx, txoIdx, {}, {}, w[j].ask);
        testAssertFalse(coin.has_value());
      }
    }

  }  // end for all txouts
}

void initialize() {
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

Context createContext(size_t n, size_t thresh) {
  RandSigDKG dkg = RandSigDKG(thresh, n, Params::NumMessages);

  Context ctx;
  ctx.p_ = Params::random(dkg.getCK());

  RegAuthSK rsk = RegAuthSK::random(ctx.p_.getRegCK(), ctx.p_.getIbeParams());

  ctx.n_ = n;
  ctx.thresh_ = thresh;
  ctx.rsk_ = rsk;
  ctx.bsk_ = dkg.getSK();
  ctx.bskShares_ = dkg.getAllShareSKs();
  ctx.rpk_ = rsk.toPK();
  ctx.bpk_ = ctx.bsk_.toPK();

  return ctx;
}

Wallet createWallet(const Context& ctx,
                    const std::string& pid,
                    const std::vector<size_t>& normalCoinVals,
                    size_t budgetCoinVal) {
  // register a randomly-generated user
  AddrSK ask = ctx.rsk_.registerRandomUser(pid);

  Wallet w;
  w.p = ctx.p_;
  w.ask = ask;
  w.bpk = ctx.bsk_.toPK();
  w.rpk = ctx.rsk_.toPK();

  // Issue normal coins to the user
  // Part of each client
  Coin prevCoin;
  for (const auto& val : normalCoinVals) {
    // ...and everything else random
    // Coin c = Factory::mintRandomCoin(Coin::NormalType(), val, ask);
    // Coin mintRandomCoin(const Fr& type, size_t val, const AddrSK& ask)
    Coin c = createNormalCoin(ctx, val, w.ask);
    w.addNormalCoin(c);

    // test serialization
    Coin currCoin;
    std::stringstream ss;
    ss << c;
    ss >> currCoin;
    assertEqual(currCoin, c) assertNotEqual(prevCoin, c);

    prevCoin = c;
  }

  // Issue budget coin
  // Coin b = Factory::mintRandomCoin(Coin::BudgetType(), budget, ask);
  // Coin mintRandomCoin(const Fr& type, size_t val, const AddrSK& ask)
  // Store the budget coin the wallet
  Coin b = createBudgetCoin(ctx, budgetCoinVal, w.ask);
  w.setBudgetCoin(b);

  return w;
}

void doRandomPayment_2t2(
    const Context& ctx, Wallet& w1, Wallet& w2, std::set<std::string>& nullset, std::vector<Wallet>& wallets) {
  testAssertGreaterThanOrEqual(w1.coins.size(), 2);

  // split these two input coins randomly amongst sender and recipient
  Fr totalVal = w1.coins.at(0).val + w1.coins.at(1).val;
  int totalValInt = static_cast<int>(totalVal.as_ulong());
  Fr val1;
  val1.set_ulong(static_cast<unsigned long>(rand() % (totalValInt - 1) + 1));  // i.e., random in [1, totalVal)
  Fr val2 = totalVal - val1;
  testAssertEqual(val1 + val2, totalVal);

  doPayment_2t2(ctx, w1, w2, 0, 1, val1.as_ulong(), nullset, wallets);
}

void doRandomPayment_2t1(
    const Context& ctx, Wallet& w1, Wallet& w2, std::set<std::string>& nullset, std::vector<Wallet>& wallets) {
  testAssertGreaterThanOrEqual(w1.coins.size(), 2);
  // ToDo: trivially random
  doPayment_2t1(ctx, w1, w2, 0, 1, nullset, wallets);
}

void doRandomPayment_1t2(
    const Context& ctx, Wallet& w1, Wallet& w2, std::set<std::string>& nullset, std::vector<Wallet>& wallets) {
  testAssertGreaterThanOrEqual(w1.coins.size(), 1);

  // Pick one coin from w1 that can be split as input
  size_t i = 0;
  for (; i < w1.coins.size(); ++i) {
    if (w1.coins[i].getValue() > 1) break;
  }
  testAssertTrue(i < w1.coins.size());

  // split the one coin randomly amongst sender and recipient
  Fr totalVal = w1.coins[i].val;
  int totalValInt = static_cast<int>(totalVal.as_ulong());
  Fr val1;
  val1.set_ulong(static_cast<unsigned long>(rand() % (totalValInt - 1) + 1));  // i.e., random in [1, totalVal)
  Fr val2 = totalVal - val1;
  testAssertEqual(val1 + val2, totalVal);

  doPayment_1t2(ctx, w1, w2, i, val1.as_ulong(), nullset, wallets);
}

void doRandomPayment_1t1(
    const Context& ctx, Wallet& w1, Wallet& w2, std::set<std::string>& nullset, std::vector<Wallet>& wallets) {
  // ToDo: trivially random
  doPayment_1t1(ctx, w1, w2, 0, nullset, wallets);
}

void doRandomCoinSplit(const Context& ctx, Wallet& w1, std::set<std::string>& nullset, std::vector<Wallet>& wallets) {
  testAssertGreaterThanOrEqual(w1.coins.size(), 1);

  // Pick one coin from w1 that can be split as input
  size_t i = 0;
  for (; i < w1.coins.size(); ++i) {
    if (w1.coins[i].getValue() > 1) break;
  }
  testAssertTrue(i < w1.coins.size());

  // split the one coin randomly amongst sender and recipient
  Fr totalVal = w1.coins[i].val;
  int totalValInt = static_cast<int>(totalVal.as_ulong());
  Fr val1;
  val1.set_ulong(static_cast<unsigned long>(rand() % (totalValInt - 1) + 1));  // i.e., random in [1, totalVal)
  Fr val2 = totalVal - val1;
  testAssertEqual(val1 + val2, totalVal);

  doCoinSplit(ctx, w1, i, val1.as_ulong(), nullset, wallets);
}

void doRandomCoinMerge(const Context& ctx, Wallet& w, std::set<std::string>& nullset, std::vector<Wallet>& wallets) {
  testAssertGreaterThanOrEqual(w.coins.size(), 2);
  // ToDo: trivially random
  doCoinMerge(ctx, w, 0, 1, nullset, wallets);
}

void doPayment_2t2(const Context& ctx,
                   Wallet& w1,
                   Wallet& w2,
                   size_t c1,
                   size_t c2,
                   size_t amount,
                   std::set<std::string>& nullset,
                   std::vector<Wallet>& wallets) {
  loginfo << "Payment 2-to-2 (Payment of two coins with change)" << endl;

  testAssertGreaterThanOrEqual(w1.coins.size(), 2);
  testAssertNotEqual(c1, c2);

  // Input coins
  std::vector<Coin> c;
  c.push_back(w1.coins.at(c1));
  c.push_back(w1.coins.at(c2));

  loginfo << "'" << w1.ask.pid << "' removing $" << w1.coins.at(c1).getValue() << " coin" << endl;
  loginfo << "'" << w1.ask.pid << "' removing $" << w1.coins.at(c2).getValue() << " coin" << endl;

  w1.coins.at(c1).val = 0;
  w1.coins.at(c2).val = 0;
  w1.coins.erase(std::remove_if(w1.coins.begin(), w1.coins.end(), [](const Coin& c) { return c.val == 0; }),
                 w1.coins.end());

  Fr totalVal = c.at(0).val + c.at(1).val;
  testAssertStrictlyGreaterThan(totalVal.as_ulong(), amount);
  Fr val1;
  val1.set_ulong(amount);
  Fr val2 = totalVal - val1;
  testAssertEqual(val1 + val2, totalVal);

  loginfo << "'" << w1.ask.pid << "' sends $" << val1 << " to '" << w2.ask.pid << "'" << endl;
  loginfo << "'" << w1.ask.pid << "' gets $" << val2 << " change" << endl;

  // the recipients and their amounts received
  std::vector<std::tuple<std::string, Fr>> recip;
  recip.push_back(std::make_tuple(w2.ask.pid, val1));  // send some money to someone else
  recip.push_back(std::make_tuple(w1.ask.pid, val2));  // give yourself some change

  // ...and Tx::Tx() will take care of the budget change
  testAssertTrue(w1.budgetCoin.has_value());
  Coin b = *w1.budgetCoin;
  w1.budgetCoin.reset();  // remove from wallet; caller is responsible for adding budget change back
  testAssertFalse(w1.budgetCoin.has_value());

  Tx tx_temp = Tx(ctx.p_, w1.ask, c, b, recip, ctx.bpk_, ctx.rpk_);

  loginfo << "Created TXN!" << endl;

  Tx tx = sendValidTxOnNetwork(ctx, tx_temp);

  addNullifiers(tx, nullset);

  signAndClaimOutputCoins(ctx, tx, wallets);

  loginfo << '\n';
}

void doPayment_2t1(const Context& ctx,
                   Wallet& w1,
                   Wallet& w2,
                   size_t c1,
                   size_t c2,
                   std::set<std::string>& nullset,
                   std::vector<Wallet>& wallets) {
  loginfo << "Payment 2-to-1 (Exact payment of two coins)" << endl;

  testAssertGreaterThanOrEqual(w1.coins.size(), 2);
  testAssertNotEqual(c1, c2);

  // Input coins
  std::vector<Coin> c;
  c.push_back(w1.coins.at(c1));
  c.push_back(w1.coins.at(c2));

  loginfo << "'" << w1.ask.pid << "' removing $" << w1.coins.at(c1).getValue() << " coin" << endl;
  loginfo << "'" << w1.ask.pid << "' removing $" << w1.coins.at(c2).getValue() << " coin" << endl;

  w1.coins.at(c1).val = 0;
  w1.coins.at(c2).val = 0;
  w1.coins.erase(std::remove_if(w1.coins.begin(), w1.coins.end(), [](const Coin& c) { return c.val == 0; }),
                 w1.coins.end());

  // Send both coins with no change
  Fr totalVal = c.at(0).val + c.at(1).val;

  loginfo << "'" << w1.ask.pid << "' sends $" << totalVal << " to '" << w2.ask.pid << "'" << endl;

  // the recipients and their amounts received
  std::vector<std::tuple<std::string, Fr>> recip;
  recip.push_back(std::make_tuple(w2.ask.pid, totalVal));  // send some money to someone else

  // ...and Tx::Tx() will take care of the budget change
  testAssertTrue(w1.budgetCoin.has_value());
  Coin b = *w1.budgetCoin;
  w1.budgetCoin.reset();  // remove from wallet; caller is responsible for adding budget change back
  testAssertFalse(w1.budgetCoin.has_value());

  Tx tx_temp = Tx(ctx.p_, w1.ask, c, b, recip, ctx.bpk_, ctx.rpk_);

  loginfo << "Created TXN!" << endl;

  Tx tx = sendValidTxOnNetwork(ctx, tx_temp);

  addNullifiers(tx, nullset);

  signAndClaimOutputCoins(ctx, tx, wallets);

  loginfo << '\n';
}

void doPayment_1t2(const Context& ctx,
                   Wallet& w1,
                   Wallet& w2,
                   size_t c1,
                   size_t amount,
                   std::set<std::string>& nullset,
                   std::vector<Wallet>& wallets) {
  loginfo << "Payment 1-to-2 (Payment of one coin with change)" << endl;

  testAssertGreaterThanOrEqual(w1.coins.size(), 1);

  std::vector<Coin> c;
  c.push_back(w1.coins.at(c1));
  loginfo << "'" << w1.ask.pid << "' removing $" << w1.coins.at(c1).getValue() << " coin" << endl;
  w1.coins.erase(w1.coins.begin() + (int)c1);

  Fr totalVal = c.at(0).val;
  Fr val1;
  val1.set_ulong(amount);
  testAssertStrictlyGreaterThan(totalVal.as_ulong(), amount);
  Fr val2 = totalVal - val1;
  testAssertEqual(val1 + val2, totalVal);

  loginfo << "'" << w1.ask.pid << "' sends $" << val1 << " to '" << w2.ask.pid << "'" << endl;
  loginfo << "'" << w1.ask.pid << "' gets $" << val2 << " change" << endl;

  // the recipients and their amounts received
  std::vector<std::tuple<std::string, Fr>> recip;
  recip.push_back(std::make_tuple(w2.ask.pid, val1));  // send some money to someone else
  recip.push_back(std::make_tuple(w1.ask.pid, val2));  // give yourself some change

  // ...and Tx::Tx() will take care of the budget change
  testAssertTrue(w1.budgetCoin.has_value());
  Coin b = *w1.budgetCoin;
  w1.budgetCoin.reset();  // remove from wallet; caller is responsible for adding budget change back
  testAssertFalse(w1.budgetCoin.has_value());

  Tx tx_temp = Tx(ctx.p_, w1.ask, c, b, recip, ctx.bpk_, ctx.rpk_);

  loginfo << "Created TXN!" << endl;

  Tx tx = sendValidTxOnNetwork(ctx, tx_temp);

  addNullifiers(tx, nullset);

  signAndClaimOutputCoins(ctx, tx, wallets);

  loginfo << '\n';
}

void doPayment_1t1(const Context& ctx,
                   Wallet& w1,
                   Wallet& w2,
                   size_t c1,
                   std::set<std::string>& nullset,
                   std::vector<Wallet>& wallets) {
  loginfo << "Payment 1-to-1 (Exact payment of one coin)" << endl;

  std::vector<Coin> c;
  c.push_back(w1.coins.at(c1));  // remove one coin from the wallet

  loginfo << "'" << w1.ask.pid << "' removing $" << w1.coins.at(c1).getValue() << " coin" << endl;
  w1.coins.erase(w1.coins.begin() + (int)c1);

  // Send a single coin with no change
  Fr totalVal = c.at(0).val;
  loginfo << "'" << w1.ask.pid << "' sends $" << totalVal << " to '" << w2.ask.pid << "'" << endl;

  // the recipients and their amounts received
  std::vector<std::tuple<std::string, Fr>> recip;
  recip.push_back(std::make_tuple(w2.ask.pid, totalVal));  // send some money to someone else

  // ...and Tx::Tx() will take care of the budget change
  testAssertTrue(w1.budgetCoin.has_value());
  Coin b = *w1.budgetCoin;
  w1.budgetCoin.reset();  // remove from wallet; caller is responsible for adding budget change back
  testAssertFalse(w1.budgetCoin.has_value());

  Tx tx_temp = Tx(ctx.p_, w1.ask, c, b, recip, ctx.bpk_, ctx.rpk_);

  loginfo << "Created TXN!" << endl;

  Tx tx = sendValidTxOnNetwork(ctx, tx_temp);

  addNullifiers(tx, nullset);

  signAndClaimOutputCoins(ctx, tx, wallets);

  loginfo << '\n';
}

void doCoinSplit(const Context& ctx,
                 Wallet& w,
                 size_t c1,
                 size_t amount,
                 std::set<std::string>& nullset,
                 std::vector<Wallet>& wallets) {
  loginfo << "Self 1-to-2 (Coin split)" << endl;

  testAssertGreaterThanOrEqual(w.coins.size(), 1);

  std::vector<Coin> c;
  c.push_back(w.coins.at(c1));
  w.coins.erase(w.coins.begin() + (int)c1);

  // split the one coin randomly into two coins
  Fr totalVal = c.at(0).val;
  testAssertStrictlyGreaterThan(totalVal.as_ulong(), amount);
  Fr val1;
  val1.set_ulong(amount);
  Fr val2 = totalVal - val1;
  testAssertEqual(val1 + val2, totalVal);

  loginfo << "'" << w.ask.pid << "' splits $" << totalVal << " into $" << val1 << " and $" << val2 << endl;

  // the recipients and their amounts received
  std::vector<std::tuple<std::string, Fr>> recip;
  recip.push_back(std::make_tuple(w.ask.pid, val1));  // give yourself first split
  recip.push_back(std::make_tuple(w.ask.pid, val2));  // give yourself second split

  Tx tx_temp = Tx(ctx.p_, w.ask, c, std::nullopt /*no budget*/, recip, ctx.bpk_, ctx.rpk_);

  loginfo << "Created TXN!" << endl;

  Tx tx = sendValidTxOnNetwork(ctx, tx_temp);

  addNullifiers(tx, nullset);

  signAndClaimOutputCoins(ctx, tx, wallets);

  loginfo << '\n';
}

void doCoinMerge(
    const Context& ctx, Wallet& w, size_t c1, size_t c2, std::set<std::string>& nullset, std::vector<Wallet>& wallets) {
  loginfo << "Self 2-to-1 (Coin merge)" << endl;

  testAssertGreaterThanOrEqual(w.coins.size(), 2);
  testAssertNotEqual(c1, c2);

  // Input coins
  std::vector<Coin> c;
  c.push_back(w.coins.at(c1));
  c.push_back(w.coins.at(c2));

  loginfo << "'" << w.ask.pid << "' removing $" << w.coins.at(c1).getValue() << " coin" << endl;
  loginfo << "'" << w.ask.pid << "' removing $" << w.coins.at(c2).getValue() << " coin" << endl;

  w.coins.at(c1).val = 0;
  w.coins.at(c2).val = 0;
  w.coins.erase(std::remove_if(w.coins.begin(), w.coins.end(), [](const Coin& c) { return c.val == 0; }),
                w.coins.end());

  // merge two coins into one coin
  Fr val1 = c.at(0).val;
  Fr val2 = c.at(1).val;
  Fr totalVal = val1 + val2;

  loginfo << "'" << w.ask.pid << "' merges $" << totalVal << " from $" << val1 << " and $" << val2 << endl;

  // the recipients and their amounts received
  std::vector<std::tuple<std::string, Fr>> recip;
  recip.push_back(std::make_tuple(w.ask.pid, totalVal));  // give yourself the merged value

  Tx tx_temp = Tx(ctx.p_, w.ask, c, std::nullopt /*no budget*/, recip, ctx.bpk_, ctx.rpk_);

  loginfo << "Created TXN!" << endl;

  Tx tx = sendValidTxOnNetwork(ctx, tx_temp);

  addNullifiers(tx, nullset);

  signAndClaimOutputCoins(ctx, tx, wallets);

  loginfo << '\n';
}

}  // namespace libutt::Simulation
