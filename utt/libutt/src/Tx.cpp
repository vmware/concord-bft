#include <utt/Configuration.h>

#include <optional>
#include <tuple>

#include <utt/Address.h>
#include <utt/BudgetProof.h>
#include <utt/Coin.h>
#include <utt/Comm.h>
#include <utt/Params.h>
#include <utt/RegAuth.h>
#include <utt/SplitProof.h>
#include <utt/Tx.h>
#include <utt/Address.h>

#include <utt/Serialization.h>

#include <xutils/NotImplementedException.h>  // WARNING: Include this last (see header file for details; thanks, C++)

std::ostream& operator<<(std::ostream& out, const libutt::Tx& tx) {
  out << tx.budgetPolicy << endl;
  out << tx.is_budgeted << endl;
  out << tx.isSplitOwnCoins << endl;
  out << tx.rcm;
  out << tx.regsig;

  libutt::serializeVector(out, tx.ins);
  libutt::serializeVector(out, tx.outs);

  out << tx.budget_pi;

  return out;
}

std::istream& operator>>(std::istream& in, libutt::Tx& tx) {
  in >> tx.budgetPolicy;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> tx.is_budgeted;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> tx.isSplitOwnCoins;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> tx.rcm;
  in >> tx.regsig;

  libutt::deserializeVector(in, tx.ins);
  libutt::deserializeVector(in, tx.outs);

  in >> tx.budget_pi;
  // If the budget policy is true, so the only reason not to have budget, is if the transaction is a split coins
  // transaction.
  return in;
}

namespace libutt {

Tx::Tx(const Params& p,
       const AddrSK& ask,
       const std::vector<Coin>& c,
       std::optional<Coin> b,  // optional budget coin
       const std::vector<std::tuple<std::string, Fr>>& recip,
       const RandSigPK& bpk,  // only used for debugging
       const RegAuthPK& rpk,
       bool budget_policy)  // only to encrypt for the recipients
    : Tx{p,
         ask.pid_hash,
         ask.pid,
         ask.rcm,
         ask.rs,
         ask.s,
         c,
         std::move(b),
         recip,
         bpk,
         rpk.vk,
         IBEEncryptor(rpk.mpk),
         budget_policy} {}

Tx::Tx(const Params& p,
       const Fr pidHash,
       const std::string& pid,
       const Comm& rcm_,
       const RandSig& rcm_sig,
       const Fr& prf,
       const std::vector<Coin>& coins,
       std::optional<Coin> b,  // optional budget coin
       const std::vector<std::tuple<std::string, Fr>>& recip,
       std::optional<RandSigPK> bpk,  // only used for debugging
       const RandSigPK& rpk,
       const IEncryptor& encryptor,
       bool budget_policy) {
  budgetPolicy = budget_policy;
#ifndef NDEBUG
  (void)bpk;
#endif
  /**
   * Step 1: Some sanity checks
   */
  assertFalse(coins.empty());

  Fr pid_hash_sender = pidHash;  // the (single) sender's PID hash

  isSplitOwnCoins = true;       // true when this TXN simply splits the sender's coins
  is_budgeted = b.has_value();  // true when this TXN is budgeted
  if (!budgetPolicy && is_budgeted) {
    logerror << "budget policy is false, but a budget coin was given" << endl;
    throw std::runtime_error("budget policy is false, but a budget coin was given");
  }
  size_t totalIn = Coin::totalValue(coins);  // total in value
  size_t totalOut = 0;                       // total out value
  size_t paidOut = 0;                        // total amount paid out to someone else

  for (const auto& coin : coins) {
    // all input coins must have same PID (including budget, checked later below)
    if (coin.pid_hash != pid_hash_sender) {
      // logdbg << "coin.pid_hash:   " << coin.pid_hash   << endl;
      // logdbg << "pid_hash_sender: " << pid_hash_sender << endl;
      throw std::runtime_error("All input coins must belong to the same sender");
    }

    // all coins must be re-randomized
    if (coin.r == Fr::zero()) {
      throw std::runtime_error("One of the input coins is not re-randomized!");
    }

    // all coins must have valid signatures
    if (bpk.has_value()) assertTrue(coin.hasValidSig(*bpk));
  }

  // get total value of all normal output coins
  std::set<size_t> forMeOutputs;
  std::vector<size_t> notForMeOutputs;
  for (size_t j = 0; j < recip.size(); j++) {
    const auto& pid_recip = std::get<0>(recip[j]);
    auto val = static_cast<size_t>(std::get<1>(recip[j]).as_ulong());

    totalOut += val;

    if (pid_recip != pid) {
      paidOut += val;
      isSplitOwnCoins = false;
      notForMeOutputs.push_back(j);
    } else {
      forMeOutputs.insert(j);
    }
  }

  // are you spending more than you have?
  if (totalIn != totalOut) {
    logerror << "Total-in is " << totalIn << " but total-out is " << totalOut << endl;
    throw std::runtime_error("Input and output normal coins must have same total");
  }

  if (is_budgeted) {
    // if splitting your own coins, you don't need budgets
    if (isSplitOwnCoins) {
      throw std::runtime_error("You need not provide a budget coin when splitting your own coins");
    }

    // are you trying to spend using someone else's budget?
    if (b->pid_hash != pid_hash_sender) {
      throw std::runtime_error("Budget coin must have same PID as all the other input normal coins");
    }

    // is there enough budget?
    if (b->getValue() < paidOut) {
      logerror << "Budget is " << b->getValue() << " but trying to pay out " << paidOut << endl;
      throw std::runtime_error("Ran out of budget");
    }
  }

  logtrace << "TXN sanity checks out!" << endl;

  /**
   * Step 2: Prepare inputs first (no need for threshold PS16 base H_j here)
   *
   *  - Re-randomize rcm and regsig (re-use for all inputs), then copy to TXN
   *  - Copy *partial* coin commitment (i.e., Coin::commForTxn())
   *  - Copy coin type & exp date
   *  - Copy coin signature
   *  - Copy pre-computed nullifier + consistency proof
   *  - Copy pre-computed (input) value commitment (VCM), whose randomness is later correlated with output VCMs
   */
  rcm = rcm_;        // has randomness zero
  regsig = rcm_sig;  // will be rerandomized

  assertTrue(regsig.verify(rcm, rpk));

  // re-randomize rcm, with randomness 'a', and then regsig
  Fr a = Fr::random_element();  // we need to pass this into the SplitProof constructor
  rcm.rerandomize(p.getRegCK(), a);
  regsig.rerandomize(a, Fr::random_element());

  assertTrue(regsig.verify(rcm, rpk));

  // number of senders
  assertGreaterThanOrEqual(coins.size(), 1);

  // vector of randomness used for the input VCMs, which we'll need to correlate to the output VCMs
  // so we can check \sum_i v_i = \sum_j v'_j for all 'normal' inputs i and outputs j
  std::vector<Fr> z;
  // Fr z_sum = Fr::zero();
  for (size_t i = 0; i < coins.size(); i++) {
    ins.emplace_back(coins[i]);
    z.push_back(coins[i].z);

    // z_sum = z_sum + z.back();
    // logtrace << "z_in: " << z.back() << endl;

    // NOTE: We defer computation of the splitproof in ins[i].pi to later when we have the
    // TXN's hash, which we use as the randomness of the splitproof
  }

  // logtrace << "z_sum: " << z_sum << endl;

  // create input for budget coin too, if any
  if (is_budgeted) {
    ins.emplace_back(*b);
  }

  /**
   * Step 3: Create the TX outputs
   *
   *  - Pseudo-randomly derive base H_j
   *     + TXN creator and replicas must agree on this, b.c. creator needs to commit using H_j
   *     + Thus, cannot (efficiently) use consensus to derive H_j
   *  - Very important that different messages being signed use different H_j
   *     + We have to be careful w/ different TXNs (that have same inputs) being sent to different replicas
   *     + My intern reassured me that replicas always agree that inputs are unspent before signing
   *     + So a replica will never signShare on two TXNs that have the same input
   *  - Otherwise, if you can get servers to sign two different messages with the same H_j, you can forge a new
   signature as:
   *     + [ g^u * g_u, (X C_1)^u * (X C_2)^u * (C_1 C_2)^u ] =  [g^{2u}, X^{2u} (C_1 C_2)^{2u} ]
   *  - We could probably just use the inputs, since:
         + if invalid, then nothing will be signed
         + if valid, then nothing will be signed twice based on those inputs (and thus based on the same H_j)
   *         * the problem of course is that replicas might sign optimistically?
   */
  size_t m = recip.size();  // number of recipients

  // pick 'm' output commitment randomizers such that they have the same sum as the 'k' input coin randomizers
  std::vector<Fr> z_recip = RangeProof::correlatedRandomness(z, m);
  // Fr z_sum_out = Fr::zero();

  std::vector<G1> H;  // h_j's
  auto t_p = random_field_elems(m);
  // goes through all normal output coins
  for (size_t j = 0; j < m; j++) {
    H.push_back(Tx::deriveRandSigBase(j));  // WARNING: needs inputs to be computed in 'ins'
    auto pid_recip = std::get<0>(recip[j]);
    auto val_recip = std::get<1>(recip[j]);

    /**
     * Compute jth 'normal' TxOut
     *  - compute icm + zkpok
     *  - compute vcm_1
     *  - compute vcm_2 under ck_tx (h_j, g) and new (encrypted) randomness
     *  - compute PedEq proof between vcm_1 and vcm_2
     *  - compute range proof for vcm_1
     *  - compute ctxt
     */
    bool icmPok = !(is_budgeted && pid_recip == pid);
    bool hasRangeProof = true;
    outs.emplace_back(p.getValCK(),
                      p.getRangeProofParams(),
                      Coin::NormalType(),
                      Coin::DoesNotExpire(),  // normal coins don't expire
                      H.back(),
                      pid_recip,
                      val_recip,
                      z_recip.at(j),
                      icmPok,
                      hasRangeProof,
                      encryptor);

    // z_sum_out = z_sum_out + z_recip[j];
  }

  // logtrace << "z_sum_out: " << z_sum_out << endl;

  /**
   * Step 4: Take care of budget proof, if budgeted TXN.
   */
  if (is_budgeted) {
    // For the budget preservation, we have to check that:
    // budget_in - \sum_{j \in output coins for another} val_out(j) = budget_out
    Fr z_budget_recip = b->z;  // the randomness of the input budget coin's 'vcm_1'
    for (auto j : notForMeOutputs) {
      z_budget_recip = z_budget_recip - z_recip[j];
    }

    H.push_back(Tx::deriveRandSigBase(m));  // WARNING: needs inputs to be computed in 'ins'

    // compute budget change
    Fr val_budget_out = Fr(static_cast<long>(b->getValue() - paidOut));

    // don't forget to add budget txout
    bool icmPok = false;
    // I thought we do not need this initially, but we do: it actually
    // ensures that there was enough budget.
    bool hasRangeProof = true;
    outs.emplace_back(p.getValCK(),
                      p.getRangeProofParams(),
                      Coin::BudgetType(),
                      b->getExpDate(),  // The budget coin inherit the old budget coin expiration date
                      H.back(),
                      pid,
                      val_budget_out,
                      z_budget_recip,
                      icmPok,
                      hasRangeProof,
                      encryptor);

    // add the output budget coin as one of the 'forMeTxos'
    forMeOutputs.insert(outs.size() - 1);

    // create the budget proof
    std::vector<CommKey> cks;
    std::vector<Comm> icms;
    std::vector<Fr> ts;
    for (auto j : forMeOutputs) {
      assertStrictlyLessThan(j, H.size());
      cks.emplace_back(CommKey({H[j], p.getCoinCK().getGen1()}));
      icms.push_back(outs[j].icm);
      ts.push_back(outs[j].t);
    }

    // for each output coin with same pid, compute budget proof that they have same pid
    budget_pi = BudgetProof(forMeOutputs, p.getRegCK(), rcm, pid_hash_sender, prf, a, cks, icms, ts);
  }

  /**
   * Step 5: Now that we can hash output, and thus derive randomness, compute
   * the splitproofs; this includes the budget coin, which was added to the inputs
   */
  std::string outsHash = TxOut::hashAll(outs);

  for (size_t i = 0; i < coins.size(); i++) {
    // logdbg << "Split proof for input #" << i << endl;
    ins[i].pi = SplitProof(p, pidHash, prf, coins.at(i), a, rcm, outsHash);
  }

  if (is_budgeted) {
    // logdbg << "Split proof for budget coin" << endl;
    ins.back().pi = SplitProof(p, pidHash, prf, *b, a, rcm, outsHash);
  }

  assertEqual(ins.size(), coins.size() + (b.has_value() ? 1 : 0));
  assertEqual(outs.size(), recip.size() + (b.has_value() ? 1 : 0));
}

bool Tx::quickPayValidate(const Params& p, const RandSigPK& bpk, const RegAuthPK& rpk, bool budget_policy) const {
  /**
   * TODO(Perf): Do we even need to check coinsig?
   * TODO(Perf): Do we even need to check regsig?
   *  - A lot of bad nullifiers could be added to the list if we do not
   */

  /**
   * Step 1: Sanity check
   */
  bool foundBudgetOutCoin = false, foundBudgetInCoin = false;

  for (auto& txout : outs) {
    foundBudgetOutCoin = foundBudgetOutCoin || (txout.coin_type == Coin::BudgetType());
    // TODO: check coin expiration here
  }

  for (auto& txin : ins) {
    foundBudgetInCoin = foundBudgetInCoin || (txin.coin_type == Coin::BudgetType());
    // TODO: check coin expiration here
  }

  // whether a budget policy is true or not, a split coins transaction should not have an input budget coin
  if (isSplitOwnCoins && foundBudgetInCoin) {
    logerror << "spliting your own coins doesn't require a budget" << endl;
    return false;
  }

  if (is_budgeted != foundBudgetOutCoin) {
    logerror << "TX claimed to be budgeted but did not have an output budget coin" << endl;
    return false;
  }

  if (is_budgeted != foundBudgetOutCoin) {
    logerror << "TX claimed to be budgeted but did not have an output budget coin" << endl;
    return false;
  }

  if (budget_policy) {
    if (!budgetPolicy) {
      logerror << "budget policy is enforced but the transaction's budget policy is false" << endl;
      return false;
    }
  }

  if (budget_policy && !isSplitOwnCoins && !is_budgeted) {
    logerror << "budget policy is enforced and budget is needed, but the transaction doesn't have a budget coin"
             << endl;
    return false;
  }

  assertTrue(!is_budgeted || budget_pi.has_value());

  /**
   * Step 2: Check registration authority's sig on registration commitment
   */
  if (!regsig.verify(rcm, rpk.vk)) {
    logerror << "TX did not have a valid regsig" << endl;
    return false;
  }

  /**
   * Step 3: Check inputs
   */
  std::string txOutsHash = TxOut::hashAll(outs);
  for (size_t i = 0; i < ins.size(); i++) {
    // check this is a normal coin (except for the last one if budgeted, which is allowed not to be normal)
    if (ins[i].coin_type != Coin::NormalType()) {
      if (!is_budgeted || i != ins.size() - 1) {
        logerror << "Expected input #" << i << " to be a normal coin" << endl;
        return false;
      }
    }

    // check bank's sig on coin commitment, for each TxIn
    logtrace << "Checking input coin #" << i << " is signed" << endl;

    // Here, we need the *full* coin commitment which contains the type and expiration date
    auto ccm_full = Coin::augmentComm(p.getCoinCK(), ins[i].ccm, ins[i].coin_type, ins[i].exp_date);
    if (!ins[i].coinsig.verify(ccm_full, bpk)) {
      logerror << "ins[" << i << "] did not have a valid coinsig" << endl;
      return false;
    }

    // check splitproof
    if (!ins[i].pi.verify(p, ins[i].null, rcm, ins[i].ccm, ins[i].vcm, txOutsHash)) {
      logerror << "txin[" << i << "] with a '" << Coin::typeToString(ins[i].coin_type)
               << "' coin did not have a valid SplitProof" << endl;
      return false;
    }
  }

  return true;
}

bool Tx::validate(const Params& p, const RandSigPK& bpk, const RegAuthPK& rpk, bool budget_policy) const {
  if (!quickPayValidate(p, bpk, rpk, budget_policy)) return false;
  /**
   * Step 4: Check value preservation of normal coins.
   *  - check the sum of in and out 'normal' coin value commitments is the same
   */
  size_t numNormalIn = ins.size() - (is_budgeted ? 1 : 0);
  size_t numNormalOut = outs.size() - (is_budgeted ? 1 : 0);

  G1 incomms = G1::zero(), outcomms = G1::zero();
  for (size_t i = 0; i < numNormalIn; i++) {
    incomms = ins[i].vcm.ped1 + incomms;
  }

  for (size_t j = 0; j < numNormalOut; j++) {
    outcomms = outs[j].vcm_1.ped1 + outcomms;
  }

  if (incomms != outcomms) {
    logerror << "The in and out commitments disagreed on the sum of the values (or randomness)" << endl;
    return false;
  }

  /**
   * Step 5: Check outputs (including budget coin)
   */
  std::vector<CommKey> ck_tx;
  for (size_t j = 0; j < outs.size(); j++) {
    // check this is a normal coin (except for the last one if budget, which is allowed not to be normal)
    if (outs[j].coin_type != Coin::NormalType()) {
      if (!is_budgeted || j != outs.size() - 1) {
        logerror << "Expected output #" << j << " to be a normal coin" << endl;
        return false;
      }
    }

    // check ck_tx = (H_j, g) is correctly computed from nullifiers
    // NOTE: We cache H_j in the output for shareSignComm to re-use
    outs[j].H = deriveRandSigBase(j);
    ck_tx.push_back(CommKey({*outs[j].H, p.getCoinCK().getGen1()}));

    // check recipient's identity commitment is indeed well-formed: e.g. it is not g_1^pid g_2^v g^t for v != 0
    logtrace << "Checking output #" << j << "'s ZKPoK" << endl;
    if (is_budgeted && budget_pi->forMeTxos.count(j) == 1) {
      // for budgeted TXNs, the budget proof already proves knowledge of sender-owned outputs, so this is unnecessary
      assertFalse(outs[j].icm_pok.has_value());
    } else {
      assertTrue(outs[j].icm_pok.has_value());
      if (!outs[j].icm_pok->verify(ck_tx.back(), outs[j].icm)) {
        logerror << "Identity commitment ZKPoK did NOT verify" << endl;
        return false;
      }
    }

    // check PedEq proof between the two vcm's with different CKs and randomness
    if (!outs[j].vcm_eq_pi.verify(p.getValCK(), outs[j].vcm_1, ck_tx.back(), outs[j].vcm_2)) {
      logerror << "Pedersen equality proof for value commitments did NOT verify" << endl;
      return false;
    }

    // check range proofs on out comms (in comms are good by invariant)
    // if(is_budgeted && j == outs.size() - 1) {
    //    assertFalse(outs[j].range_pi.has_value());
    //} else {
    assertTrue(outs[j].range_pi.has_value());
    if (!outs[j].range_pi->verify(p.rpp, outs[j].vcm_1)) {
      logerror << "Range proof failed verifying" << endl;
      return false;
    }
    //}
  }

  /**
   * Step 6: Check budget details
   *  - check pid of input budget coin matches pid of output budget coin and of normal change coins
   *  - value preservation of budget
   */
  if (is_budgeted) {
    const auto& bout = outs.back();
    if (bout.coin_type != Coin::BudgetType()) {
      logerror << "Expected last output coin to be a budget coin" << endl;
      return false;
    }

    if (ins.back().coin_type != Coin::BudgetType()) {
      logerror << "Expected last input coin to be a budget coin" << endl;
      return false;
    }

    assertTrue(budget_pi.has_value());

    // coompile list of CKs, icm's and rcm and pass as arguments
    std::vector<CommKey> cks;
    std::vector<Comm> icms;
    for (auto j : budget_pi->forMeTxos) {
      assertStrictlyLessThan(j, ck_tx.size());
      cks.emplace_back(ck_tx[j]);
      icms.push_back(outs[j].icm);
    }

    if (!budget_pi->verify(p.getRegCK(), rcm, cks, icms)) {
      logerror << "Budget proof did NOT verify" << endl;
      return false;
    }

    incomms = ins.back().vcm.asG1();  // input budget coin's vcm
    outcomms = bout.vcm_1.asG1();     // output budget coin's vcm

    // since the budget proof passed, we can rely on the truthfullness of budget_pi.forMeTxos
    // to tell which TXOs are NOT for me and need to be accounted for in the budget
    for (size_t j = 0; j < outs.size(); j++) {
      if (budget_pi->forMeTxos.count(j) == 0) {
        outcomms = outcomms + outs[j].vcm_1.asG1();
      }
    }

    if (incomms != outcomms) {
      logerror << "Budget value preservation failed verification" << endl;
      return false;
    }
  }

  return true;
}

G1 Tx::deriveRandSigBase(size_t txoIdx) const {
  auto vec = getNullifiers();
  std::string nulls = vec.at(0);

  for (size_t i = 1; i < vec.size(); i++) {
    nulls += "|" + vec[i];
  }

  // TODO(Crypto): See libutt/hashing.md
  return hashToGroup<G1>("ps16base|" + nulls + "|" + std::to_string(txoIdx));
}

std::vector<std::string> Tx::getNullifiers() const {
  std::vector<std::string> nulls;

  for (auto& txin : ins) {
    nulls.push_back(txin.null.toUniqueString());
  }

  return nulls;
}

std::vector<Comm> Tx::getCommVector(size_t txoIdx, const G1& H) const {
  auto& txo = outs.at(txoIdx);

  Comm scm(getSN(txoIdx) * H);  // H^sn g^0
  Comm tcm(txo.coin_type * H);  // H^type g^0
  Comm dcm(txo.exp_date * H);   // H^exp_date g^0

  return {txo.icm, scm, txo.vcm_2, tcm, dcm};
}
std::map<size_t, RandSigShare> Tx::shareSignCoins(const RandSigShareSK& bskShare) const {
  std::map<size_t, RandSigShare> ret;
  for (size_t txoIdx = 0; txoIdx < outs.size(); txoIdx++) {
    ret[txoIdx] = shareSignCoin(txoIdx, bskShare);
  }
  return ret;
}

RandSigShare Tx::shareSignCoin(size_t txoIdx, const RandSigShareSK& bskShare) const {
  // We have icm and vcm commitments under CK (H, g), but we also need commitments
  // to the serial number, type and the exp_date (if any) under this CK
  // for the threshold PS16 scheme to work. Since the server knows these, the
  // server just creates their commitments with randomness zero via getCommVector()

  // issue new coin by signing the "separated-out" coin commitment

  // WARNING: It is important when signing that a replica derives its own H
  if (!outs.at(txoIdx).H.has_value()) {
    // this will be the case in quickPayValidate()
    outs[txoIdx].H = deriveRandSigBase(txoIdx);
  }

  assertTrue(outs.at(txoIdx).H.has_value());
  G1 H = *outs[txoIdx].H;

  return bskShare.shareSign(getCommVector(txoIdx, H), H);
}
std::unordered_map<size_t, TxOut> Tx::getMyTransactions(const IDecryptor& decryptor) const {
  std::unordered_map<size_t, TxOut> ret;
  for (size_t i = 0; i < outs.size(); i++) {
    auto& txo = outs[i];
    auto ptxt = decryptor.decrypt(txo.ctxt);
    if (ptxt.empty()) {
      continue;
    }
    // parse the plaintext as (value, vcm_2 randomness, icm randomness)
    auto vdt = bytesToFrs(ptxt);
    assertEqual(vdt.size(), 3);
    Fr val = vdt[0];
    Fr d = vdt[1];
    Fr t = vdt[2];
    ret.emplace(i, txo);
    ret[i].val = val;
    ret[i].d = d;
    ret[i].t = t;
  }
  return ret;
}
std::optional<Coin> Tx::tryClaimCoin(const Params& p,
                                     size_t txoIdx,
                                     const AddrSK& ask,
                                     size_t n,
                                     const std::vector<RandSigShare>& sigShares,
                                     const std::vector<size_t>& signerIds,
                                     const RandSigPK& bpk,
                                     const IDecryptor& decryptor) const {
  // WARNING: Use std::vector<TxOut>::at(j) so it can throw if out of bounds
  auto& txo = outs.at(txoIdx);

  Fr val;  // coin value
  Fr d;    // vcm_2 value commitment randomness
  Fr t;    // identity commitment randomness

  // decrypt the ciphertext
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

  RandSig sig = RandSigShare::aggregate(n, sigShares, signerIds, p.getCoinCK(), r);

#ifndef NDEBUG
  {
    auto sn = getSN(txoIdx);
    logtrace << "sn: " << sn << endl;
    Comm ccm = Comm::create(p.getCoinCK(),
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
  Coin c(p.getCoinCK(), p.null, getSN(txoIdx), val, txo.coin_type, txo.exp_date, ask);

  // re-randomize the coin signature
  Fr u_delta = Fr::random_element();
  c.sig = sig;
  c.sig.rerandomize(c.r, u_delta);

  assertTrue(c.hasValidSig(bpk));
  assertNotEqual(c.r, Fr::zero());  // should output a re-randomized coin always

  return c;
}

}  // namespace libutt
