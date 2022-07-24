#pragma once

#include <cstddef>
#include <optional>
#include <tuple>

#include <utt/BudgetProof.h>
#include <utt/PolyCrypto.h>
#include <utt/TxIn.h>
#include <utt/TxOut.h>

#include <xassert/XAssert.h>
#include <xutils/AutoBuf.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>

namespace libutt {
class Coin;
class Params;
class RegAuthPK;
class Tx;
}  // namespace libutt

std::ostream& operator<<(std::ostream&, const libutt::Tx&);
std::istream& operator>>(std::istream&, libutt::Tx&);

namespace libutt {

class Tx {
 public:
  bool isSplitOwnCoins;  // true when splitting your own coins; in this case, no budget coins are given as input, to
                         // save TXN creation & validation time

  Comm rcm;        // commitment to sending user's registration
  RandSig regsig;  // signature on the registration commitment 'rcm'

  std::vector<TxIn> ins;
  std::vector<TxOut> outs;

  std::optional<BudgetProof> budget_pi;  // proof that (1) output budget coin has same pid as input coins and (2) that
                                         // this might be a "self-payment" TXN, so budget should be saved

 public:
  Tx() {}

  Tx(std::istream& in) : Tx() { in >> *this; }

  /**
   * Creates a TXN that join-splits the given (vector of) coins to the specified (vector of) PIDs and amounts.
   */
  Tx(const Params& p,
     const AddrSK& ask,
     const std::vector<Coin>& c,
     std::optional<Coin> b,  // optional budget coin
     const std::vector<std::tuple<std::string, Fr>>& recip,
     const RandSigPK& bpk,   // only used for debugging
     const RegAuthPK& rpk);  // only to encrypt for the recipients

  Tx(const Params& p,
     const Fr pidHash,
     const std::string& pid,
     const Comm& rcm,
     const RandSig& rcm_sig,
     const Fr& prf,
     const std::vector<Coin>& c,
     std::optional<Coin> b,  // optional budget coin
     const std::vector<std::tuple<std::string, Fr>>& recip,
     std::optional<RandSigPK> bpk,  // only used for debugging
     const RandSigPK& rpk,
     const IBE::MPK& mpk);  // only to encrypt for the recipients

 public:
  size_t getSize() const {
    return sizeof(isSplitOwnCoins) + rcm.getSize() + regsig.getSize() + ins.back().getSize() * ins.size() +
           sizeof(size_t) + outs.back().getSize() * outs.size() + sizeof(size_t) + sizeof(bool) +
           (budget_pi.has_value() ? budget_pi->getSize() : 0);
  }

  bool isBudgeted() const { return budget_pi.has_value(); }

  /**
   * Our threshold variant of PS16 requires that the TXN creator and the replicas
   * agree on a PS16 base h under which the PS16 signature (h, h^{\sum_i m_i y_i + x})
   * will be computed.
   *
   * This function picks such a unique h by hashing all of the input nullifiers.
   * Because a nullifier can never be spent twice (i.e., replicas never sign
   * before agreeing on the nullifier not being spent), we are not concerned
   * about an h being used to sign twice by the replicas, since that would require
   * the same inputs to pass TX validation.
   */
  G1 deriveRandSigBase(size_t txoIdx) const;

  bool quickPayValidate(const Params& p, const RandSigPK& bpk, const RegAuthPK& rpk) const;

  bool validate(const Params& p, const RandSigPK& bpk, const RegAuthPK& rpk) const;

  /**
   * Returns the nullifiers of all coins spent by this TXN, including the budget coin's.
   */
  std::vector<std::string> getNullifiers() const;

  /**
   * Returns the vector of commitments that needs to be used for threshold PS signing/verification of output # 'txoIdx'.
   * i.e., { icm, H^sn, vcm_2, H^cointype, H^expdate }
   * where H is the pseudorandomly-derived PS16 base for this output
   * Used in Tx::shareSignComm
   */
  std::vector<Comm> getCommVector(size_t txoIdx, const G1& H) const;

  /**
   * If Tx::validate() passes, each BFT replica will compute a signature share on each output's coin.
   */
  RandSigShare shareSignCoin(size_t txoIdx, const RandSigShareSK& bskShare) const;

  /**
   * Used by BFT client to verify signature share on an output.
   */
  bool verifySigShare(size_t txoIdx, const RandSigShare& sigShare, const RandSigSharePK& bpkShare) const {
    G1 H;

    if (outs[txoIdx].H.has_value()) {
      // This will be cached here if the client created the TXN, sent it to the replicas, received back sigshares and
      // called this method to verify them
      H = *outs[txoIdx].H;
    } else {
      // TODO(Perf): The client who calls this shouldn't have to recompute H here, but this is the easiest way right now
      H = deriveRandSigBase(txoIdx);
    }

    return sigShare.verify(getCommVector(txoIdx, H), bpkShare);
  }

  /**
   * Attempts to claim the output specified by 'idx':
   * i.e., decrypt the denomination, identity commitment randomness and value commitment randomness from the coin's
   * ciphertext.
   *
   * If it succeeds, it additionally aggregates all the provided signature shares into a coin signature
   * and returns a signed, re-randomized coin.
   *
   * WARNING: Assuming there is no need to check the coin signature since it comes from the signed TXN ledger, so it is
   * known to be valid.
   */
  std::optional<Coin> tryClaimCoin(const Params& p,
                                   size_t txoIdx,
                                   const AddrSK& ask,
                                   size_t n,
                                   const std::vector<RandSigShare>& sigShares,
                                   const std::vector<size_t>& signerIds,
                                   const RandSigPK& bpk) const;

  std::string getHashHex() const {
    std::stringstream ss;
    ss << *this;

    // TODO(Crypto): See libutt/hashing.md
    return hashToHex("tx|" + ss.str());
  }

  Fr getSN(size_t txoIdx) const {
    // TODO(Crypto): See libutt/hashing.md
    return hashToField("sn|" + getHashHex() + "|" + std::to_string(txoIdx));
  }

 public:
  bool operator==(const Tx& o) const {
    return isSplitOwnCoins == o.isSplitOwnCoins && rcm == o.rcm && regsig == o.regsig &&
           // TODO: ins == o.ins &&, if we need to
           // TODO: outs == o.outs &&, if we need to
           budget_pi == o.budget_pi && true;
  }

  bool operator!=(const Tx& o) const { return !operator==(o); }
};

}  // end of namespace libutt
