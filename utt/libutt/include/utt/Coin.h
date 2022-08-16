#pragma once

#include <iostream>
#include <limits>

#include <utt/Comm.h>
#include <utt/RandSig.h>
#include <utt/Nullifier.h>
#include <utt/PolyCrypto.h>

#include <xutils/Log.h>

// WARNING: Forward declaration(s), needed for the serialization declarations below
namespace libutt {
class AddrSK;
class CommKey;
class Coin;
class Factory;
class RandSigPK;
class RandSigSK;
}  // namespace libutt

// WARNING: Declare (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream&, const libutt::Coin&);
std::istream& operator>>(std::istream&, libutt::Coin&);

namespace libutt {
/**
 * The private & public information associated with a coin, whose knowledge is necessary for spending it.
 */
class Coin {
 public:
  friend class Factory;
  friend std::ostream& ::operator<<(std::ostream&, const libutt::Coin&);
  friend std::istream& ::operator>>(std::istream&, libutt::Coin&);  // needs access to ccm_txn

 public:
  CommKey ck;   // coin commitment key, needed for re-randomization
  Fr pid_hash;  // owner's PID hash
  Fr sn;        // serial number
  Fr val;       // denomination

  // TODO: turn CoinType and ExpirationDate into their own class with an toFr() method, or risk getting burned!
  Fr type;      // either Coin::NormalType() or Coin::BudgetType()
  Fr exp_date;  // expiration date; TODO: Fow now, using 32/64-bit UNIX time, since we never check expiration in the
                // current prototype

  Fr r;  // randomness used to commit to the coin

  RandSig sig;  // signature on coin commitment from bank

  //
  // NOTE: We pre-compute these when we create/claim a coin, to make spending it faster!
  //
  Fr t;            // randomness for nullifier proof
  Nullifier null;  // nullifier for this coin, pre-computed for convenience

  Comm vcm;  // value commitment for this coin, pre-computed for convenience
  Fr z;      // value commitment randomness

  // NOTE: Cannot have splitproof here because that also uses the registration signature

 protected:
  /**
   * Partial coin commitment to (pid, sn, val) with randomness r
   * This is what is included in a TXN.
   *
   * Full coin commitment to (pid, sn, val, type, exp_date), also with randomness r,
   * would be what coin signatures would be verified against.
   *
   * However, instead of storing it here separately, we compute it on the fly via Coin::augmentComm()
   */
  Comm ccm_txn;

 public:
  // WARNING: Used only for deserialization
  Coin() {}
  // Used to create the coin's partial commitment (without nullfier)
  Coin(const CommKey& ck, const Fr& sn, const Fr& val, const Fr& type, const Fr& exp_date, const Fr& pidHash);

  Coin(const CommKey& ck,
       const Nullifier::Params& np,
       const Fr& prf,
       const Fr& sn,
       const Fr& val,
       const Fr& type,
       const Fr& exp_date,
       const Fr& pidHash);

  // Used in TxOut::tryMakeMine and Factory
  Coin(const CommKey& ck,
       const Nullifier::Params& np,
       const Fr& sn,
       const Fr& val,
       const Fr& type,
       const Fr& exp_date,
       const AddrSK& ask);

  // Used to deserialize Coin from a file
  Coin(std::istream& in) : Coin() { in >> *this; }

 public:
  /**
   * Randomly generates an incomplete coin (no signature, nullifier or vcm).
   *
   * Used for testing and setting up benchmarking Wallet objects.
   */
  static Coin random(size_t val, const Fr& type) {
    assertTrue(type == NormalType() || type == BudgetType());

    Coin c;
    c.pid_hash = Fr::random_element();
    c.sn = Fr::random_element();
    c.val = Fr(static_cast<long>(val));
    c.type = type;
    c.t = Fr::random_element();
    c.r = Fr::random_element();
    return c;
  }

  // TODO(libff): We use Fr(2) rather than Fr(0) here due to a bug in libff:
  // https://github.com/scipr-lab/libff/issues/108
  static Fr NormalType() { return Fr::one() + Fr::one(); }
  static Fr BudgetType() { return Fr::one(); }

  // TODO: figure out how you want to encode a month+year in an Fr
  static Fr SomeExpirationDate() { return Fr(2392391); }
  // TODO(libff): We use Fr::one() rather than Fr::zero() here due to a bug in libff:
  // https://github.com/scipr-lab/libff/issues/108
  static Fr DoesNotExpire() { return Fr::one(); }

  static std::string typeToString(const Fr& type) {
    if (type == Coin::NormalType()) {
      return "normal";
    } else if (type == Coin::BudgetType()) {
      return "budget";
    } else {
      return "unknown";
    }
  }

  /**
   * Returns the total value of the specified coins.
   *
   * NOTE: Used for sanity checking in Tx::Tx()
   */
  static size_t totalValue(const std::vector<Coin>& coins) {
    size_t val = 0;
    for (const auto& c : coins) {
      val += c.getValue();
    }
    return val;
  }

  /**
   * Coin::ccm_txn (or Coin::commForTxn()) is a *partial* coin commitment which
   * does not include the coin type and expiration date.
   *
   * This call augments such a partial commitment with the type and expiration date.
   * i.e., returns g_1^pid g_2^sn g_3^r g_4^type g_5^expdarte g^r, which is needed to verify signatures against
   *
   * Called in Tx::validate
   */
  static Comm augmentComm(const CommKey& coinCK, const Comm& ccmTxn, const Fr& type, const Fr& exp_date);

 protected:
  /**
   * Computes the partial coin commitment: g_1^pid g_2^sn g_3^val g^r (assumes r is pre-set) and the value commitment
   * g_3^val g^z, by picking a random z
   */
  void commit();

 public:
  /**
   * Call this when you need a full coin commitment to sign using RandSigSK::sign or
   * to verify using RandSig::verify
   *
   * Called in Factory::mintRandomCoin() and in Coin::hasValidSig()
   */
  Comm augmentComm() const { return Coin::augmentComm(ck, ccm_txn, type, exp_date); }
  void createNullifier(const Nullifier::Params& np, const Fr& prf);
  bool isNormal() const { return type == Coin::NormalType(); }
  bool isBudget() const { return type == Coin::BudgetType(); }

  size_t getValue() const { return static_cast<size_t>(val.as_ulong()); }
  std::string getType() const { return typeToString(type); }
  Fr getExpDate() const { return exp_date; }
  /**
   * Call this when you need a partial coin commitment to include in a TXN
   * via Tx::Tx()
   */
  const Comm& commForTxn() const { return ccm_txn; }

  /**
   * Computes the full coin commitment and checks that it is correctly signed.November
   */
  bool hasValidSig(const RandSigPK& pk) const;

  /**
   * Builds a partial coin commitment from the (pid, sn, val) coin secrets.
   */

 public:
  bool operator==(const Coin& o) const {
    return ck == o.ck && pid_hash == o.pid_hash && sn == o.sn && val == o.val && type == o.type &&
           exp_date == o.exp_date && r == o.r && sig == o.sig && t == o.t && null == o.null && vcm == o.vcm &&
           z == o.z && ccm_txn == o.ccm_txn && true;
  }

  bool operator!=(const Coin& o) const { return !operator==(o); }
};

}  // end of namespace libutt
