#pragma once

#include <iostream>

#include <utt/Comm.h>
#include <utt/IBE.h>
#include <utt/Nullifier.h>
#include <utt/PolyCrypto.h>
#include <utt/RangeProof.h>

#include <xassert/XAssert.h>

// WARNING: Forward declaration(s), needed for the serialization declarations below
namespace libutt {
class Params;
}

// WARNING: Declare (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream&, const libutt::Params&);
std::istream& operator>>(std::istream&, libutt::Params&);

namespace libutt {

/**
 * Parameters for UTT, consisting of:
 *
 *  - Type III Bilinear groups G_1 and G_2 (w/ generators g and g_tilde)
 *  - Coin commitment key
 *  - Registration commitment key
 *  - Pointcheval-Sanders parameters
 *    + Same as coin commitment key
 *  - PRF public parameters (i.e., Boneh-Boyen sigs or Dodis-Yampolskiy VRFs)
 *    + A few extra things for ZK proof of evaluation for PRF
 *  - IBE public parameters
 *  - range proof public parameters
 */
class Params {
 public:
  // Coin commitment key
  CommKey ck_coin;

  // Registration commitment key
  CommKey ck_reg;

  // Value commitment key
  CommKey ck_val;

  // PRF public params
  Nullifier::Params null;

  // IBE public params
  IBE::Params ibe;

  // range proof public parameters
  RangeProof::Params rpp;

  // The number of sub-messages that compose a coin, which we commit to via Pedersen:
  // (pid, sn, v, type, expdate) -> 5
  static const size_t NumMessages = 5;

 public:
  // WARNING: For deserialization only
  Params() {}

  // Desearializes Params from an istream
  Params(std::istream& in) : Params() { in >> *this; }

 public:
  // Creates random public parameters
  static Params random();

  // Creates random public parameters, but for a pre-specified commitment key (currently computed via a RandSigDKG)
  static Params random(const CommKey& ck_coin);

 public:
  const CommKey& getCoinCK() const { return ck_coin; }
  const CommKey& getRegCK() const { return ck_reg; }
  const CommKey& getValCK() const { return ck_val; }
  IBE::Params getIbeParams() const { return ibe; }
  RangeProof::Params getRangeProofParams() const { return rpp; }

 public:
  bool operator==(const Params& o) const;

  bool operator!=(const Params& o) const { return !operator==(o); }
};
}  // namespace libutt
