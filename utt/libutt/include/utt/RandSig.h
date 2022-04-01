#pragma once

#include <iostream>

#include <utt/Comm.h>
#include <utt/PolyCrypto.h>

namespace libutt {
class Comm;
class CommKey;
class RandSig;
class RandSigPK;
class RandSigShare;
class RandSigShareSK;
class RandSigSharePK;
}  // namespace libutt

std::ostream& operator<<(std::ostream&, const libutt::RandSig&);
std::istream& operator>>(std::istream&, libutt::RandSig&);

std::ostream& operator<<(std::ostream&, const libutt::RandSigPK&);
std::istream& operator>>(std::istream&, libutt::RandSigPK&);

std::ostream& operator<<(std::ostream&, const libutt::RandSigShare&);
std::istream& operator>>(std::istream&, libutt::RandSigShare&);

std::ostream& operator<<(std::ostream&, const libutt::RandSigShareSK&);
std::istream& operator>>(std::istream&, libutt::RandSigShareSK&);

// NOTE: RandSigSharePK just inherits RandSigPK
// std::ostream& operator<<(std::ostream&, const libutt::RandSigSharePK&);
// std::istream& operator>>(std::istream&, libutt::RandSigSharePK&);

namespace libutt {

/**
 * Secret key for a non-threshold PS16 signature
 * (See RandSigShareSK for threshold version.)
 */
class RandSigSK {
 public:
  CommKey ck;  // technically not a secret, but needed for toPK()
  Fr x;
  G1 X;
  std::vector<Fr> y;  // not needed for signing commitments, but using it for debugging the DKG

 public:
  static RandSigSK random(const CommKey& ck);

 public:
  // Returns a PS16 signature, with randomness u, on the specified commitment c
  // NOTE: For now, only used by the single-server registration authority (see utt/RegAuth.h)
  RandSig sign(const Comm& c, const Fr& u) const;
  RandSig sign(const Comm& c) const;

  RandSigPK toPK() const;
};

/**
 * Share SK for threshold PS16.
 */
class RandSigShareSK {
 public:
  CommKey ck;  // technically not a secret, but needed for toPK()
  /*
   * This ith signer's shares of x and y_k's
   * i.e., [x]_i and [y_k]_i for all k\in \{1,\dots, \ell\}
   */
  std::vector<Fr> x_and_ys;

 public:
  /**
   * Returns a PS16 signature share, with randomness h = g^u, on the specified commitment c
   *
   * WARNING: Assuming caller has a proof-of-knowledge of opening of the commitments in c, which is the case in UTT.
   */
  RandSigShare shareSign(const std::vector<Comm>& c, const G1& h) const;

  RandSigSharePK toPK() const;

  bool operator==(const RandSigShareSK& o) const { return x_and_ys == o.x_and_ys; }

  bool operator!=(const RandSigShareSK& o) const { return !operator==(o); }
};

/**
 * Public key for a PS16 signature
 */
class RandSigPK {
 public:
  G1 g;
  G2 g_tilde;
  G2 X_tilde;
  std::vector<G2> Y_tilde;  // not needed for verifying sigs commitments, but using it for debugging the DKG

 public:
  bool operator==(const RandSigPK& o) const {
    return g == o.g && g_tilde == o.g_tilde && X_tilde == o.X_tilde && Y_tilde == o.Y_tilde && true;
  }

  bool operator!=(const RandSigPK& o) const { return !operator==(o); }
};

/**
 * Share PK for threshold PS16.
 */
class RandSigSharePK : public RandSigPK {};

/**
 * A PS16 signature on a Comm.
 */
class RandSig {
 public:
  G1 s1;  // i.e., h = g^u
  G1 s2;  // i.e., h^{x + \sum_i m_i y_i}

 public:
  size_t getSize() const { return _g1_size + _g1_size; }

  RandSig() {}
  RandSig(std::istream& in) { in >> *this; }

  // RandSig(RandSig&& other)
  //    : s1(std::move(other.s1)), s2(std::move(other.s2))
  //{}

 public:
  void rerandomize(const Fr& r_delta, const Fr& u_delta) {
    // s2 = (s2 * s1^r_delta)^u_delta
    //    = s2^u_delta s1^{r_delta * u_delta}
    s2 = multiExp(s2, s1, u_delta, r_delta * u_delta);

    // WARNING: s1 above is different than s1 below! So don't move this line above!
    s1 = u_delta * s1;
  }

  /**
   * Checks the PS16 signature on the specified commitment
   *
   * WARNING: We do NOT verify that cc.hasCorrectG2(). We assume this is done in Tx::validate()
   */
  bool verify(const Comm& cc, const RandSigPK& pk) const;

 public:
  bool operator==(const RandSig& o) const { return s1 == o.s1 && s2 == o.s2; }
  bool operator!=(const RandSig& o) const { return !operator==(o); }
};

/**
 * A PS16 signature *share* on a Comm
 */
class RandSigShare {
 public:
  G1 s1;  // i.e., h_i = g^{u_i}
  G1 s2;  // i.e., h^{x_i} (\prod_i g_i^m_i g^r)^{u_i}

 public:
  RandSigShare() {}

  RandSigShare(std::istream& in) { in >> *this; }

  RandSigShare(const G1& s1, const G1& s2) : s1(s1), s2(s2) {}

 public:
  /**
   * Checks the PS16 signature *share* on the specified commitments.
   *
   * WARNING: We do NOT verify that each cc.hasCorrectG2(). We assume this is done in Tx::validate()
   */
  bool verify(const std::vector<Comm>& c, const RandSigSharePK& pk) const;

 public:
  /**
   * Aggregates a threshold signature from a subset of size 't' of the total 'n' replicas.
   *
   * The subset of replicas IDs (from {0, 1, ..., n-1}) is in 'signerIds'.
   * sigShares[i] is the signature share computed by the replica with ID signerIds[i].
   *
   * r[k] is the randomness used for the kth commitment passed into RandSigShareSK::shareSign
   */
  static RandSig aggregate(size_t n,
                           const std::vector<RandSigShare>& sigShares,
                           const std::vector<size_t>& signerIds,
                           const CommKey& ck,
                           const std::vector<Fr>& r);

 public:
  bool operator==(const RandSigShare& o) const { return s1 == o.s1 && s2 == o.s2 && true; }

  bool operator!=(const RandSigShare& o) const { return !operator==(o); }
};

}  // end of namespace libutt
