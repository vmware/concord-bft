#pragma once

#include <utt/PolyCrypto.h>

// WARNING: Forward declaration(s), needed for the serialization declarations below
namespace libutt {
class Nullifier;
}

// WARNING: Declare (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream&, const libutt::Nullifier&);
std::istream& operator>>(std::istream&, libutt::Nullifier&);

namespace libutt {

// foward declarations
class AddrSK;

/**
 * When a coin with PRF key s and serial number sn is spent, it must be marked as 'spent' by the bank.
 *
 * This is done by:
 *     (1) deriving a privacy-preserving nullifier h^{1/(s+sn)} which is not linkable to the coin owner's pid and their
 * IBE encryption PK (2) adding this nullifer to a 'spent list'
 */
class Nullifier {
 public:
  class Params {
   public:
    G1 h;
    G2 h_tilde, w_tilde;
    GT ehh;  // e(h, h_tilde)

   public:
    static Params random() {
      Params p;
      p.h = G1::random_element();
      p.h_tilde = G2::random_element();
      p.w_tilde = G2::random_element();
      p.ehh = ReducedPairing(p.h, p.h_tilde);
      return p;
    }

   public:
    bool operator==(const Params& o) const { return h == o.h && h_tilde == o.h_tilde && w_tilde == o.w_tilde && true; }

    bool operator!=(const Params& o) const { return !operator==(o); }
  };

 public:
  G1 n;

  /**
   * We store these here, since it makes nullifier creation and verification
   * very convenient.
   */
  GT y;   // e(nullif, w_tilde)^t
  G2 vk;  // h_tilde^{s+sn} w_tilde^t

 public:
  size_t getSize() const { return _g1_size + _gt_size + _g2_size; }
  // WARNING: For deserialization only!
  Nullifier() {}

  Nullifier(std::istream& in) : Nullifier() { in >> *this; }
  Nullifier(const Params& p, const Fr& prf, const Fr& sn, const Fr& t);
  /**
   * Used to create a nullifier when spending the specified coin via a transaction.
   *
   * @param   t   randomizer used for the nullifier proof
   */
  Nullifier(const Params& p, const AddrSK& ask, const Fr& sn, const Fr& t);

 public:
  /**
   * WARNING: This is what one should append to the nullifier list, since
   * the serialization returns randomized components (y, vk) which would
   * lead to different nullifiers for the same coin.
   */
  std::string toUniqueString() const {
    std::stringstream ss;
    ss << n;
    return ss.str();
  }

  /**
   * Checks that the nullifer was computed consistently.
   *
   * NOTE: This is only meaningful after a \Sigma-protocol ZK verification has vouched for the
   * correcteness of y and vk.
   */
  bool verify(const Params& p) const;

 public:
  bool operator==(const Nullifier& o) const { return n == o.n && y == o.y && vk == o.vk && true; }

  bool operator!=(const Nullifier& o) const { return !operator==(o); }
};

}  // end of namespace libutt

std::ostream& operator<<(std::ostream&, const libutt::Nullifier::Params&);
std::istream& operator>>(std::istream&, libutt::Nullifier::Params&);
