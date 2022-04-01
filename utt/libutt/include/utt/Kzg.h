#pragma once

#include <utt/PolyCrypto.h>

namespace libutt {
namespace KZG {
class Params;
}
}  // namespace libutt

std::ostream& operator<<(std::ostream& out, const libutt::KZG::Params& kpp);
std::istream& operator>>(std::istream& in, libutt::KZG::Params& kpp);

namespace libutt {

namespace KZG {

class Params {
 public:
  // TODO(Crypto): remove KZG trapdoor from parameters or mark it as insecure somehow
  Fr s;                  // trapdoor (useful for debugging; never used in practice)
  size_t q;              // max degree of polynomials we can commit to
  std::vector<G1> g1si;  // g1si[i] = g1^{s^i}
  std::vector<G2> g2si;  // g2si[i] = g2^{s^i}, don't need all of these for range proofs (but maybe later on)
  GT eg1g2;              // e(g1, g2) precomputed (can be useful)
  G2 g2inv;              // i.e., g2^{-1}

 protected:
  /**
   * Generates Kate public parameters on the fly. Used for testing.
   */
  Params(size_t q);

 public:
  Params() : q(0) {}

 public:
  static Params random(size_t q) { return Params(q); }

 public:
  /**
   * Returns the max degree of a polynomial these parameters can be used to commit to.
   */
  size_t getMaxDegree() const {
    assertEqual(g1si.size() - 1, q);
    assertEqual(g2si.size() - 1, q);
    return q;
  }

  void resize(size_t newQ) {
    q = newQ;
    g1si.resize(q + 1);  // g1^{s^i} with i from 0 to q, including q
    g2si.resize(q + 1);  // g2^{s^i} with i from 0 to q, including q
  }

  const G1& g1(size_t i = 0) const { return g1si[i]; }
  const G2& g2(size_t i = 0) const { return g2si[i]; }

  const G1& g1toS() const { return g1si[1]; }

  const G2& g2toS() const {
    assertStrictlyGreaterThan(g2si.size(), 1);
    return g2si[1];
  }

  const Fr& getTrapdoor() const { return s; }

  /**
   * Verifies that value = p(point), where p is committed in polyComm.
   */
  bool verifyProof(const G1& polyComm, const G1& proof, const Fr& value, const Fr& point) const;

  /**
   * Verifies that valComm = g^p_j(z), where p_j is committed in polyComm and z is committed in acc = g2^{s - z}.
   */
  bool verifyProof(const G1& polyComm, const G1& proof, const G1& valComm, const G2& acc) const;

 public:
  bool operator==(const Params& o) const { return q == o.q && s == o.s && g1si == o.g1si && g2si == o.g2si && true; }

};  // end of Params

/**
 * Commits to the polynomial 'f' in the group 'Group' using KZG public params 'kpp'.
 */
template <typename Group>
Group commit(const Params& kpp, const std::vector<Fr>& f) {
  assertGreaterThanOrEqual(kpp.getMaxDegree(), f.size() - 1);

  return multiExp<Group>(kpp.g1si.begin(), kpp.g1si.begin() + static_cast<long>(f.size()), f.begin(), f.end());
}

/**
 * Evaluates f(x) at x = point by dividing:
 *
 *    q = f / (x - point),
 *    r = f mod (x - point) = f(point)
 *
 * Returns the quotient polynomial in q and the evaluation in r
 */
std::tuple<std::vector<Fr>, Fr> naiveEval(const std::vector<Fr>& f, const Fr& point);

/**
 * Returns the proof for f_id(point) and f_id(point) itself.
 */
std::tuple<G1, Fr> naiveProve(const Params& kpp, const std::vector<Fr>& f, const Fr& point);

}  // namespace KZG

}  // namespace libutt
