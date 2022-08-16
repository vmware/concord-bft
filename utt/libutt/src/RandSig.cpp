#include <utt/Configuration.h>

#include <utt/Comm.h>
#include <utt/PolyOps.h>
#include <utt/RandSig.h>

std::ostream& operator<<(std::ostream& out, const libutt::RandSig& sig) {
  out << sig.s1 << endl;
  out << sig.s2 << endl;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::RandSig& sig) {
  in >> sig.s1;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> sig.s2;
  libff::consume_OUTPUT_NEWLINE(in);

  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::RandSigPK& pk) {
  out << pk.g << endl;
  out << pk.g_tilde << endl;
  out << pk.X_tilde << endl;
  out << pk.Y_tilde;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::RandSigPK& pk) {
  in >> pk.g;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pk.g_tilde;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pk.X_tilde;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pk.Y_tilde;
  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::RandSigShare& sigShare) {
  out << sigShare.s1 << endl;
  out << sigShare.s2 << endl;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::RandSigShare& sigShare) {
  in >> sigShare.s1;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> sigShare.s2;
  libff::consume_OUTPUT_NEWLINE(in);

  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::RandSigShareSK& shareSK) {
  out << shareSK.ck;
  out << shareSK.x_and_ys;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::RandSigShareSK& shareSK) {
  in >> shareSK.ck;
  in >> shareSK.x_and_ys;
  return in;
}

// NOTE: RandSigSharePK just inherits RandSigPK
// std::ostream& operator<<(std::ostream& out, const libutt::RandSigSharePK& sharePK) {
//    out << sharePK.g_tilde << endl;
//    out << sharePK.X_tilde << endl;
//    out << sharePK.Y_tilde;
//    return out;
//}
//
// std::istream& operator>>(std::istream& in, libutt::RandSigSharePK& sharePK) {
//    in >> sharePK.g_tilde;
//    libff::consume_OUTPUT_NEWLINE(in);
//    in >> sharePK.X_tilde;
//    libff::consume_OUTPUT_NEWLINE(in);
//    in >> sharePK.Y_tilde;
//    return in;
//}

namespace libutt {

RandSigSK RandSigSK::random(const CommKey& ck) {
  RandSigSK sk;

  sk.ck = ck;
  sk.x = Fr::random_element();
  sk.X = sk.x * ck.getGen1();

  return sk;
}

RandSig RandSigSK::sign(const Comm& c, const Fr& u) const {
  RandSig sig;
  sig.s1 = u * ck.getGen1();
  sig.s2 = u * (X + c.ped1);
  return sig;
}

RandSig RandSigSK::sign(const Comm& c) const { return RandSigSK::sign(c, Fr::random_element()); }

RandSigPK RandSigSK::toPK() const {
  RandSigPK pk;

  pk.g = ck.getGen1();
  pk.g_tilde = ck.getGen2();
  pk.X_tilde = x * ck.getGen2();
  for (size_t k = 0; k < y.size(); k++) {
    pk.Y_tilde.push_back(y[k] * pk.g_tilde);
  }

  return pk;
}

RandSigShare RandSigShareSK::shareSign(const std::vector<Comm>& c, const G1& h) const {
  RandSigShare sigShare;

  testAssertEqual(c.size(), x_and_ys.size() - 1);

  // the first part is trivial: just the base h
  sigShare.s1 = h;

  // the second part is: h^{[x]_i} \prod_k cm_k^{[y_k]_i}
  std::vector<G1> c1;
  c1.push_back(h);
  for (auto& cm : c) c1.push_back(cm.asG1());

  sigShare.s2 = multiExp(c1, x_and_ys);

  return sigShare;
}

RandSigSharePK RandSigShareSK::toPK() const {
  assertStrictlyGreaterThan(x_and_ys.size(), 0);

  RandSigSharePK pk;

  pk.g = ck.getGen1();
  pk.g_tilde = ck.getGen2();
  pk.X_tilde = x_and_ys[0] * pk.g_tilde;
  pk.Y_tilde.resize(x_and_ys.size() - 1);

  for (size_t i = 0; i < x_and_ys.size() - 1; i++) {
    pk.Y_tilde[i] = x_and_ys[i + 1] * pk.g_tilde;
  }

  return pk;
}

bool RandSig::verify(const Comm& cc, const RandSigPK& pk) const {
  // TODO(Perf): precompute -pk.g_tilde
  CommKey cktemp;
  cktemp.g.push_back(pk.g);
  cktemp.g_tilde.push_back(pk.g_tilde);

  return cc.hasCorrectG2(cktemp) && MultiPairing({s2, s1}, {-pk.g_tilde, pk.X_tilde + cc.asG2()}) == GT::one();
  // return ReducedPairing(s2, pk.g_tilde) == ReducedPairing(s1, pk.X_tilde + cc.asG2());
}

bool RandSigShare::verify(const std::vector<Comm>& c, const RandSigSharePK& pk) const {
  assertEqual(c.size(), pk.Y_tilde.size());

  std::vector<G1> g1s;
  std::vector<G2> g2s;

  g1s.push_back(s2);
  g2s.push_back(-pk.g_tilde);  // TODO(Perf): precompute -pk.g_tilde
  g1s.push_back(s1);
  g2s.push_back(pk.X_tilde);
  for (size_t i = 0; i < c.size(); i++) {
    g1s.push_back(c[i].asG1());
    g2s.push_back(pk.Y_tilde[i]);
  }
  return MultiPairing(g1s, g2s) == GT::one();

  // auto lhs = ReducedPairing(s2, pk.g_tilde);
  // auto rhs = ReducedPairing(s1, pk.X_tilde);
  // for(size_t i = 0; i < c.size(); i++) {
  //    rhs = rhs * ReducedPairing(c[i].asG1(), pk.Y_tilde[i]);
  //}
  // return lhs == rhs;
}
RandSig RandSigShare::aggregate(size_t n,
                                const std::vector<RandSigShare>& sigShares,
                                const std::vector<size_t>& signerIds) {
  RandSig sig;

  // WARNING: Assuming all shares verify and have the same base 'h' is wrong
  // since individual calls on RandSigShare::verify(sigShares[i], ...) will not guarantee this!
  // Instead, the caller must ensure this manually, which we do here.
  sig.s1 = sigShares[0].s1;
  std::vector<G1> s2;
  for (auto& ss : sigShares) {
    if (ss.s1 != sig.s1) {
      throw std::runtime_error("Expected signature shares with the same 'h'");
    }

    s2.push_back(ss.s2);
  }

  auto lagr = lagrange_coefficients_naive(n, signerIds);

  sig.s2 = multiExp(s2, lagr);
  return sig;
}
RandSig RandSigShare::aggregate(size_t n,
                                const std::vector<RandSigShare>& sigShares,
                                const std::vector<size_t>& signerIds,
                                const CommKey& ck,
                                const std::vector<Fr>& r) {
  RandSig sig;

  // WARNING: Assuming all shares verify and have the same base 'h' is wrong
  // since individual calls on RandSigShare::verify(sigShares[i], ...) will not guarantee this!
  // Instead, the caller must ensure this manually, which we do here.
  sig.s1 = sigShares[0].s1;
  std::vector<G1> s2;
  for (auto& ss : sigShares) {
    if (ss.s1 != sig.s1) {
      throw std::runtime_error("Expected signature shares with the same 'h'");
    }

    s2.push_back(ss.s2);
  }

  auto lagr = lagrange_coefficients_naive(n, signerIds);

  sig.s2 = multiExp(s2, lagr);

  // unblind sig.s2
  std::vector<G1> g = ck.g;  // g_1, g_2, \dots, g_\ell, g
  g.pop_back();              // g_1, g_2, \dots, g_\ell

  sig.s2 = sig.s2 - multiExp(g, r);

  return sig;
}
}  // namespace libutt
