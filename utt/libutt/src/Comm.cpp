#include <utt/Configuration.h>

#include <utt/Comm.h>

#include <utt/Serialization.h>  // WARNING: Include this last (see header file for details; thanks, C++)

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream& out, const libutt::CommKey& ck) {
  assertTrue(!ck.hasG2() || ck.g.size() == ck.g_tilde.size());

  out << ck.g << endl;
  out << ck.g_tilde << endl;

  return out;
}

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::istream& operator>>(std::istream& in, libutt::CommKey& ck) {
  in >> ck.g;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> ck.g_tilde;
  libff::consume_OUTPUT_NEWLINE(in);

  assertTrue(!ck.hasG2() || ck.g.size() == ck.g_tilde.size());

  return in;
}

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream& out, const libutt::Comm& cc) {
  out << cc.ped1 << endl;
  out << cc.ped2 << endl;

  return out;
}

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::istream& operator>>(std::istream& in, libutt::Comm& cc) {
  in >> cc.ped1;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> cc.ped2;
  libff::consume_OUTPUT_NEWLINE(in);

  return in;
}

namespace libutt {
CommKey CommKey::fromTrapdoor(const G1& g, const G2& g_tilde, const std::vector<Fr>& y) {
  size_t n = y.size();
  assertStrictlyGreaterThan(n, 0);

  CommKey ck;

  ck.g.resize(n);
  ck.g_tilde.resize(n);

  for (size_t i = 0; i < n; i++) {
    ck.g[i] = y[i] * g;
    ck.g_tilde.at(i) = y[i] * g_tilde;
  }

  ck.g.push_back(g);
  ck.g_tilde.push_back(g_tilde);

  assertEqual(ck.g.size(), ck.g_tilde.size());
  assertEqual(ck.g.size(), n + 1);

  return ck;
}

CommKey CommKey::random(size_t n) {
  auto g = G1::random_element();
  auto g_tilde = G2::random_element();
  auto e = random_field_elems(n);

  return CommKey::fromTrapdoor(g, g_tilde, e);
}

std::string CommKey::toString() const {
  std::stringstream ss;
  ss << *this;
  return ss.str();
}

bool Comm::hasCorrectG2(const CommKey& ck) const {
  testAssertTrue(hasG2());
  // TODO(Perf): avoid this inversion
  return MultiPairing({ped1, -ck.getGen1()}, {ck.getGen2(), *ped2}) == GT::one();
  // return ReducedPairing(ped1, ck.getGen2()) == ReducedPairing(ck.getGen1(), *ped2);
}

void Comm::rerandomize(const CommKey& ck, const Fr& r_delta) {
  ped1 = ped1 + r_delta * ck.getGen1();

  if (hasG2()) *ped2 = *ped2 + r_delta * ck.getGen2();
}

Comm Comm::create(const CommKey& ck, const std::vector<Fr>& m, bool withG2) {
  assertEqual(m.size(), ck.numMessages() + 1);

  Comm cm;
  cm.ped1 = multiExp<G1>(ck.g, m);

  // TODO(libff): Bug https://github.com/scipr-lab/libff/issues/108
  // assertEqual(ReducedPairing(G1::zero(), ck.getGen2()), GT::one()^Fr::zero());
  // assertEqual(ReducedPairing(ck.getGen1(), G2::zero()), GT::one()^Fr::zero());
  // assertEqual(ReducedPairing(G1::zero(), ck.getGen2()), ReducedPairing(ck.getGen1(), G2::zero()));
  if (withG2) {
    assertTrue(ck.hasG2());
    cm.ped2 = multiExp<G2>(ck.g_tilde, m);
    assertTrue(ck.hasCorrectG2());
    assertTrue(cm.hasCorrectG2(ck));
  }

  return cm;
}

std::vector<Comm> Comm::create(const CommKey& ck, const std::vector<Fr>& m, const std::vector<Fr>& r, bool withG2) {
  assertEqual(m.size(), r.size());
  assertEqual(ck.numMessages(), 1);

  std::vector<Comm> comms;
  for (size_t k = 0; k < m.size(); k++) {
    comms.push_back(Comm::create(ck, {m[k], r[k]}, withG2));
  }

  return comms;
}
}  // namespace libutt
