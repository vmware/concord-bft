#include <utt/Configuration.h>

#include <cstdlib>

#include <utt/Address.h>
#include <utt/Comm.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>
#include <utt/PolyOps.h>

std::ostream& operator<<(std::ostream& out, const libutt::RegAuthPK& rpk) {
  out << rpk.vk;
  out << rpk.mpk;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::RegAuthPK& rpk) {
  in >> rpk.vk;
  in >> rpk.mpk;
  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::RegAuthSharePK& rpk) {
  out << rpk.vk;
  out << rpk.mpk;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::RegAuthSharePK& rpk) {
  in >> rpk.vk;
  in >> rpk.mpk;
  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::RegAuthShareSK& rsk) {
  out << rsk.sk;
  out << rsk.msk;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::RegAuthShareSK& rsk) {
  in >> rsk.sk;
  in >> rsk.msk;
  return in;
}
namespace libutt {

RegAuthSK RegAuthSK::random(const CommKey& ck_reg, const IBE::Params& p) {
  RegAuthSK rsk;

  rsk.ck_reg = ck_reg;

  // pick PS16 SK to sign registration commitments
  rsk.sk = RandSigSK::random(ck_reg);

  // pick Boneh-Franklin MSK
  rsk.p = p;
  rsk.msk = IBE::MSK::random();

  return rsk;
}

RegAuthSK RegAuthSK::generateKeyAndShares(CommKey& ck_reg, size_t t, size_t n, const IBE::Params& p_) {
  (void)(ck_reg);
  RegAuthSK ras;
  ras.p = p_;
  ras.msk = IBE::MSK::random();
  size_t ell = 2;  // Registration commit contains only 3 messages
  // degree t-1 polynomial f(X) such that t players can reconstruct f(0) = x, which encodes the bank's secret key x
  std::vector<Fr> poly_f = random_field_elems(t);
  Fr x = poly_f[0];

  // degree t-1 polynomials Y_k(X) for secret sharing the y_k = Y_k(0)
  std::vector<std::vector<Fr>> polys_y;
  // PS16 secrets y_1, y_2, \dots, y_k
  std::vector<Fr> y;
  for (size_t k = 0; k < ell; k++) {
    auto poly = random_field_elems(t);
    polys_y.push_back(poly);
    y.push_back(poly[0]);
  }

  G1 g = G1::random_element();
  G2 g_tilde = G2::random_element();
  auto ck = CommKey::fromTrapdoor(g, g_tilde, y);

  ras.ck_reg = ck;
  // initialize the single-server PS16 SK (useful for testing)
  ras.sk.ck = ck;
  ras.sk.x = x;
  ras.sk.y = y;
  ras.sk.X = x * g;

  /**
   * Compute the SK shares [x]_i, [y_k]_i, \forall k\in \{1,\dots,\ell\}
   * i.e., evaluate the secret-sharing polys at n pre-determined points (i.e., Nth roots of unity, with smallest N = 2^k
   * >= n)
   */
  std::vector<Fr> evals_f = poly_fft(poly_f, n);

  std::vector<std::vector<Fr>> evals_y;
  for (size_t k = 0; k < ell; k++) {
    evals_y.push_back(poly_fft(polys_y[k], n));
  }

  // [x]_i = f(\omega_N^i)
  // [y_k]_i = Y_k(\omega_N^i), \forall k\in\{1,\dots,\ell\}
  std::vector<RegAuthShareSK> skRegShares;
  for (size_t i = 0; i < n; i++) {
    RegAuthShareSK as;
    RandSigShareSK ss;

    ss.ck = ras.ck_reg;
    ss.x_and_ys.push_back(evals_f[i]);
    for (size_t k = 0; k < ell; k++) {
      ss.x_and_ys.push_back(evals_y[k][i]);
    }
    as.sk = ss;
    as.ck_reg = ras.ck_reg;
    as.p = ras.p;
    as.msk = ras.msk;
    skRegShares.push_back(as);
  }
  ras.shares = skRegShares;
  return ras;
}
// AddrSK RegAuthSK::randomUser() const {
//    // WARNING: Not a secure PRNG, but okay for this.
//    return registerUser(std::to_string(rand()) + "@rand.com");
//}

AddrSK RegAuthSK::registerRandomUser(const std::string& pid) const {
  AddrSK ask;

  ask.pid = pid;
  ask.pid_hash = AddrSK::pidHash(pid);

  // pick random PRF key
  ask.s = Fr::random_element();

  // Boneh-Franklin IBE
  ask.e = msk.deriveEncSK(p, ask.pid);

  // NOTE: Randomness zero, since the user will always add it in when transacting
  std::vector<Fr> m = {ask.getPidHash(), ask.s, Fr::zero()};
  // need commitment to be in both G1 and G2 for signature verification to work
  ask.rcm = Comm::create(ck_reg, m, true);

  // Sign the registration commitment
  ask.rs = sk.sign(ask.rcm, Fr::random_element());

#ifndef NDEBUG
  auto vk = sk.toPK();
  assertTrue(ask.rs.verify(ask.rcm, vk));
  // logdbg << pid << " w/ RCM: " << ask.rcm << endl;
  // logdbg << pid << " w/ regsig: " << ask.rs << endl;
  // logdbg << "RPK::vk " << vk << endl;
#endif

  return ask;
}

}  // end of namespace libutt
