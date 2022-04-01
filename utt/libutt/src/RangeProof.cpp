#include <utt/Configuration.h>

#include <optional>

#include <utt/internal/plusaes.h>

#include <utt/Comm.h>
#include <utt/Kzg.h>
#include <utt/PolyCrypto.h>
#include <utt/PolyOps.h>
#include <utt/RangeProof.h>

#include <utt/Serialization.h>  // WARNING: must include last

#include <xutils/Timer.h>
#include <xutils/NotImplementedException.h>

std::ostream& operator<<(std::ostream& out, const libutt::RangeProof::Params& pp) {
  out << pp.ck_val;
  out << pp.kpp;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::RangeProof::Params& pp) {
  in >> pp.ck_val;
  in >> pp.kpp;
  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::KzgPedEqProof& pi) {
  out << pi.c_p << endl;
  out << pi.w_p << endl;
  out << pi.cm_p << endl;
  out << pi.Y << endl;
  out << pi.e << endl;
  out << pi.alpha << endl;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::KzgPedEqProof& pi) {
  in >> pi.c_p;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pi.w_p;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pi.cm_p;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pi.Y;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pi.e;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pi.alpha;
  libff::consume_OUTPUT_NEWLINE(in);

  return in;
}

namespace libutt {

std::vector<Fr> RangeProof::Params::omegas;
Fr RangeProof::Params::two;
std::vector<Fr> RangeProof::Params::XNminus1;
std::vector<Fr> RangeProof::Params::XminusLastOmega;

RangeProof::Params RangeProof::Params::random(const CommKey& ck_val) {
  RangeProof::Params p;

  size_t maxDeg = 3 * RangeProof::Params::MAX_BITS + 1;
  p.kpp = KZG::Params::random(maxDeg);
  p.ck_val = ck_val;

  return p;
}

void RangeProof::Params::initializeOmegas() {
  size_t N = RangeProof::Params::MAX_BITS;

  RangeProof::Params::omegas = get_all_roots_of_unity(N);
  RangeProof::Params::two = Fr(2);
  RangeProof::Params::XNminus1 = RangeProof::getAccumulator(N);
  RangeProof::Params::XminusLastOmega = RangeProof::getMonomial(RangeProof::Params::omegas[N - 1]);
}

Fr KzgPedEqProof::hash(const KZG::Params& kpp,
                       const CommKey& ck_val,
                       const G1& kzgComm,
                       const Comm& pedComm,
                       const std::vector<G1>& X,
                       const GT& Z) const {
  stringstream ss;

  ss << kpp.g1() << endl;
  ss << kpp.g2() << endl;
  ss << kpp.eg1g2 << endl;
  ss << ck_val << endl;
  ss << kzgComm << endl;
  ss << pedComm << endl;

  serializeVector(ss, X);
  ss << Z << endl;

  return hashToField(ss.str());
}

/**
 * Returns the polynomial \gamma such that \gamma(1) = val64 and \gamma(\omega^i) = 2*\gamma(\omega^{i+1}) + v_i, where
 * v_i is the ith bit of 'val64'
 */
std::vector<Fr> __computeGamma(size_t val64, size_t N, const Fr& val) {
  size_t numBits = sizeof(val64) * 8;
  assertEqual(numBits, N);

  std::vector<Fr> v;  // bits of val64
  for (size_t i = 0; i < numBits; i++) {
    bool bit = (val64 % 2 == 1);
    val64 >>= 1;
    v.push_back(bit ? Fr::one() : Fr::zero());
  }
  assertEqual(val64, 0);
  assertEqual(v.size(), numBits);

  const Fr omega_p = Fr::random_element();   // random eval point \omega'
  const Fr omega_pp = Fr::random_element();  // random eval point \omega''
  const Fr gamma_r1 = Fr::random_element();  // \gamma(\omega')
  const Fr gamma_r2 = Fr::random_element();  // \gamma(\omega'')

  /**
   * Step 1: Interpolate a \gamma(X) such that:
   *
   *  1. \gamma(\omega^{N-1}) = v_{N-1}
   *  2. \gamma(\omega^i)     = 2\gamma(\omega^{i+1}) + v_i
   *  3. \gamma(\omega')      = random
   *  3. \gamma(\omega'')     = random
   *
   * We could implement this in O(N\log^2 N) time using Fast Lagrange interpolation.
   * But that'd be slower and more complex than the following simpler & faster approach:
   *
   * Let \gamma(X) = (X - \omega')(X - \omega'') g_1(X) + (X^N - 1) g_2(X)
   * Note that:
   *
   *  1. \gamma(\omega^i) = (\omega^i - \omega')(\omega^i - \omega'') g_1(\omega^i) + 0
   *  2. \gamma(\omega')  = 0 + [ (\omega' )^N - 1 ] g_2(\omega' )
   *  3. \gamma(\omega'') = 0 + [ (\omega'')^N - 1 ] g_2(\omega'')
   *
   * Thus, using one inverse FFT, we can interpolate g_1 such that:
   *
   *  \forall i\in[0,N),
   *    g_1(\omega^i) = \gamma(\omega^i) / [ (\omega^i - \omega')(\omega^i - \omega'') ]
   *
   * (We compute these in g1_evals[] below.)
   *
   * Then, using the Lagrange formula, we can interpolate g_2 such that:
   *
   *  1. g_2(\omega ') = \gamma(\omega' ) / [ (\omega' )^N - 1 ] = g2_eval[0]
   *  2. g_2(\omega'') = \gamma(\omega'') / [ (\omega'')^N - 1 ] = g2_eval[1]
   *
   * Specifically,
   *
   *  g_2(X) = (X - \omega' ) / (\omega'' - \omega') g2_eval[1] +
   *         + (X - \omega'') / (\omega' - \omega'') g2_eval[0]
   */
  std::vector<Fr> gamma_evals(N);  // \gamma(\omega^i)

  // need to fill in 'gamma_evals' in reverse order
  gamma_evals[N - 1] = v[N - 1];
  for (size_t i = 1; i < N; i++) {
    size_t j = (N - 1) - i;
    gamma_evals[j] = RangeProof::Params::two * gamma_evals[j + 1] + v[j];
  }

  // once gamma_evals are fixed, we can compute g1_evals
  std::vector<Fr> g1_evals(N);
  for (size_t i = 0; i < N; i++) {
    // i.e., (\omega^i - \omega') (\omega^i - \omega'')
    auto denom = (RangeProof::Params::omegas[i] - omega_p) * (RangeProof::Params::omegas[i] - omega_pp);

    // i.e., \gamma(\omega^i) / [ (\omega^i - \omega') (\omega^i - \omega'') ]
    g1_evals[i] = gamma_evals[i] * denom.inverse();  // i.e., divides by denom
  }

  // then, we can compute g_1(X) via an inverse FFT
  std::vector<Fr> g1 = poly_inverse_fft(g1_evals, N);

  // lastly, we can compute g_2(X) directly
  const Fr omega_p_N = (omega_p ^ N) - Fr::one();    // \omega' ^ N - 1
  const Fr omega_pp_N = (omega_pp ^ N) - Fr::one();  // \omega''^ N - 1
  std::vector<Fr> g2_evals({gamma_r1 * omega_p_N.inverse(), gamma_r2 * omega_pp_N.inverse()});

  const Fr diff = omega_pp - omega_p;  // (\omega'' - \omega')
  Fr diff_inv = diff.inverse();        // 1 / (\omega'' - \omega')
  /**
   * Compute:
   *
   *     (X - \omega' ) / (\omega'' - \omega') g2_eval[1] = [
   *         (X g2_eval[1]) / (\omega'' - \omega') ] -
   *       - [ (\omega' g2_eval[1]) / (\omega'' - \omega')
   *     ]
   */
  const std::vector<Fr> g2_left = {-(omega_p * g2_evals[1] * diff_inv), g2_evals[1] * diff_inv};

  diff_inv = -diff_inv;  // 1 / (\omega' - \omega'')
  /**
   *   (X - \omega'' ) / (\omega' - \omega'') g2_eval[0] =
   * = [ (X g2_eval[0]) / (\omega' - \omega'') ] - [ \omega'' / (\omega' - \omega'') g2_eval[0] ]
   */
  const std::vector<Fr> g2_right = {-(omega_pp * diff_inv * g2_evals[0]), g2_evals[0] * diff_inv};

  std::vector<Fr> g2 = poly_add(g2_left, g2_right);

  // (X - \omega')(X - \omega'')
  std::vector<Fr> g1_sel = poly_mult_naive<Fr>(RangeProof::getMonomial(omega_p), RangeProof::getMonomial(omega_pp));
  // X^N - 1
  std::vector<Fr> g2_sel = RangeProof::Params::XNminus1;

  const std::vector<Fr> gamma = poly_add(poly_mult_naive(g1_sel, g1), poly_mult_naive(g2_sel, g2));

#ifndef NDEBUG
  // logdbg << "Checking \\gamma(X) was computed correctly" << endl;
  Fr sameVal = poly_eval(gamma, RangeProof::Params::omegas[0]);
  assertEqual(sameVal, val);

  // logdbg << "val: " << val << endl;
  for (size_t i = 0; i < N - 1; i++) {
    // \gamma(\omega^i)
    auto goi = poly_eval(gamma, RangeProof::Params::omegas[i]);
    // \gamma(\omega^{i+1})
    auto goi1 = poly_eval(gamma, RangeProof::Params::omegas[i + 1]);

    // z_i = \gamma(\omega^i) - 2\gamma(\omega^{i+1})
    assertEqual(v[i], goi - RangeProof::Params::two * goi1);
    // logdbg << "v[" << i << "]: " << v[i] << endl;
  }
  // logdbg << "v[" << N-1 << "]: " << v[N-1] << endl;
  assertEqual(v[N - 1], poly_eval(gamma, RangeProof::Params::omegas[N - 1]));

  Fr sameRand1 = poly_eval(gamma, omega_p);
  assertEqual(sameRand1, gamma_r1);

  Fr sameRand2 = poly_eval(gamma, omega_pp);
  assertEqual(sameRand2, gamma_r2);
  logtrace << "\\gamma(X) was computed correctly!" << endl;
#else
  (void)val;
#endif
  return gamma;
}

void __assertZeroOnOmegas(const std::vector<Fr>& f) {
#ifndef NDEBUG
  // WARNING: Very slow! Can't do FFTs of size N when deg f >= N
  for (size_t i = 0; i < RangeProof::Params::omegas.size(); i++) {
    auto& omega = RangeProof::Params::omegas.at(i);
    // logdbg << "\\omega[" << i << "]: " << omega << endl;
    assertEqual(poly_eval(f, omega), Fr::zero());
  }
#else
  (void)f;
#endif
}

std::vector<Fr> __computeW2(const std::vector<Fr>& gamma) {
  // \gamma(X) ( 1 - \gamma(X) )
  std::vector<Fr> w2_interm = poly_mult(gamma, poly_subtract(poly_constant(Fr::one()), gamma));

  assertTrue(libfqfft::_is_zero(poly_div_naive_rem(RangeProof::Params::XNminus1, RangeProof::Params::XminusLastOmega)));

  // \gamma(X) ( 1 - \gamma(X) ) (X^N - 1) / (X - \omega^{N-1})
  return poly_mult(w2_interm, poly_div_naive_quo(RangeProof::Params::XNminus1, RangeProof::Params::XminusLastOmega));
}

std::vector<Fr> __computeW3(const std::vector<Fr>& gamma) {
  // \gamma(X) - 2*\gamma(X\omega)
  std::vector<Fr> w3_left =
      poly_subtract(gamma, poly_scale(RangeProof::Params::two, poly_Xomega(gamma, RangeProof::Params::omegas[1])));

  // ( \gamma(X) - 2*\gamma(X\omega) ) [ 1 - ( \gamma(X) - 2*\gamma(X\omega) ) ]
  std::vector<Fr> w3_middle = poly_mult(w3_left, poly_subtract(poly_constant(Fr::one()), w3_left));
  assertEqual(poly_eval(w3_middle, RangeProof::Params::omegas[0]), Fr::zero());

  // ( \gamma(X) - 2*\gamma(X\omega) ) [ 1 - ( \gamma(X) - 2*\gamma(X\omega) ) ] (X - \omega^{N-1})
  return poly_mult_naive(w3_middle, RangeProof::Params::XminusLastOmega);
}

KzgPedEqProof __computeKzgPedAgreementProof(const RangeProof::Params& p,
                                            const Comm& vcm,
                                            const Fr& val,
                                            const Fr& z,
                                            const G1& gammaKzg,
                                            const std::vector<Fr>& gamma) {
  const KZG::Params& kpp = p.kpp;

  // compute KZG witness for \gamma(\omega^0)
  G1 kzgWit;
  Fr sameVal;
  std::tie(kzgWit, sameVal) = KZG::naiveProve(kpp, gamma, RangeProof::Params::omegas[0]);
  assertEqual(sameVal, val);
  assertTrue(kpp.verifyProof(gammaKzg, kzgWit, val, Fr::one()));

  // prove that the committed \gamma(1) is the same as the value committed in vcm
  return KzgPedEqProof(kpp, p.ck_val, gammaKzg, val, kzgWit, vcm, z);
}

RangeProof::RangeProof(const Params& p, const Comm& vcm, const Fr& val, const Fr& z) {
  size_t val64 = val.as_ulong();
  static_assert(sizeof(val64) == Params::MAX_BITS / 8, "Expected Fr::as_ulong() to return 64-bit value");

  size_t N = Params::MAX_BITS;

  // gamma has degree N + 1
  const auto gamma = __computeGamma(val64, N, val);
  assertEqual(gamma.size() - 1, N + 1);

  auto w2 = __computeW2(gamma);
  assertEqual(w2.size() - 1, 3 * N + 1);
  __assertZeroOnOmegas(w2);
  auto w3 = __computeW3(gamma);
  assertEqual(w3.size() - 1, 2 * N + 3);
  __assertZeroOnOmegas(w3);

  // Fiat-Shamir randomness \tau and \rho
  Fr tau, rho;
  std::tie(tau, rho) = deriveTauAndRho(p, vcm);

  // w = w_2 + \tau w_3
  std::vector<Fr> w = poly_add(w2, poly_scale(tau, w3));

  // commit to \gamma and prove \gamma(1) agrees with val committed in vcm
  kzgGamma = KZG::commit<G1>(p.kpp, gamma);
  kzgPed_pi = __computeKzgPedAgreementProof(p, vcm, val, z, kzgGamma, gamma);

  // compute quotient q(X) that proves that w(X) is zero at all roots of unity
  std::vector<Fr> q, rem;
  {
    // logdbg << "Dividing degree " << w.size() << " poly by X^" << N << " - 1 took ";
    // ScopedTimer t(std::cout, "poly_div: ");

    // WARNING: This takes 430 microseconds; much, much slower
    // std::tie(q, rem) = poly_div_naive(w, RangeProof::Params::XNminus1);

    // NOTE: This takes 8 microseconds; much, much faster (thank you libpolycrypto/AMTs)
    std::tie(q, rem) = poly_div_xnc(w, Params::XNminus1);
  }
  assertFalse(libfqfft::_is_zero(q));
  assertTrue(libfqfft::_is_zero(rem));

  // commit to quotient q
  kzgQ = KZG::commit<G1>(p.kpp, q);
  // proof for \gamma(\rho)
  std::tie(gammaOfRhoWit, gammaOfRho) = KZG::naiveProve(p.kpp, gamma, rho);
  // proof for \gamma(\rho\omega)
  std::tie(gammaOfRhoOmegaWit, gammaOfRhoOmega) = KZG::naiveProve(p.kpp, gamma, rho * Params::omegas[1]);
  // proof for q(\rho)
  std::tie(qOfRhoWit, qOfRho) = KZG::naiveProve(p.kpp, q, rho);
}

bool RangeProof::verify(const Params& p, const Comm& vcm) const {
  auto kpp = p.kpp;

  if (!kzgPed_pi.verify(kpp, p.ck_val, kzgGamma, Params::omegas[0], vcm)) {
    logerror << "KZG-Pedersen agreement proof did NOT verify" << endl;
    return false;
  }

  Fr tau, rho;
  std::tie(tau, rho) = deriveTauAndRho(p, vcm);

  if (!kpp.verifyProof(kzgGamma, gammaOfRhoWit, gammaOfRho, rho)) {
    logerror << "KZG proof for \\gamma(\\rho) did NOT verify" << endl;
    return false;
  }

  if (!kpp.verifyProof(kzgGamma, gammaOfRhoOmegaWit, gammaOfRhoOmega, rho * Params::omegas[1])) {
    logerror << "KZG proof for \\gamma(\\rho\\omega) did NOT verify" << endl;
    return false;
  }

  if (!kpp.verifyProof(kzgQ, qOfRhoWit, qOfRho, rho)) {
    logerror << "KZG proof for q(\\rho) did NOT verify" << endl;
    return false;
  }

  size_t N = Params::MAX_BITS;
  auto w2_rho =
      gammaOfRho * (Fr::one() - gammaOfRho) * (((rho ^ N) - Fr::one()) * (rho - Params::omegas[N - 1]).inverse());
  auto gm2g = (gammaOfRho - Params::two * gammaOfRhoOmega);
  auto w3_rho = gm2g * (Fr::one() - gm2g) * (rho - Params::omegas[N - 1]);

  return w2_rho + tau * w3_rho == qOfRho * ((rho ^ N) - Fr::one());
}

std::vector<Fr> RangeProof::correlatedRandomness(const std::vector<Fr>& z, size_t m) {
  size_t k = z.size();
  Fr insum = Fr::zero();
  for (size_t i = 0; i < k; i++) {
    insum = insum + z[i];
  }

  std::vector<Fr> r;
  Fr outsum = Fr::zero();
  for (size_t j = 0; j < m - 1; j++) {
    r.push_back(Fr::random_element());
    outsum = outsum + r.back();
  }

  // make insum == outsum
  r.push_back(insum - outsum);
  outsum = outsum + r.back();

#ifndef NDEBUG
  assertEqual(insum, outsum);
#endif
  return r;
}
}  // namespace libutt

std::ostream& operator<<(std::ostream& out, const libutt::RangeProof& rp) {
  out << rp.kzgGamma << endl;
  out << rp.kzgQ << endl;
  out << rp.kzgPed_pi;
  out << rp.gammaOfRho << endl;
  out << rp.gammaOfRhoWit << endl;
  out << rp.gammaOfRhoOmega << endl;
  out << rp.gammaOfRhoOmegaWit << endl;
  out << rp.qOfRho << endl;
  out << rp.qOfRhoWit << endl;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::RangeProof& rp) {
  in >> rp.kzgGamma;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> rp.kzgQ;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> rp.kzgPed_pi;

  in >> rp.gammaOfRho;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> rp.gammaOfRhoWit;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> rp.gammaOfRhoOmega;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> rp.gammaOfRhoOmegaWit;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> rp.qOfRho;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> rp.qOfRhoWit;
  libff::consume_OUTPUT_NEWLINE(in);

  return in;
}
