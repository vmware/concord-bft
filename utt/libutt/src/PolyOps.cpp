#include <utt/Configuration.h>

#include <utt/PolyOps.h>
#include <utt/NtlLib.h>

#include <xassert/XAssert.h>

namespace libutt {

void __compute_numerators(std::vector<Fr>& N0,
                          const std::vector<Fr>& allOmegas,
                          const std::vector<size_t>& T,
                          const Fr& num0) {
  size_t N = allOmegas.size();
  assertTrue(Utils::isPowerOfTwo(N));

  N0.resize(T.size());
  size_t j = 0;
  for (auto i : T) {
    /**
     * Recall that:
     *  a) Inverses can be computed fast as: (w_N^k)^{-1} = w_N^{-k} = w_N^N w_N^{-k} = w_N^{N-k}
     *  b) Negations can be computed fast as: -w_N^k = w_N^{k + N/2}
     *
     * So, (0 - w_N^i)^{-1} = (w_N^{i + N/2})^{-1} = w_N^{N - (i + N/2)} = w_N^{N/2 - i}
     * If N/2 < i, then you wrap around to N + N/2 - i.
     */
    size_t idx;

    if (N / 2 < i) {
      idx = N + N / 2 - i;
    } else {
      idx = N / 2 - i;
    }

    N0[j++] = num0 * allOmegas[idx];
  }

  assertEqual(j, N0.size());
}

std::vector<Fr> __lagrange_coefficients_naive(const std::vector<Fr>& allOmegas,
                                              const std::vector<Fr>& someOmegas,
                                              const std::vector<size_t>& T) {
  std::vector<Fr> L;
  size_t N = allOmegas.size();
  assertTrue(Utils::isPowerOfTwo(N));
  assertEqual(someOmegas.size(), T.size());
  assertStrictlyLessThan(
      someOmegas.size(),
      N);  // assuming N-1 out of N is "max" threshold, which we don't have to assume (but it would be less efficient to
           // do Lagrange interpolation if you have the evaluations at all N Nth roots of unity)

  // WARNING: We want to make sure N*N fits in a size_t, so we can optimize the num0 code below to only compute % once
  testAssertStrictlyLessThan(N, 1u << 31);
  testAssertEqual(sizeof(size_t), 8);

  // Num(0) = \prod_{i\in T} (0 - w_N^i) = (-1)^|T| w_N^{(\sum_{i\in T} i) % N}
  Fr num0 = 1;
  std::vector<Fr> N0, D;

  L.resize(someOmegas.size());

  if (T.size() % 2 == 1) num0 = -Fr::one();

  size_t sum = 0;
  // NOTE: this sum is < 1 + 2 + ... + N < N*N. That's why we assert sizeof(N*N) < sizeof(size_t)
  for (auto i : T) {
    // sum = (sum + i) % N; // a bit slower, so should avoid
    sum += i;
  }
  sum %= N;
  assertStrictlyLessThan(sum, allOmegas.size());
  num0 *= allOmegas[sum];

  // Naively, we'd be doing a lot more field operations:
  // for(auto i : T) {
  //    size_t idx;
  //    if(i + N/2 >= N) {
  //        // idx = (i + N/2) - N
  //        idx = i - N/2;
  //    } else {
  //        idx = i + N/2;
  //    }
  //    num0 *= allOmegas[idx];
  //}

  // N0[i] = N_i(0) = Num(0) / (0 - x_i)
  __compute_numerators(N0, allOmegas, T, num0);

  // D[i] = product (x_i - x_j), j != i
  D.resize(someOmegas.size(), 1);
  for (size_t i = 0; i < someOmegas.size(); i++) {
    for (size_t j = 0; j < someOmegas.size(); j++) {
      if (j != i) {
        D[i] *= (someOmegas[i] - someOmegas[j]);
      }
    }
  }

  // L[i] = L_i(0) = N_i(0) / D_i
  for (size_t i = 0; i < L.size(); i++) {
    L[i] = N0[i] * D[i].inverse();
  }

  return L;
}

std::vector<Fr> lagrange_coefficients_naive(size_t n, const std::vector<size_t>& T) {
  size_t N = Utils::smallestPowerOfTwoAbove(n);
  std::vector<Fr> allOmegas = get_all_roots_of_unity(N);

  return lagrange_coefficients_naive(allOmegas, T);
}

std::vector<Fr> lagrange_coefficients_naive(const std::vector<Fr>& allOmegas, const std::vector<size_t>& T) {
  std::vector<Fr> someOmegas;
  for (size_t i : T) {
    assertStrictlyLessThan(i, allOmegas.size());
    someOmegas.push_back(allOmegas[i]);
  }

  return __lagrange_coefficients_naive(allOmegas, someOmegas, T);
}

ZZ_pX poly_from_roots_ntl(const vec_ZZ_p& roots, long startIncl, long endExcl) {
  // base case is startIncl == endExcl - 1, so return (x - root)
  if (startIncl == endExcl - 1) {
    ZZ_pX monom(NTL::INIT_MONO, 1);
    monom[0] = -roots[startIncl];
    monom[1] = 1;
    assertEqual(NTL::deg(monom), 1);
    return monom;
  }

  ZZ_pX p;

  long middle = (startIncl + endExcl) / 2;

  NTL::mul(p, poly_from_roots_ntl(roots, startIncl, middle), poly_from_roots_ntl(roots, middle, endExcl));

  return p;
}

}  // namespace libutt
