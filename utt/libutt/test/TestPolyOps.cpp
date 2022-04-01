#include <utt/Configuration.h>

#include <utt/PolyCrypto.h>
#include <utt/PolyOps.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>

using namespace libutt;

std::vector<Fr> poly_from_roots_naive(const std::vector<Fr>& roots) {
  std::vector<Fr> monom(2), acc(1, Fr::one());
  for (size_t i = 0; i < roots.size(); i++) {
    Fr r = roots[i];
    // logdbg << "Multiplying in root " << r << endl;

    monom[0] = -r;
    monom[1] = 1;
    libfqfft::_polynomial_multiplication_on_fft(acc, acc, monom);

    testAssertEqual(acc.size(), i + 2);

    // poly_print(acc);
  }

  return acc;
}

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  libutt::initialize(nullptr, 0);

  const size_t maxSize = 64;
  std::vector<Fr> roots;
  for (size_t i = 0; i < maxSize; i++) {
    roots.push_back(Fr::random_element());
    loginfo << "Testing interpolation from " << i + 1 << " roots..." << endl;

    auto expected = poly_from_roots_naive(roots);
    auto computed = poly_from_roots(roots);

    testAssertEqual(computed.size(), roots.size() + 1);
    testAssertEqual(expected, computed);
  }

  size_t N = 4;
  std::vector<Fr> poly;

  // test 1: evaluate random poly at roots of unity, then re-interpolate
  poly = random_field_elems(N);
  std::vector<Fr> evals = poly_fft(poly, N);
  std::vector<Fr> samePoly = poly_inverse_fft(evals, N);

  testAssertEqual(poly, samePoly);

  // test 2: interpolate polynomial from roots of unity, then re-evaluate
  evals = random_field_elems(N);
  poly = poly_inverse_fft(evals, N);
  std::vector<Fr> sameEvals = poly_fft(poly, N);

  testAssertEqual(evals, sameEvals);

  N = 8;
  std::vector<Fr> a = random_field_elems(N);
  std::vector<Fr> b = random_field_elems(N);
  std::vector<Fr> c;

  libfqfft::_polynomial_multiplication(c, a, b);
  a = poly_mult_naive(a, b);

  testAssertEqual(a, c);

  return 0;
}
