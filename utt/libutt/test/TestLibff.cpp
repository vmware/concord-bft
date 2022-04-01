#include <utt/Configuration.h>

#include <utt/PolyCrypto.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>

using namespace libutt;

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  libutt::initialize(nullptr, 0);

  // libff naming weirdness that I'm trying to understand:
  // zero() is the group's identity (since ECs use additive notation)
  // one() is the group's generator

  std::clog << "G1 zero: " << G1::zero() << std::endl;
  std::clog << "G1 one: " << G1::one() << std::endl;
  testAssertNotEqual(G1::one(), G1::zero());

  std::clog << "G2 zero: " << G2::zero() << std::endl;
  std::clog << "G2 one: " << G2::one() << std::endl;
  testAssertNotEqual(G2::one(), G2::zero());

  // a, b <-$- random
  Fr a = Fr::random_element(), b = Fr::random_element();
  // compute (ab)
  Fr ab = a * b;
  // g1^a
  G1 g1a = a * G1::one();
  // g2^a
  G2 g2b = b * G2::one();
  // gt^(ab)

  // test some simple arithmetic
  testAssertEqual(b * g1a, (ab)*G1::one());
  testAssertEqual(a * g2b, (ab)*G2::one());

  // Get generator in GT
  GT gt = ReducedPairing(G1::one(), G2::one());

  // test pairing
  GT gt_ab = gt ^ (ab);

  testAssertEqual(gt_ab, ReducedPairing(g1a, g2b));

  testAssertEqual(gt_ab, ReducedPairing(g1a, G2::one()) ^ b);

  testAssertEqual(gt_ab, ReducedPairing(G1::one(), g2b) ^ a);

  std::cout << "Test '" << argv[0] << "' finished successfully" << std::endl;

  return 0;
}
