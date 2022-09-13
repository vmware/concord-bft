#include <utt/Configuration.h>

#include <utt/PolyCrypto.h>

#include <iostream>
#include <ctime>

#include <xutils/Log.h>
#include <xutils/Utils.h>
#include <xutils/Timer.h>
#include <xassert/XAssert.h>

using namespace std;
using namespace libutt;

void testMultiPairing(size_t numPairings);

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  libutt::initialize(nullptr, 0);

  testMultiPairing(1);
  testMultiPairing(2);
  testMultiPairing(3);
  testMultiPairing(4);
  testMultiPairing(10);
  testMultiPairing(11);

  loginfo << "All tests succeeded!" << endl;

  return 0;
}

void testMultiPairing(size_t numPairings) {
  std::vector<G1> a(numPairings);
  std::vector<G2> b(numPairings);

  for (size_t i = 0; i < numPairings; i++) {
    a[i] = G1::random_element();
    b[i] = G2::random_element();
  }

  loginfo << "Picked " << numPairings << " random group elements in G1" << endl;
  loginfo << "Picked " << numPairings << " random group elements in G2" << endl;
  loginfo << "Testing a size-" << numPairings << " (+1) multipairing" << endl;

  GT r1 = GT::one();
  for (size_t i = 0; i < numPairings; i++) {
    r1 = r1 * ReducedPairing(a[i], b[i]);
  }
  GT r2 = MultiPairingNaive(a, b);

  testAssertNotEqual(r1, GT::one());
  testAssertEqual(r1, r2);

  GT r3 = MultiPairing(a, b);
  testAssertEqual(r1, r3);
}
