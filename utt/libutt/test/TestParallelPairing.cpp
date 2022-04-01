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

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  libutt::initialize(nullptr, 0);

  size_t numIters = 100;

#ifdef USE_MULTITHREADING
  loginfo << "Multithreading enabled!" << endl;
#else
  loginfo << "Multithreading disabled!" << endl;
#endif

  std::vector<G1> a(numIters);
  std::vector<G2> b(numIters);

#ifdef USE_MULTITHREADING
#pragma omp parallel for
#endif
  for (size_t i = 0; i < numIters; i++) {
    a[i] = G1::random_element();
    b[i] = G2::random_element();
  }

  loginfo << "Picked " << numIters << " random group elements in G1 and G2" << endl;

#ifdef USE_MULTITHREADING
#pragma omp parallel for
#endif
  for (size_t i = 0; i < numIters; i++) {
    // TODO: this seems to crash for some reason.
    // ReducedPairing(a[i], b[i]);
    libff::default_ec_pp::reduced_pairing(a[i], b[i]);
  }

  loginfo << "All tests succeeded!" << endl;

  return 0;
}
