#include <utt/Configuration.h>

#include <utt/Comm.h>
#include <utt/PolyCrypto.h>
#include <utt/RandSig.h>
#include <utt/RandSigDKG.h>

#include <cmath>
#include <ctime>
#include <iostream>
#include <fstream>
#include <random>
#include <stdexcept>
#include <vector>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>
#include <xutils/Timer.h>
#include <xutils/Utils.h>

using namespace std;
using namespace libutt;

size_t ell = 5;  // number of committed messages

void testCentralized() {
  CommKey ck = CommKey::random(ell);
  RandSigSK sk = RandSigSK::random(ck);
  RandSigPK pk_tmp = sk.toPK(), pk;

  // test (de)serialization
  std::stringstream ss;

  // NOTE: For now, we never need to serialize RandSigSK's (e.g., RegAuth is just PKs, because coins are pre-issued in
  // our benchmarks; also, bank is distributed, so serializing RandSigShareSK's instead.)
  // ss << sk_tmp;
  // ss >> sk;
  // testAssertEqual(sk, sk_tmp);

  ss << pk_tmp;
  ss >> pk;
  testAssertEqual(pk, pk_tmp);

  auto m = random_field_elems(ell + 1);  // last message is the randomness
  Comm cm = Comm::create(ck, m, true);
  logdbg << "cm: " << endl << cm << endl;

  auto u = Fr::random_element();
  logdbg << "u: " << u << endl;
  auto sig = sk.sign(cm, u);
  logdbg << "sig: " << endl << sig << endl;
  testAssertTrue(sig.verify(cm, pk));

  auto delta_r = Fr::random_element();
  auto delta_u = Fr::random_element();

  auto oldcm = cm;
  cm.rerandomize(ck, delta_r);
  logdbg << "cm': " << endl << cm << endl;
  testAssertNotEqual(cm, oldcm);
  testAssertFalse(sig.verify(cm, pk));  // signature should no longer verify

  auto oldsig = sig;
  sig.rerandomize(delta_r, delta_u);
  logdbg << "sig': " << endl << sig << endl;
  testAssertNotEqual(sig, oldsig);

  testAssertTrue(sig.verify(cm, pk));  // signature should verify again
}

int main(int argc, char *argv[]) {
  libutt::initialize(nullptr, 0);
  // srand(static_cast<unsigned int>(time(NULL)));
  (void)argc;
  (void)argv;

  testCentralized();

  loginfo << "All is well." << endl;

  return 0;
}
