#include <utt/Configuration.h>

#include <utt/Address.h>
#include <utt/Coin.h>
#include <utt/Nullifier.h>
#include <utt/Params.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>

using namespace std;
using namespace libutt;

int main(int argc, char *argv[]) {
  libutt::initialize(nullptr, 0);
  // srand(static_cast<unsigned int>(time(NULL)));
  (void)argc;
  (void)argv;

  Nullifier::Params p = Nullifier::Params::random();

  // TODO: Hacky, for now, because I know that Nullifer only accesses AddrSK::s. Otherwise, would need to use
  // RegAuth::registerRandomUser().
  AddrSK ask;
  ask.s = Fr::random_element();

  Coin c1 = Coin::random(1000, Coin::NormalType());
  Coin c2 = Coin::random(1000, Coin::NormalType());

  Nullifier null0;
  Fr t1 = Fr::random_element();
  Fr t2 = Fr::random_element();
  Nullifier null1(p, ask, c1.sn, t1), null2(p, ask, c1.sn, t2);

  // empty object is different than random nullifier
  testAssertNotEqual(null0, null1);

  // computing the same nullifier twice works, but they use different randomness
  // so they are NOT the same
  testAssertNotEqual(null1, null2);

  // same secret, but different SNs => different nullifiers
  Coin c3 = c1;
  c3.sn = Fr::random_element();
  null2 = Nullifier(p, ask, c3.sn, t1);
  testAssertNotEqual(null1, null2);

  // different secret, same SNs => different nullifiers
  c3 = c1;
  ask.s = Fr::random_element();
  null2 = Nullifier(p, ask, c3.sn, t1);
  testAssertNotEqual(null1, null2);

  // Test (de)serialization
  stringstream ss;

  // ...via empty object
  ss << null1;
  Nullifier samenull;
  testAssertNotEqual(null1, samenull);

  ss >> samenull;
  testAssertEqual(null1, samenull);

  // ...via constructor
  ss << null1;
  Nullifier null3(ss);
  testAssertEqual(null1, null3);

  loginfo << "All is well." << endl;

  return 0;
}
