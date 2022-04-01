#include <utt/Configuration.h>

#include <utt/IBE.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/Utils.h>

using namespace std;
using namespace libutt;

int main(int argc, char *argv[]) {
  libutt::initialize(nullptr, 0);
  // srand(static_cast<unsigned int>(time(NULL)));
  (void)argc;
  (void)argv;

  IBE::Params p = IBE::Params::random();
  IBE::MSK msk = IBE::MSK::random();
  IBE::MPK mpk = msk.toMPK(p);

  std::string pid = "testuser@testdomain.com";
  IBE::EncSK encsk = msk.deriveEncSK(p, pid);

  // test EncSK serializes well
  std::stringstream ss;
  ss << encsk;
  IBE::EncSK sameEncSK;
  ss >> sameEncSK;
  testAssertEqual(encsk, sameEncSK);

  Fr v = Fr::random_element();
  Fr r_1 = Fr::random_element();
  Fr r_2 = Fr::random_element();
  logdbg << "v init: " << v << endl;
  logdbg << "r_1 init: " << r_1 << endl;
  logdbg << "r_2 init: " << r_2 << endl;

  IBE::Ctxt ctxt = mpk.encrypt(pid, frsToBytes({v, r_1, r_2})), sameCtxt;
  ss << ctxt;
  ss >> sameCtxt;
  testAssertEqual(ctxt, sameCtxt);

  Fr samev = Fr::random_element(), samer_1 = Fr::random_element(), samer_2 = Fr::random_element();

  bool success;
  AutoBuf<unsigned char> ptxt;
  std::tie(success, ptxt) = encsk.decrypt(ctxt);

  testAssertTrue(success);

  auto vec = bytesToFrs(ptxt);
  samev = vec[0];
  samer_1 = vec[1];
  samer_2 = vec[2];
  logdbg << "v decr: " << samev << endl;
  logdbg << "r_1 decr: " << samer_1 << endl;
  logdbg << "r_2 decr: " << samer_2 << endl;

  testAssertEqual(r_2, samer_2);
  testAssertEqual(r_1, samer_1);
  testAssertEqual(v, samev);

  // Test (in)equality
  auto ctxtCopy = ctxt;
  testAssertEqual(ctxt, ctxtCopy);

  testAssertNotEqual(ctxt,
                     mpk.encrypt(pid, frsToBytes({Fr::random_element(), Fr::random_element(), Fr::random_element()})));

  loginfo << "All is well." << endl;

  return 0;
}
