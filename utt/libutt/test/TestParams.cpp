#include <utt/Configuration.h>

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

  Params p = Params::random();
  Params otherp = Params::random();
  Params samep = p;

  testAssertNotEqual(p, otherp);
  testAssertEqual(p, samep);

  // Test (de)serialization
  stringstream ss;

  ss << p;
  Params diffp(ss);
  testAssertEqual(p, diffp);

  ss << p;
  otherp = Params(ss);
  testAssertEqual(p, otherp);

  loginfo << "All is well." << endl;

  return 0;
}
