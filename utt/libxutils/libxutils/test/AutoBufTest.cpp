/*
 * OtherTests.cpp
 *
 *  Created on: Thursday, September 21st, 2017
 *      Author: alinush
 */
#include <xutils/Log.h>
#include <xutils/AutoBuf.h>
#include <xutils/Utils.h>
#include <xassert/XAssert.h>

using namespace std;

void testUtils();
void testRandomSubsets();

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  unsigned int seed = static_cast<unsigned int>(time(NULL));
  loginfo << "Randomness seed passed to srand(): " << seed << endl;
  srand(seed);

  AutoBuf<unsigned char> buf(128);
  buf.zeroize();

  for (int i = 0; i < 128; i++) {
    testAssertEqual(buf[i], 0);
  }

  AutoBuf<unsigned char> bufint(128l);

  loginfo << "Test passed!" << endl;

  AutoBuf<unsigned char> moved = std::move(bufint);
  moved = std::move(bufint);

  return 0;
}
