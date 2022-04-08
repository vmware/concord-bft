/*
 * OtherTests.cpp
 *
 *  Created on: Thursday, September 21st, 2017
 *      Author: alinush
 */

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cassert>
#include <memory>
#include <algorithm>
#include <stdexcept>

#include <xutils/Log.h>
#include <xutils/Timer.h>
#include <xutils/Utils.h>
#include <xutils/NotImplementedException.h>
#include <xassert/XAssert.h>

using namespace std;
using namespace libutt;

void testUtils();
void testRandomSubsets();

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  unsigned int seed = static_cast<unsigned int>(time(NULL));
  loginfo << "Randomness seed passed to srand(): " << seed << endl;
  srand(seed);

  testUtils();
  testRandomSubsets();

  testAssertEqual(Utils::smallestPowerOfTwoAbove(0), 1);
  testAssertEqual(Utils::smallestPowerOfTwoAbove(1), 1);
  testAssertEqual(Utils::smallestPowerOfTwoAbove(2), 2);
  testAssertEqual(Utils::smallestPowerOfTwoAbove(3), 4);
  testAssertEqual(Utils::smallestPowerOfTwoAbove(4), 4);
  testAssertEqual(Utils::smallestPowerOfTwoAbove(5), 8);
  testAssertEqual(Utils::smallestPowerOfTwoAbove(6), 8);

  testAssertEqual(Utils::greatestPowerOfTwoBelow(0), 0);
  testAssertEqual(Utils::greatestPowerOfTwoBelow(1), 1);
  testAssertEqual(Utils::greatestPowerOfTwoBelow(2), 2);
  testAssertEqual(Utils::greatestPowerOfTwoBelow(3), 2);
  testAssertEqual(Utils::greatestPowerOfTwoBelow(4), 4);
  testAssertEqual(Utils::greatestPowerOfTwoBelow(5), 4);
  testAssertEqual(Utils::greatestPowerOfTwoBelow(6), 4);

  testAssertEqual(Utils::log2floor(1), 0);
  testAssertEqual(Utils::log2floor(2), 1);
  testAssertEqual(Utils::log2floor(3), 1);
  testAssertEqual(Utils::log2floor(4), 2);
  testAssertEqual(Utils::log2floor(5), 2);
  testAssertEqual(Utils::log2floor(6), 2);
  testAssertEqual(Utils::log2floor(7), 2);
  testAssertEqual(Utils::log2floor(8), 3);
  testAssertEqual(Utils::log2floor(15), 3);
  testAssertEqual(Utils::log2floor(16), 4);
  testAssertEqual(Utils::log2floor(512), 9);
  testAssertEqual(Utils::log2floor(1024 * 1024), 20);

  testAssertEqual(Utils::log2ceil(1), 0);
  testAssertEqual(Utils::log2ceil(2), 1);
  testAssertEqual(Utils::log2ceil(3), 2);
  testAssertEqual(Utils::log2ceil(4), 2);
  testAssertEqual(Utils::log2ceil(5), 3);
  testAssertEqual(Utils::log2ceil(6), 3);
  testAssertEqual(Utils::log2ceil(7), 3);
  testAssertEqual(Utils::log2ceil(8), 3);
  testAssertEqual(Utils::log2ceil(15), 4);
  testAssertEqual(Utils::log2ceil(16), 4);
  testAssertEqual(Utils::log2ceil(511), 9);
  testAssertEqual(Utils::log2ceil(512), 9);
  testAssertEqual(Utils::log2ceil(513), 10);
  testAssertEqual(Utils::log2ceil(1024 * 1024), 20);

  try {
    std::string a("abc");
    throw libxutils::NotImplementedException(a);
    throw libxutils::NotImplementedException();
    throw libxutils::NotImplementedException("a");
  } catch (const libxutils::NotImplementedException& e) {
    loginfo << "Caught NotImplementedException: " << e.what() << endl;
  } catch (const std::exception& e) {
    assertFail("Did not expect another exception");
  }

  { ScopedTimer<> t(std::cout, "Default ", " microseconds\n"); }

  { ScopedTimer<std::chrono::seconds> t(std::cout, "Seconds ", " seconds\n"); }

  loginfo << "Exited gracefully with 0." << endl;
  return 0;
}

void testUtils() {
  testAssertEqual(Utils::numBits(1), 1);
  testAssertEqual(Utils::numBits(2), 2);
  testAssertEqual(Utils::numBits(3), 2);
  testAssertEqual(Utils::numBits(4), 3);
  testAssertEqual(Utils::numBits(5), 3);
  testAssertEqual(Utils::numBits(6), 3);
  testAssertEqual(Utils::numBits(7), 3);
  testAssertEqual(Utils::numBits(8), 4);

  loginfo << "Utils::numBits passed!" << endl;

  testAssertEqual(Utils::pow2(0), 1);
  testAssertEqual(Utils::pow2(1), 2);
  testAssertEqual(Utils::pow2(2), 4);
  testAssertEqual(Utils::pow2(3), 8);
  testAssertEqual(Utils::pow2(4), 16);
  testAssertEqual(Utils::pow2(5), 32);

  loginfo << "Utils::pow2 passed!" << endl;

  testAssertEqual(Utils::withCommas(3), "3");
  testAssertEqual(Utils::withCommas(30), "30");
  testAssertEqual(Utils::withCommas(100), "100");
  testAssertEqual(Utils::withCommas(1234), "1,234");
  testAssertEqual(Utils::withCommas(91234), "91,234");
  testAssertEqual(Utils::withCommas(191234), "191,234");
  testAssertEqual(Utils::withCommas(7191234), "7,191,234");

  loginfo << "Utils::withCommas passed!" << endl;

  testAssertEqual(Utils::isPowerOfTwo(0), false);
  testAssertEqual(Utils::isPowerOfTwo(1), true);
  testAssertEqual(Utils::isPowerOfTwo(2), true);
  testAssertEqual(Utils::isPowerOfTwo(3), false);
  testAssertEqual(Utils::isPowerOfTwo(4), true);
  testAssertEqual(Utils::isPowerOfTwo(5), false);
  testAssertEqual(Utils::isPowerOfTwo(6), false);
  testAssertEqual(Utils::isPowerOfTwo(7), false);
  testAssertEqual(Utils::isPowerOfTwo(8), true);

  loginfo << "Utils::isPowerOfTwo passed!" << endl;

  std::vector<std::chrono::microseconds::rep> musList = {
      60,                                                  // 60 mus
      61,                                                  // 61 mus
      1000,                                                // 1.0 ms
      1100,                                                // 1.1 ms
      60 * 1000,                                           // 60 ms
      61 * 1000,                                           // 61 ms
      1000 * 1000,                                         // 1.0 secs
      1100 * 1000,                                         // 1.1 secs
      60 * 1000 * 1000,                                    // 60 s
      61 * 1000 * 1000,                                    // 61 s
      1100 * 1000 * 1000ull,                               // 18.33 mins
      24 * 60 * 1000 * 1000ull,                            // 24 mins
      36 * 60 * 1000 * 1000ull,                            // 36 mins
      73 * 60 * 1000 * 1000ull,                            // 1.21 hrs
      23 * 60 * 60 * 1000 * 1000ull,                       // 23 hrs
      24 * 60 * 60 * 1000 * 1000ull,                       // 1 day
      27 * 60 * 60 * 1000 * 1000ull,                       // 1.125 days
      365ull * 24ull * 60ull * 60ull * 1000ull * 1000ull,  // 365 days
      366ull * 24ull * 60ull * 60ull * 1000ull * 1000ull,  // 1.002 years
  };

  loginfo << sizeof(unsigned long long) << " bytes for ull" << endl;
  loginfo << sizeof(std::chrono::microseconds) << " bytes for microseconds::rep" << endl;

  for (auto mus : musList) {
    loginfo << mus << " mus -> ";
    std::cout << Utils::humanizeMicroseconds(mus);
    std::cout << endl;
  }

  Utils::humanizeBytes(1024);
}

void testRandomSubsets() {
  std::vector<int> v;
  std::set<int> s;
  const int n = 4;
  const int k = 2;

  // repeat enough times to make sure all subsets are picked (in expectation)
  for (size_t i = 0; i < 100; i++) {
    Utils::randomSubset(v, n, k);
    Utils::randomSubset(s, n, k);
    testAssertEqual(v.size(), s.size());

    std::for_each(v.begin(), v.end(), [](int& el) {
      testAssertGreaterThanOrEqual(el, 0);
      testAssertStrictlyLessThan(el, n);
    });
  }

  std::copy(v.begin(), v.end(), std::ostream_iterator<int>(std::cout, " "));
  std::cout << endl;
}
