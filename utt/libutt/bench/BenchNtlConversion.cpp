#include <iostream>
#include <ctime>
#include <vector>
#include <sstream>

#include <xutils/Log.h>
#include <xutils/Timer.h>

#include <utt/PolyCrypto.h>
#include <utt/NtlLib.h>
#include <utt/Utils.h>

using namespace NTL;
using namespace std;
using namespace libutt;

int main() {
  initialize(nullptr, 0);

  // degree bound
  size_t sz = 1024 * 512;

  std::vector<Fr> fr1, fr2;
  ZZ_pX zp, zp2;

  loginfo << "Picking random ZZ_pX... ";
  NTL::random(zp, static_cast<long>(sz));
  cout << "done." << endl;

  ManualTimer t1;

  t1.restart();
  convNtlToLibff_slow(zp, fr1);
  auto delta = t1.stop().count();
  logperf << sz << " NTL to libff conversions (slow; w/ stringstream): " << delta / 1000 << " millisecs" << endl;

  t1.restart();
  convNtlToLibff(zp, fr2);
  delta = t1.stop().count();
  logperf << sz << " NTL to libff conversions (fast; w/ byte array): " << delta / 1000 << " millisecs" << endl;

  t1.restart();
  convLibffToNtl_slow(fr2, zp2);
  delta = t1.stop().count();
  logperf << sz << " libff to NTL conversions: " << delta / 1000 << " millisecs" << endl;

  if (fr1 != fr2) {
    logerror << "Our two implementation for converting from NTL to libff disagree" << endl;
    return 1;
  }

  if (zp != zp2) {
    logerror << "Converting from libff to NTL (slow; with byte array) failed" << endl;
    return 1;
  }

  // loginfo << "libff recovered polys: " << endl;
  // poly_print(std::cout, fr);
  // poly_print(std::cout, fr2);
  // poly_print(std::cout, fr3);

  loginfo << "All done!" << endl;

  return 0;
}
