#include <utt/Configuration.h>

#include <utt/PolyCrypto.h>
#include <utt/NtlLib.h>

#include <vector>
#include <cmath>
#include <iostream>
#include <ctime>
#include <fstream>

#include <libfqfft/polynomial_arithmetic/basic_operations.hpp>
#include <libff/common/double.hpp>

#include <xutils/Log.h>
#include <xutils/Timer.h>
#include <xassert/XAssert.h>

#include <NTL/ZZ_pX.h>

using namespace std;
using namespace libfqfft;
using namespace libutt;

int main() {
  initialize(nullptr, 0);
  std::vector<G1> bases;
  int count = 3, numBench = 0;
  double avgPctage = 0.0;
  size_t maxSize = 1024 * 128;

  loginfo << "Picking " << maxSize << " random G1 elements to bench multiexponentiation with...";
  bases.resize(maxSize);
  for (size_t i = 0; i < bases.size(); i++) {
    bases[i] = G1::random_element();
  }
  std::cout << endl;

  // for (size_t i = 1; i <= 1024*128; i *= 2) {
  for (size_t i = maxSize; i >= 1; i /= 2) {
    logperf << "poly degree = " << i - 1 << ", iters = " << count << endl;
    numBench++;
    AveragingTimer conv, exp;

    ZZ_pX poly;
    std::vector<Fr> ffpoly;

    bases.resize(i);

    for (int rep = 0; rep < count; rep++) {
      random(poly, static_cast<long>(i));

      conv.startLap();
      // conv_zp_fr(poly, ffpoly);
      convNtlToLibff(poly, ffpoly);
      conv.endLap();

      exp.startLap();
      multiExp(bases, ffpoly);
      exp.endLap();
    }

    auto avgConv = conv.averageLapTime(), avgExp = exp.averageLapTime();
    double pctage = (double)avgConv / (double)(avgConv + avgExp) * 100.0;
    logperf << " + Conv to libff: " << (double)avgConv / 1000000 << " seconds." << endl;
    logperf << " + Multiexp:      " << (double)avgExp / 1000000 << " seconds." << endl;
    logperf << " + " << pctage << "% time spent converting" << endl;
    logperf << endl;

    avgPctage += pctage;
  }

  avgPctage /= numBench;

  logperf << endl;
  logperf << "On average, " << avgPctage << "% time spent converting" << endl;
  return 0;
}
