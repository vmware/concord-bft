#include <utt/PolyCrypto.h>

#include <vector>
#include <cmath>
#include <iostream>
#include <ctime>
#include <fstream>

#include <xutils/Log.h>
#include <xutils/Utils.h>
#include <xutils/Timer.h>
#include <xassert/XAssert.h>

using namespace std;

using namespace libutt;

int main(int argc, char *argv[]) {
  libutt::initialize(nullptr, 0);

  if (argc < 2) {
    cout << "Usage: " << argv[0] << " <num-iters>" << endl;
    cout << endl;
    cout << "OPTIONS: " << endl;
    cout << "   <num-iters>    the number of times to repeat the exponentiation" << endl;
    cout << endl;

    return 1;
  }

  size_t n = static_cast<size_t>(std::stoi(argv[1]));

  loginfo << "Picking " << n << " random G1    elements..." << endl;
  auto a = random_group_elems<G1>(n);
  loginfo << "Picking " << n << " random G2    elements..." << endl;
  auto b = random_group_elems<G2>(n);
  loginfo << "Picking " << n << " random field elements..." << endl;
  auto e = random_field_elems(n);

  loginfo << "Doing " << n << " G1 exponentiations..." << endl;
  AveragingTimer tn("Exp G1");
  G1 r;
  for (size_t i = 0; i < n; i++) {
    tn.startLap();
    r = e[i] * a[i];
    tn.endLap();
  }

  logperf << tn << endl;
  logperf << "Total time: " << Utils::humanizeMicroseconds(tn.totalLapTime()) << endl;

  loginfo << "Doing " << n << " G2 exponentiations..." << endl;
  AveragingTimer tm("Exp G2");
  G2 s;
  for (size_t i = 0; i < n; i++) {
    tm.startLap();
    s = e[i] * b[i];
    tm.endLap();
  }

  logperf << tm << endl;

  logperf << "Total time: " << Utils::humanizeMicroseconds(tm.totalLapTime()) << endl;

  return 0;
}
