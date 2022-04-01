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
    
    if(argc < 2) {
        cout << "Usage: " << argv[0] << " <n>" << endl;
        cout << endl;
        cout << "Measures the time for a size-<n> multipairing." << endl;
        return 1;
    }

    size_t numPairings = static_cast<size_t>(std::stoi(argv[1]));

    std::vector<G1> a(numPairings);
    std::vector<G2> b(numPairings);

    for(size_t i = 0; i < numPairings; i++) {
        a[i] = G1::random_element();
        b[i] = G2::random_element();
    }

    loginfo << "Benchmarking size-" << numPairings << " (+1) multipairings" << endl;

    AveragingTimer t1("naive");
    t1.startLap();
    GT r1 = MultiPairingNaive(a, b);
    t1.endLap();
    testAssertNotEqual(r1, GT::one());
    
    loginfo << "Naive pairings average time / pairing: " << static_cast<size_t>(t1.totalLapTime()) / numPairings << " mus" << endl;

    AveragingTimer t2("multi");
    t2.startLap();
    GT r2 = MultiPairing(a, b);
    t2.endLap();
    testAssertEqual(r1, r2);

    loginfo << "Multipairing average time / pairing: " << static_cast<size_t>(t2.totalLapTime()) / numPairings << " mus" << endl;

    loginfo << "All tests succeeded!" << endl;

    return 0;
}
