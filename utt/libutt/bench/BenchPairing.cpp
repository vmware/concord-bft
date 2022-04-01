#include <utt/PolyCrypto.h>

#include <vector>
#include <cmath>
#include <iostream>
#include <ctime>
#include <fstream>

#include <xutils/Log.h>
#include <xutils/Timer.h>
#include <xassert/XAssert.h>

using namespace std;

using libutt::Fr;
using libutt::G1;
using libutt::G2;

int main(int argc, char *argv[]) {
    libutt::initialize(nullptr, 0);

    if(argc < 2) {
        cout << "Usage: " << argv[0] << " <num-iters>" << endl;
        cout << endl;
        cout << "OPTIONS: " << endl;
        cout << "   <num-iters>    the number of times to repeat the pairing computation" << endl;  
        cout << endl;

        return 1;
    }

    int n = std::stoi(argv[1]);

    G1 a = G1::random_element();
    G2 b = G2::random_element();

    AveragingTimer tn("Same a, b");
    for(int i = 0; i < n; i++) {
        tn.startLap();
        libutt::ReducedPairing(a, b);
        tn.endLap();
    }

    logperf << tn << endl;
    
    AveragingTimer td("Different a, b");
    for(int i = 0; i < n; i++) {
        G1 x = G1::random_element();
        G2 y = G2::random_element();
        td.startLap();
        libutt::ReducedPairing(x, y);
        td.endLap();
    }

    logperf << td << endl;

    return 0;
}
