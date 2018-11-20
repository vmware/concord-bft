/*
 * RelicViabilityTest.cpp
 *
 *  Created on: Thursday, September 21st, 2017
 *      Author: alinush
 */


#include "threshsign/Configuration.h"

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cassert>
#include <memory>
#include <stdexcept>

#include "Log.h"
#include "Utils.h"
#include "Timer.h"
#include "XAssert.h"

using namespace std;

#include "threshsign/bls/relic/Library.h"
#include "threshsign/bls/relic/BlsThresholdAccumulator.h"
#include "threshsign/VectorOfShares.h"
#include "app/RelicMain.h"

using namespace BLS::Relic;

template<class GT>
void benchFastMultExp(int numIters, int numSigners, int reqSigners);

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
    (void)lib;
    (void)args;

    unsigned int seed = static_cast<unsigned int>(time(NULL));
    loginfo << "Randomness seed passed to srand(): " << seed << endl;
    srand(seed);

    loginfo << "Benchmarking fast exponentiated multiplication in G1..." << endl;
    benchFastMultExp<G1T>(100, 1500, 1000);

    loginfo << endl;

    loginfo << "Benchmarking fast exponentiated multiplication in G2..." << endl;
    benchFastMultExp<G2T>(100, 1500, 1000);

    return 0;
}

template<class GT>
void benchFastMultExp(int numIters, int numSigners, int reqSigners) {
    GT r1, r2, r3;
    int n = numSigners + (rand() % 2);
    int k = reqSigners + (rand() % 2);
    assertLessThanOrEqual(reqSigners, numSigners);
    int maxBits = Library::Get().getG2OrderNumBits();
    //int maxBits = 256;
    loginfo << "iters = " << numIters << ", reqSigners = " << reqSigners << ", numSigners = " << numSigners << ", max bits = " << maxBits << endl;

    VectorOfShares s;
    VectorOfShares::randomSubset(s, n, k);


    std::vector<GT> a;
    std::vector<BNT> e;
    a.resize(static_cast<size_t>(n) + 1);
    e.resize(static_cast<size_t>(n) + 1);

    for(ShareID i = s.first(); s.isEnd(i) == false; i = s.next(i)) {
        a[static_cast<size_t>(i)].Random();
        e[static_cast<size_t>(i)].RandomMod(Library::Get().getG2Order());
    }

    assertEqual(r1, GT::Identity());
    assertEqual(r2, GT::Identity());
    assertEqual(r3, GT::Identity());

    // Slow way
    AveragingTimer t1("Naive way:      ");
    for(int i = 0; i < numIters; i++) {
        t1.startLap();
        r2 = GT::Identity();
        for(ShareID i = s.first(); s.isEnd(i) == false; i = s.next(i)) {

            GT& base = a[static_cast<size_t>(i)];
            BNT& exp = e[static_cast<size_t>(i)];

            GT pow = GT::Times(base, exp);
            r2.Add(pow);
        }
        t1.endLap();
    }

    // Fast way
    AveragingTimer t2("fastMultExp:    ");
    for(int i = 0; i < numIters; i++) {
        t2.startLap();
        r1 = fastMultExp<GT>(s, a, e, maxBits);
        t2.endLap();
    }

    // Fast way
    AveragingTimer t3("fastMultExpTwo: ");
    for(int i = 0; i < numIters; i++) {
        t3.startLap();
        r3 = fastMultExpTwo<GT>(s, a, e);
        t3.endLap();
    }

    loginfo << "Ran for " << numIters << " iterations" << endl;
    loginfo << t1 << endl;
    loginfo << t2 << endl;
    loginfo << t3 << endl;

    // Same way?
    if(r1 != r2 || r1 != r3) {
        throw std::runtime_error("Incorrect results returned by one of the implementations.");
    }
}

