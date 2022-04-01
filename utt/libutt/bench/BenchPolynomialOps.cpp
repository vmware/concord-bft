#include <utt/PolyOps.h>
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

int main(int argc, char *argv[]) {
    libutt::initialize(nullptr, 0);

    loginfo << endl;
    loginfo << "Initial NTL numThreads: " << NTL::AvailableThreads() << endl;
    loginfo << endl;
    
    size_t startSz = 16;
    // 2^24 - 1 seems to be the max # of roots of unity supported by BN254
    // Also it seems to be the max degree for which libntl doesn't crash dvorak, but makes it very slow
    if(argc > 1)
        startSz = static_cast<size_t>(std::stoi(argv[1]));

    int count = 2;
    if(argc > 2)
        count = std::stoi(argv[2]);

    if(argc > 3) {
        long numThreads = std::stoi(argv[3]);
        if(numThreads > static_cast<int>(getNumCores())) {
            logerror << "Number of cores for libntl (" << numThreads << ") cannot be bigger than # of cores on machine, which is " << getNumCores() << endl;
            return 1;
        }

        if(numThreads > 1) {
            NTL::SetNumThreads(numThreads);
            loginfo << "Changed NTL NumThreads to " << numThreads << endl;
            loginfo << "NTL pool active: " << NTL::GetThreadPool()->active() << endl;
            loginfo << endl;
        }
    }

    loginfo << "Multiplication benchmark for degree " << startSz << ". Iterating " << count << " time(s)" << endl;
    loginfo << endl;

    for (size_t i = startSz; i <= startSz; i *= 2) {
        vector<Fr> a, b;
        vector<Fr> p1, p2, res;
        ZZ_pX polyX, polyA, polyB;

        bool noFFT = i > 32768;

        AveragingTimer c1, c2, c3, c4;
        for (int rep = 0; rep < count; rep++) {
            {
                //ScopedTimer<> t(std::cout, "Picking random polynomials took ", " microsecs\n");
                NTL::random(polyA, static_cast<long>(i+1));
                NTL::random(polyB, static_cast<long>(i+1));
                convNtlToLibff(polyA, a);
                convNtlToLibff(polyB, b);
            }

            if(!noFFT) {
                c1.startLap();
                _polynomial_multiplication(p1, a, b);
                c1.endLap();
            }


            if(i <= 4096) {
                c2.startLap();
                polynomial_multiplication_naive(p2, a, b);
                c2.endLap();
            }

            c3.startLap();
            {
                c4.startLap();
                mul(polyX, polyA, polyB);
                c4.endLap();
            }
            convNtlToLibff(polyX, p2);
            c3.endLap();

            if(!noFFT) {
                _polynomial_subtraction(res, p1, p2);
                if (res.size() != 0) {
                    logerror
                            << "The two multiplication functions returned different products"
                            << endl;
                    throw std::runtime_error(
                            "One of the multiplication implementations is wrong");
                }
            }
            p1.clear();
            p2.clear();
            
        }
        logperf << "a.size() = " << a.size() << ", b.size() = " << b.size() << ", iters = " << count
                << endl;
        if(c1.numIterations() == 0) { 
            //logperf << " + FFT: skipped"  << endl;
        } else {
            logperf << " + FFT mult: " << (double) c1.averageLapTime() / 1000000
                    << " seconds." << endl;
        }
        if(c2.numIterations() == 0) { 
            //logperf << " + Naive: skipped"  << endl;
        } else {
            logperf << " + Naive mult: " << (double) c2.averageLapTime() / 1000000
                    << " seconds." << endl;
        }
        logperf << " + NTL mult: "
                << (double) c4.averageLapTime() / 1000000 << " +/- " << c4.stddev() / 1000000 << " seconds " << endl;
        logperf << " + NTL (with conv): " << (double) c3.averageLapTime() / 1000000
                << " seconds." << endl;
        logperf << endl;
    }
    return 0;
}
