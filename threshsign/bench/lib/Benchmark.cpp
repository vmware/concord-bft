/*
 * Benchmark.cpp
 *
 *  Created on: Jul 16, 2017
 *      Author: alinush
 */

#include "threshsign/Configuration.h"

#include "Benchmark.h"

#include "threshsign/ThresholdSignaturesTypes.h"

#include "Log.h"

std::vector<std::pair<int, int>> Benchmark::getThresholdTestCases(int maxf, bool oneF, bool twoF, bool threeF, int increment) {
    std::vector<std::pair<int,int>> nk;	// first = n, second = k

    int c = 1;
    for(int f = 1; f <= maxf; f += increment) {
        // The total number of signers
        int n = 3*f + 1 + 2*c;

        if(n > MAX_NUM_OF_SHARES) {
            logerror << "You're asking for n = " << n << " signers, but MAX_NUM_OF_SHARES = " << MAX_NUM_OF_SHARES << std::endl;
            throw std::runtime_error("Cannot support that many signers. Please recompile with higher MAX_NUM_OF_SHARES");
        }

        int k1 = 1*f + c + 1;		// threshold f+1+c
        int k2 = 2*f + c + 1;		// threshold 2f+1+c
        int k3 = 3*f + c + 1;		// threshold 3f+1+c

        if(oneF)
            nk.push_back(std::pair<int, int>(n, k1));
        if(twoF)
            nk.push_back(std::pair<int, int>(n, k2));
        if(threeF)
            nk.push_back(std::pair<int, int>(n, k3));
    }

    return nk;
}
