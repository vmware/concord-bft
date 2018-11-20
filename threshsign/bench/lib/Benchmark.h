/*
 * Benchmark.h
 *
 *  Created on: Jul 16, 2017
 *      Author: alinush
 */
#pragma once

#include <vector>

class Benchmark {
public:
    static std::vector<std::pair<int, int>> getThresholdTestCases(int maxf, bool oneF, bool twoF, bool threeF, int increment = 50);
};
