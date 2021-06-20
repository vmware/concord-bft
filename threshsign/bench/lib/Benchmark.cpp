// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "threshsign/Configuration.h"

#include "Benchmark.h"

#include "threshsign/ThresholdSignaturesTypes.h"

#include "Logger.hpp"

std::vector<std::pair<int, int>> Benchmark::getThresholdTestCases(
    int maxf, bool oneF, bool twoF, bool threeF, int increment) {
  std::vector<std::pair<int, int>> nk;  // first = n, second = k

  int c = 1;
  for (int f = 1; f <= maxf; f += increment) {
    // The total number of signers
    int n = 3 * f + 1 + 2 * c;

    if (n > MAX_NUM_OF_SHARES) {
      LOG_ERROR(THRESHSIGN_LOG,
                "You're asking for n = " << n << " signers, but MAX_NUM_OF_SHARES = " << MAX_NUM_OF_SHARES);
      throw std::runtime_error("Cannot support that many signers. Please recompile with higher MAX_NUM_OF_SHARES");
    }

    int k1 = 1 * f + c + 1;  // threshold f+1+c
    int k2 = 2 * f + c + 1;  // threshold 2f+1+c
    int k3 = 3 * f + c + 1;  // threshold 3f+1+c

    if (oneF) nk.push_back(std::pair<int, int>(n, k1));
    if (twoF) nk.push_back(std::pair<int, int>(n, k2));
    if (threeF) nk.push_back(std::pair<int, int>(n, k3));
  }

  return nk;
}
