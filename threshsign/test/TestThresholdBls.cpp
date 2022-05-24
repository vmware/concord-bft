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

#include "TestThresholdBls.h"

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cassert>
#include <memory>
#include <stdexcept>
#include <inttypes.h>

#include "Logger.hpp"
#include "Utils.h"
#include "Timer.h"
#include "XAssert.h"

#include "threshsign/bls/relic/Library.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"

#include "app/RelicMain.h"

using namespace std;
using namespace BLS::Relic;

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)args;
  (void)lib;

  std::vector<std::pair<int, int>> nk = {/*{1, 1},
                                         {2, 1},*/
                                         {3, 2},
                                         /*{3, 3},
                                         {5, 3},
                                         {5, 4},
                                         {10, 4},
                                         {11, 7},
                                         {11, 11},
                                         {33, 17},
                                         {33, 32},
                                         {64, 33},
                                         {128, 65},
                                         {257, 129},
                                         {260, 159},
                                         {257, 257},
                                         {276, 275}*/};

  //    std::vector<std::pair<int, int>> nk;
  //	for(size_t i = 1; i <= 301; i += 1) {
  //	    nk.push_back(std::pair<int, int>(i, i));
  //	}

  BLS::Relic::BlsPublicParameters params(BLS::Relic::PublicParametersFactory::getWhatever());

  const char* msg = "blaBlaBlaBlaBlaBlaBlaBlaBlaBlaBlaBlaBlaBlaBlaBlaBla";
  for (bool multisig : {true, false}) {
    LOG_INFO(THRESHSIGN_LOG, "");
    LOG_INFO(THRESHSIGN_LOG, "Testing threshold signatures (useMultisig = " << multisig << ")");
    LOG_INFO(THRESHSIGN_LOG, "");

    for (auto it = nk.begin(); it != nk.end(); it++) {
      int n = it->first;
      int k = it->second;
      (void)n;
      (void)k;

      ThresholdBlsTest t(params, n, k, multisig);
      t.generateKeys();
      t.test(reinterpret_cast<const unsigned char*>(msg), static_cast<int>(strlen(msg)));
    }
  }

  return 0;
}
