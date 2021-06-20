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

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cassert>
#include <memory>
#include <stdexcept>
#include <sstream>
#include <inttypes.h>

#include "app/RelicMain.h"

#include "Logger.hpp"
#include "Utils.h"
#include "Timer.h"
#include "XAssert.h"

using namespace std;

#include "threshsign/bls/relic/Library.h"

using namespace BLS::Relic;

/**
 * RELIC has some bugs in gt_size_bin(): it gets stuck in an infinte loop if given a zero or one gt_t.
 * This test suite makes sure g1_t, g2_t and bn_t don't have similar problems.
 */
void testRandomSerialize() {
  GTT tr, tz, tu;
  gt_rand(tr);
  gt_zero(tz);
  gt_set_unity(tu);

  stringstream ss;
  ss << "Random GTT: " << tr << endl;
  ss << "Zero GTT: " << tz << endl;
  ss << "Unity GTT: " << tu << endl;

  LOG_TRACE(THRESHSIGN_LOG, endl << ss.str());
  ss.clear();

  G1T g1r, g1i;
  g1_rand(g1r);
  g1_set_infty(g1i);

  ss << "Random G1T: " << g1r << endl;
  ss << "Infty G1T: " << g1i << endl;
  LOG_TRACE(THRESHSIGN_LOG, endl << ss.str());
  ss.clear();

  G2T g2r, g2i;
  g2_rand(g2r);
  g2_set_infty(g2i);

  ss << "Random G2T: " << g2r << endl;
  ss << "Infty G2T: " << g2i << endl;
  LOG_TRACE(THRESHSIGN_LOG, endl << ss.str());
  ss.clear();

  BNT b2r, b2z, b2u;
  bn_rand(b2r, BN_POS, BN_PRECI);
  bn_zero(b2z);
  bn_set_dig(b2u, 1);

  ss << "Random BNT: " << b2r << endl;
  ss << "Zero BNT: " << b2z << endl;
  ss << "One BNT: " << b2u << endl;
  LOG_TRACE(THRESHSIGN_LOG, endl << ss.str());
  ss.clear();
}

template <class T>
void assertDeserializesCorrectly(const T& orig) {
  // Serialize to std::string
  stringstream ss;
  ss << orig;
  std::string origStr(ss.str());

  // Deserialize from std::string
  T copy(origStr);

  testAssertEqual(orig, copy);
}

void testSerializeDeserialize() {
  {
    BNT orig;
    bn_rand(orig, BN_POS, BN_PRECI);

    assertDeserializesCorrectly(orig);
  }

  {
    G1T orig;
    g1_rand(orig);

    assertDeserializesCorrectly(orig);
  }

  {
    G2T orig;
    g2_rand(orig);

    assertDeserializesCorrectly(orig);
  }
}

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)args;
  (void)lib;

  LOG_TRACE(THRESHSIGN_LOG, "Serialization test...");
  int n = 1000;
  for (int i = 0; i < n; i++) {
    if (i % 100 == 0) {
      LOG_TRACE(THRESHSIGN_LOG, i + 1 << " out of " << n);
    }

    testRandomSerialize();

    testSerializeDeserialize();
  }

  return 0;
}
