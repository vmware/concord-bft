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

#include <string>
#include <memory>
#include <stdexcept>

#include "Logger.hpp"
#include "Utils.h"
#include "XAssert.h"
#include "AutoBuf.h"
#include "app/RelicMain.h"

#include "threshsign/VectorOfShares.h"

using namespace std;
using namespace BLS::Relic;
using namespace threshsign;

void testIth() {
  LOG_INFO(THRESHSIGN_LOG, "Testing ith() and skip()...");
  VectorOfShares signers;

  signers.add(3);
  if (signers.ith(1) != 3) {
    throw std::logic_error("3 is supposed to be first");
  }

  signers.add(5);
  if (signers.ith(2) != 5) {
    throw std::logic_error("5 is supposed to be second");
  }

  signers.add(7);
  if (signers.skip(3, 1) != signers.next(3)) {
    throw std::logic_error("skip and next disagree");
  }

  if (signers.skip(3, 1) != 5) {
    throw std::logic_error("skip is wrong");
  }
  if (signers.skip(3, 2) != 7) {
    throw std::logic_error("skip is wrong");
  }
}

void assertCorrectSerialization(const VectorOfShares& vec) {
  AutoBuf<unsigned char> buf(vec.getByteCount());
  vec.toBytes(buf, buf.size());

  // LOG_DEBUG(THRESHSIGN_LOG, "Serialized vector " << vec << " to " << Utils::bin2hex(buf, buf.size()));
  // logdbg << endl;

  VectorOfShares vecin;
  vecin.fromBytes(buf, buf.size());
  testAssertEqual(vec, vecin);
}

void testSerialization() {
  LOG_INFO(THRESHSIGN_LOG, "Testing serialization...");

  VectorOfShares vec;
  assertCorrectSerialization(vec);

  vec.add(1);
  assertCorrectSerialization(vec);

  vec.add(2);
  assertCorrectSerialization(vec);

  vec.add(MAX_NUM_OF_SHARES);
  assertCorrectSerialization(vec);

  vec.clear();
  for (int i = 1; i <= MAX_NUM_OF_SHARES; i++) {
    vec.add(i);
  }
  assertCorrectSerialization(vec);

  for (int i = 0; i < 128; i++) {
    vec.clear();
    VectorOfShares::randomSubset(vec, MAX_NUM_OF_SHARES, MAX_NUM_OF_SHARES / 2);
    assertCorrectSerialization(vec);
  }
}

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)lib;
  (void)args;

  testSerialization();
  testIth();

  return 0;
}
