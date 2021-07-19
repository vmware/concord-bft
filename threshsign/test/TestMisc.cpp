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

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cassert>
#include <memory>
#include <algorithm>
#include <stdexcept>

#include "Logger.hpp"
#include "Utils.h"
#include "XAssert.h"
#include "app/Main.h"

using namespace std;

void testUtils();

int AppMain(const std::vector<std::string>& args) {
  (void)args;

  testUtils();

  return 0;
}

void testUtils() {
  testAssertEqual(Utils::numBits(1), 1);
  testAssertEqual(Utils::numBits(2), 2);
  testAssertEqual(Utils::numBits(3), 2);
  testAssertEqual(Utils::numBits(4), 3);
  testAssertEqual(Utils::numBits(5), 3);
  testAssertEqual(Utils::numBits(6), 3);
  testAssertEqual(Utils::numBits(7), 3);
  testAssertEqual(Utils::numBits(8), 4);

  LOG_INFO(THRESHSIGN_LOG, "Utils::numBits passed!");

  testAssertEqual(Utils::pow2(0), 1);
  testAssertEqual(Utils::pow2(1), 2);
  testAssertEqual(Utils::pow2(2), 4);
  testAssertEqual(Utils::pow2(3), 8);
  testAssertEqual(Utils::pow2(4), 16);
  testAssertEqual(Utils::pow2(5), 32);

  LOG_INFO(THRESHSIGN_LOG, "Utils::pow2 passed!");
}
