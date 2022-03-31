// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//

#include "gtest/gtest.h"
#include "utilization.hpp"

using namespace concordUtil;

TEST(Utilization, one_interval) {
  utilization util;
  util.addDuration({100, 120});
  auto result = util.getUtilization();
  ASSERT_EQ(result, 100);
}

TEST(Utilization, full_util) {
  utilization util;
  util.addDuration({100, 120});
  util.addDuration({120, 140});
  util.addDuration({140, 400});
  auto result = util.getUtilization();
  ASSERT_EQ(result, 100);
}

TEST(Utilization, partial_util) {
  utilization util;
  util.addDuration({80, 120});
  util.addDuration({200, 280});
  // total time = 200
  // op time = 120
  // util = 60
  auto result = util.getUtilization();
  ASSERT_EQ(result, 60);
}

TEST(Utilization, do_no_add) {
  utilization util;
  // Start smaller than end
  util.addDuration({120, 80});
  auto result = util.getUtilization();
  ASSERT_EQ(result, 0);
}

TEST(Utilization, do_no_add_with_marker) {
  utilization util;
  util.addMarker();
  // Start smaller than end
  util.addDuration({120, 80});
  auto result = util.getUtilization();
  ASSERT_EQ(result, 0);
}

TEST(Utilization, overlapping) {
  utilization util;
  // Start smaller than end
  util.addDuration({100, 200});
  util.addDuration({150, 300});
  auto result = util.getUtilization();
  ASSERT_EQ(result, 100);
}

TEST(Utilization, overlapping_and_gap) {
  utilization util;
  // Start smaller than end
  util.addDuration({100, 200});
  util.addDuration({150, 300});
  util.addDuration({350, 390});
  auto result = util.getUtilization();
  ASSERT_EQ(result, 82);
}

TEST(Utilization, partial_util2) {
  utilization util;
  util.addDuration({80, 120});
  util.addDuration({120, 280});
  util.addDuration({400, 410});
  util.addDuration({600, 615});
  // total time = 535
  // op time = 225
  // util = 42
  auto result = util.getUtilization();
  ASSERT_EQ(result, 42);
}
