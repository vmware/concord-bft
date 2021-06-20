// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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
#include "RollingAvgAndVar.hpp"
#include <iostream>
#include <random>

using namespace bftEngine::impl;

TEST(RollingAvgAndVar, variance) {
  RollingAvgAndVar rav;
  ASSERT_EQ(0, rav.numOfElements());

  for (auto i : {45.5, 200.0, 332.0, 400.0, 480.0}) {
    rav.add(i);
  }
  ASSERT_EQ(rav.var(), 29458.25);

  rav.reset();
  // zero variance
  for (auto i : {45.5, 45.5, 45.5, 45.5, 45.5}) {
    rav.add(i);
  }
  ASSERT_EQ(rav.var(), 0);

  rav.reset();
  // Variance is positive
  for (auto i : {-45.5, -200.0, -332.0, -400.0, -480.0}) {
    rav.add(i);
  }
  ASSERT_EQ(rav.var(), 29458.25);
}

// Test variance precision - large mean, small var
// https://www.johndcook.com/blog/2008/09/26/comparing-three-methods-of-computing-standard-deviation/
TEST(RollingAvgAndVar, variance_presicion) {
  RollingAvgAndVar rav;
  ASSERT_EQ(0, rav.numOfElements());

  std::default_random_engine generator;
  std::uniform_real_distribution<double> distribution(0.0, 0.5);

  double largeNum = 1000000000;
  for (int i = 0; i < 1000; ++i) {
    double number = largeNum + distribution(generator);
    rav.add(number);
  }
  ASSERT_LT(rav.var(), 0.5);
  ASSERT_GT(rav.avg(), largeNum);
}

TEST(RollingAvgAndVar, mean) {
  RollingAvgAndVar rav;
  ASSERT_EQ(0, rav.numOfElements());

  for (auto v : {1.0, 2.0, 3.0, 4.0, 5.0}) {
    rav.add(v);
  }

  ASSERT_EQ(5, rav.numOfElements());
  ASSERT_EQ(3, rav.avg());

  rav.reset();

  for (auto v : {1.0, 1.0, 1.0, 3.0}) {
    rav.add(v);
  }
  ASSERT_EQ(6.0 / 4, rav.avg());

  rav.reset();
  for (auto i = 0; i < 1000; ++i) {
    rav.add(i);
  }
  ASSERT_EQ(499.5, rav.avg());

  rav.reset();
  for (auto i = 0; i < 1000; ++i) {
    rav.add(0);
  }
  ASSERT_EQ(0, rav.avg());

  rav.reset();
  for (auto i = 0; i < 1000; ++i) {
    rav.add(-1 * i);
  }
  ASSERT_EQ(-499.5, rav.avg());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
