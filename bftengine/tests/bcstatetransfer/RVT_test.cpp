// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <iostream>
#include <string>
#include <random>
#include <vector>
#include <cryptopp/integer.h>

#include "gtest/gtest.h"

#include "Logger.hpp"

#define HASH_SIZE 256

using namespace std;
using namespace CryptoPP;

static std::string randomString(size_t length) {
  static auto& chrs = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::mt19937 rg{std::random_device{}()};
  // String literals are null terminated and index starts from 0
  std::uniform_int_distribution<std::string::size_type> pick(1, sizeof(chrs) - 2);
  std::string s;
  s.reserve(length);
  while (length--) s += chrs[pick(rg)];

  return s;
}

struct InputValues {
  InputValues(const string& l1, const string& l2, const string& p)
      : leaf1(l1.c_str()), leaf2(l2.c_str()), parent(p.c_str()) {}
  Integer leaf1;   // existing leaf val, shld size be 128 bytes?
  Integer leaf2;   // newly added leaf val
  Integer parent;  // mod by parent val (shld size be any different than leaf), shld be 256 byte?
};

class RVTTest : public ::testing::Test {
 public:
  InputValues values_{randomString(HASH_SIZE), randomString(HASH_SIZE), randomString(HASH_SIZE)};
};

TEST_F(RVTTest, basicAdditionSubtraction) {
  auto a = values_.leaf1;
  auto b = values_.leaf2;
  auto c = values_.leaf1 + values_.leaf2;
  ASSERT_EQ(a + b, c);
  ASSERT_EQ(c - b, a);
}

TEST_F(RVTTest, cumulativeAssociativeProperty) {
  auto a = values_.leaf1;
  auto b = values_.leaf2;
  auto c = values_.parent;
  ASSERT_EQ(a + b + c, c + a + b);
  ASSERT_NE(a - b, b - a);
  ASSERT_EQ(a + (b + c), (c + a) + b);
  ASSERT_NE(a + (b + c), (c + a) - b);
}

class RVTHashTestParamFixture : public RVTTest, public testing::WithParamInterface<InputValues> {};
TEST_P(RVTHashTestParamFixture, basicSumAndModOps) {
  auto hash = GetParam();
  auto a = values_.leaf1;
  auto b = values_.leaf2;
  auto c = values_.parent;

  Integer mod_res = (a + b) % c;
  Integer div_res = (a + b) / c;
  ASSERT_EQ(c * div_res + mod_res, a + b);
}

INSTANTIATE_TEST_CASE_P(
    basicSumAndModOps,
    RVTHashTestParamFixture,
    ::testing::Values(InputValues(randomString(HASH_SIZE), randomString(HASH_SIZE), randomString(HASH_SIZE)),
                      InputValues(randomString(HASH_SIZE), randomString(HASH_SIZE), randomString(HASH_SIZE)),
                      InputValues(randomString(HASH_SIZE), randomString(HASH_SIZE), randomString(HASH_SIZE))), );

class RVTTestParamFixture : public RVTTest, public testing::WithParamInterface<std::string> {};
TEST_P(RVTTestParamFixture, validateRawValue) {
  auto str = GetParam();
  Integer input(static_cast<CryptoPP::byte*>((unsigned char*)str.c_str()), str.size());

  ASSERT_EQ(input.MinEncodedSize(), str.size());
  // Encode to string doesn't work
  vector<char> enc_input(input.MinEncodedSize());
  input.Encode((CryptoPP::byte*)enc_input.data(), enc_input.size());
  ostringstream oss_en;
  for (auto& c : enc_input) {
    oss_en << c;
  }
  ASSERT_EQ(oss_en.str(), str);
}
INSTANTIATE_TEST_CASE_P(validateRawValue,
                        RVTTestParamFixture,
                        ::testing::Values(randomString(HASH_SIZE),
                                          randomString(HASH_SIZE),
                                          randomString(HASH_SIZE),
                                          randomString(HASH_SIZE)), );

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
