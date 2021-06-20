// Concord
//
// Copyright (c) 2019, 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <cstdlib>
#include <fstream>

#include "gtest/gtest.h"
#include "sha_hash.hpp"

using namespace concord::util;

namespace {

// Convert a lowercase string hash to an array
template <typename Hash>
typename Hash::Digest string_to_array(const char* s) {
  typename Hash::Digest hash = {0};

  for (size_t i = 0; i < Hash::SIZE_IN_BYTES * 2; i++) {
    auto c = s[i];
    ConcordAssert((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
    uint8_t nibble = 0;
    if (c >= '0' && c <= '9') {
      nibble = c - '0';
    } else if (c >= 'a' && c <= 'f') {
      nibble = c - 'a' + 10;
    }

    if (i % 2 == 0) {
      // upper nibble
      hash[i / 2] = nibble << 4;
    } else {
      // lower nibble
      hash.at(i / 2) |= nibble;
    }
  }

  return hash;
}

template <typename T>
struct SHATest : public ::testing::Test {
  using Type = T;
};

// All hash strings in this test were computed with https://emn178.github.io/online-tools/sha3_256.html
struct SHA3_256TestType {
  using Hash = SHA3_256;
  static constexpr auto OPENSSL_TYPE = "sha3-256";
  static constexpr auto EMPTY_DIGEST = "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a";
  static constexpr auto ARTIST_DIGEST = "2a3697512e6ce65dcf220b5c189c1045db4aaf59855a507b873d51c7505c54a5";
  static constexpr auto REM_DIGEST = "c817e3a5067a8ff4641fe6c7001717302cd07265ced4f7735719bcafbf55315c";
};

// All hash strings in this test were computed with https://emn178.github.io/online-tools/sha256.html
struct SHA2_256TestType {
  using Hash = SHA2_256;
  static constexpr auto OPENSSL_TYPE = "sha256";
  static constexpr auto EMPTY_DIGEST = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  static constexpr auto ARTIST_DIGEST = "70b1e8e06785cae451496104850781f33faf6cc8e0777ecd3a9ccaaefb154b2d";
  static constexpr auto REM_DIGEST = "ca1a9546e25a6074e7ae971701beeeb10ab85920792bd2031fb995832baf8833";
};

using TestTypes = ::testing::Types<SHA3_256TestType, SHA2_256TestType>;
TYPED_TEST_CASE(SHATest, TestTypes, );

TYPED_TEST(SHATest, basic) {
  using Test = typename TestFixture::Type;
  using Hash = typename Test::Hash;

  auto sha = Hash{};
  auto expected = string_to_array<Hash>(Test::EMPTY_DIGEST);
  const char* empty = "";
  auto hash = sha.digest(empty, 0);
  ASSERT_EQ(expected, hash);
  // Repeat check, to see that we can reuse the ctx in the Hash object and
  // get the same result.
  hash = sha.digest(empty, 0);
  ASSERT_EQ(expected, hash);

  expected = string_to_array<Hash>(Test::ARTIST_DIGEST);
  hash = sha.digest("artist", 6);
  ASSERT_EQ(expected, hash);

  expected = string_to_array<Hash>(Test::REM_DIGEST);
  hash = sha.digest("REM", 3);
  ASSERT_EQ(expected, hash);
}

TYPED_TEST(SHATest, update) {
  using Test = typename TestFixture::Type;
  using Hash = typename Test::Hash;

  auto sha = Hash{};
  auto left = sha.digest("artist", 6);
  auto right = sha.digest("REM", 3);

  // Write the left and write hashes concatenated, so we can run a commandline
  // program on them.
  char filename[] = "/tmp/sha_tests_XXXXXX";
  ::close(::mkstemp(filename));

  std::fstream fs;
  fs.open(filename, std::fstream::out | std::fstream::binary);
  fs.write((char*)left.data(), left.size());
  fs.write((char*)right.data(), right.size());
  fs.close();

  // Run an openssl command to generate the result hash
  std::string command = std::string{"openssl "} + Test::OPENSSL_TYPE + " -r " + filename;
  char output[Hash::SIZE_IN_BYTES * 2];
  FILE* cmdfile = popen(command.c_str(), "r");
  auto result = fread(output, 1, Hash::SIZE_IN_BYTES * 2, cmdfile);
  ASSERT_EQ(result, Hash::SIZE_IN_BYTES * 2);
  pclose(cmdfile);
  auto expected = string_to_array<Hash>(output);

  // Delete the tmpfile
  ASSERT_EQ(0, remove(filename));

  sha.init();
  sha.update(left.data(), left.size());
  sha.update(right.data(), right.size());

  ASSERT_EQ(expected, sha.finish());
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
