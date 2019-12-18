// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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
using namespace std;

#include "gtest/gtest.h"
#include "sha3_256.h"

using namespace concord::util;

namespace {

// Convert a lowercase string hash to an array
SHA3_256::Digest string_to_array(const char* s) {
  SHA3_256::Digest hash = {0};

  for (size_t i = 0; i < SHA3_256::SIZE_IN_BYTES * 2; i++) {
    auto c = s[i];
    Assert((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
    uint8_t nibble = 0;
    ;
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

// All hash strings in this test were computed with https://emn178.github.io/online-tools/sha3_256.html
TEST(sha3_256_test, basic) {
  auto sha3 = SHA3_256();
  auto expected = string_to_array("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a");
  const char* empty = "";
  auto hash = sha3.digest(empty, 0);
  ASSERT_EQ(expected, hash);
  // Repeat check, to see that we can reuse the ctx in the SHA3_256 object and
  // get the same result.
  hash = sha3.digest(empty, 0);
  ASSERT_EQ(expected, hash);

  expected = string_to_array("2a3697512e6ce65dcf220b5c189c1045db4aaf59855a507b873d51c7505c54a5");
  hash = sha3.digest("artist", 6);
  ASSERT_EQ(expected, hash);

  expected = string_to_array("c817e3a5067a8ff4641fe6c7001717302cd07265ced4f7735719bcafbf55315c");
  hash = sha3.digest("REM", 3);
  ASSERT_EQ(expected, hash);
}

TEST(sha3_256, update) {
  auto sha3 = SHA3_256();
  auto left = sha3.digest("artist", 6);
  auto right = sha3.digest("REM", 3);

  // Write the left and write hashes concatenated, so we can run a commandline
  // program on them.
  char filename[] = "/tmp/sha3_256_test_XXXXXX";
  ::close(::mkstemp(filename));

  std::fstream fs;
  fs.open(filename, std::fstream::out | std::fstream::binary);
  fs.write((char*)left.data(), left.size());
  fs.write((char*)right.data(), right.size());
  fs.close();

  // Run an openssl command to generate the result hash
  std::string command = std::string("openssl sha3-256 -r ") + filename;
  char output[SHA3_256::SIZE_IN_BYTES * 2];
  FILE* cmdfile = popen(command.c_str(), "r");
  auto result = fread(output, 1, SHA3_256::SIZE_IN_BYTES * 2, cmdfile);
  ASSERT_EQ(result, SHA3_256::SIZE_IN_BYTES * 2);
  pclose(cmdfile);
  auto expected = string_to_array(output);

  // Delete the tmpfile
  ASSERT_EQ(0, remove(filename));

  sha3.init();
  sha3.update(left.data(), left.size());
  sha3.update(right.data(), right.size());

  ASSERT_EQ(expected, sha3.finish());
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
