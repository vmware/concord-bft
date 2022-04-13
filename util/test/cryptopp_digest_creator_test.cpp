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
#include "hex_tools.h"
#include "cryptopp_digest_creator.hpp"

using concord::util::digest::CryptoppDigestCreator;

using DigestGeneratorTest = CryptoppDigestCreator;

// Hashes are generated via https://emn178.github.io/online-tools/sha256.html
struct SHA256_Hashes {
  std::string input;
  std::string hash;
};
SHA256_Hashes hashes[] = {{"sha256", "5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e"},
                          {"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                          {"vmware", "592cc302663c0021ffa92186bf3c1a579a97e5e01f8b28f766855e5b121f8bda"}};

namespace {

TEST(cryptopp_digest_creator_test, test_compute) {
  char digest[65]{};

  DigestGeneratorTest digest_generator;
  const size_t digestLen = digest_generator.digestLength();

  ASSERT_TRUE(digest_generator.compute(hashes[0].input.c_str(), hashes[0].input.size(), digest, digestLen));
  ASSERT_EQ(0, memcmp(hashes[0].hash.c_str(), concordUtils::bufferToHex(digest, digestLen, false).c_str(), digestLen));

  ASSERT_TRUE(digest_generator.compute(hashes[1].input.c_str(), hashes[1].input.size(), digest, digestLen));
  ASSERT_EQ(0, memcmp(hashes[1].hash.c_str(), concordUtils::bufferToHex(digest, digestLen, false).c_str(), digestLen));

  ASSERT_TRUE(digest_generator.compute(hashes[2].input.c_str(), hashes[2].input.size(), digest, digestLen));
  ASSERT_EQ(0, memcmp(hashes[2].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
}

TEST(cryptopp_digest_creator_test, test_update) {
  char digest[65]{};
  {
    DigestGeneratorTest digest_generator;
    const size_t digestLen = digest_generator.digestLength();

    digest_generator.update(hashes[0].input.data(), hashes[0].input.size());
    digest_generator.writeDigest(digest);
    ASSERT_EQ(0, memcmp(hashes[0].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
  }
  {
    DigestGeneratorTest digest_generator;
    const size_t digestLen = digest_generator.digestLength();

    digest_generator.update(hashes[1].input.data(), hashes[1].input.size());
    digest_generator.writeDigest(digest);
    ASSERT_EQ(0, memcmp(hashes[1].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
  }
  {
    DigestGeneratorTest digest_generator;
    const size_t digestLen = digest_generator.digestLength();

    digest_generator.update(hashes[2].input.data(), hashes[2].input.size());
    digest_generator.writeDigest(digest);
    ASSERT_EQ(0, memcmp(hashes[2].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
  }
}

TEST(cryptopp_digest_creator_test, test_multiple_updates) {
  char digest[65]{};
  DigestGeneratorTest digest_generator;
  const size_t digestLen = digest_generator.digestLength();

  std::string str = "This is a ";
  digest_generator.update(str.data(), str.size());

  str = "test string.";
  digest_generator.update(str.data(), str.size());
  digest_generator.writeDigest(digest);
  ASSERT_EQ(0,
            memcmp("3eec256a587cccf72f71d2342b6dfab0bbca01697c7e7014540bdd62b72120da",
                   concordUtils::bufferToHex(digest, digestLen, false).data(),
                   digestLen));
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
