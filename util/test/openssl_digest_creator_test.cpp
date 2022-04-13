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
#include "sha_hash.hpp"
#include "openssl_digest_creator.ipp"

using concord::util::SHA2_256;
using concord::util::SHA3_256;
using concord::util::digest::OpenSSLDigestCreator;

using DigestGeneratorTest_SHA2_256 = OpenSSLDigestCreator<SHA2_256>;
using DigestGeneratorTest_SHA3_256 = OpenSSLDigestCreator<SHA3_256>;

// Hashes are generated via https://emn178.github.io/online-tools/sha256.html
struct SHA_Hashes {
  std::string input;
  std::string hash;
};
SHA_Hashes sha_256_hashes[] = {{"sha256", "5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e"},
                               {"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                               {"vmware", "592cc302663c0021ffa92186bf3c1a579a97e5e01f8b28f766855e5b121f8bda"}};

SHA_Hashes sha3_256_hashes[] = {{"sha3_256", "5ae36d2b4b37209703fb327119f63354c93b5a35a07e1397aad3285b37910fe9"},
                                {"", "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a"},
                                {"vmware", "8c1f524205612f55c39b1f96ca6ebd07262b0b3fd3b25bc90d7b8694024524b9"}};

namespace {

// SHA2_256 tests.
TEST(openssl_digest_creator_test, test_compute_sha2_256) {
  char digest[65]{};

  DigestGeneratorTest_SHA2_256 digest_generator;
  const size_t digestLen = digest_generator.digestLength();

  ASSERT_TRUE(
      digest_generator.compute(sha_256_hashes[0].input.c_str(), sha_256_hashes[0].input.size(), digest, digestLen));
  ASSERT_EQ(
      0,
      memcmp(sha_256_hashes[0].hash.c_str(), concordUtils::bufferToHex(digest, digestLen, false).c_str(), digestLen));

  ASSERT_TRUE(
      digest_generator.compute(sha_256_hashes[1].input.c_str(), sha_256_hashes[1].input.size(), digest, digestLen));
  ASSERT_EQ(
      0,
      memcmp(sha_256_hashes[1].hash.c_str(), concordUtils::bufferToHex(digest, digestLen, false).c_str(), digestLen));

  ASSERT_TRUE(
      digest_generator.compute(sha_256_hashes[2].input.c_str(), sha_256_hashes[2].input.size(), digest, digestLen));
  ASSERT_EQ(
      0, memcmp(sha_256_hashes[2].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
}

TEST(openssl_digest_creator_test, test_update_sha2_256) {
  char digest[65]{};
  {
    DigestGeneratorTest_SHA2_256 digest_generator;
    const size_t digestLen = digest_generator.digestLength();

    digest_generator.update(sha_256_hashes[0].input.data(), sha_256_hashes[0].input.size());
    digest_generator.writeDigest(digest);
    ASSERT_EQ(
        0,
        memcmp(sha_256_hashes[0].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
  }
  {
    DigestGeneratorTest_SHA2_256 digest_generator;
    const size_t digestLen = digest_generator.digestLength();

    digest_generator.update(sha_256_hashes[1].input.data(), sha_256_hashes[1].input.size());
    digest_generator.writeDigest(digest);
    ASSERT_EQ(
        0,
        memcmp(sha_256_hashes[1].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
  }
  {
    DigestGeneratorTest_SHA2_256 digest_generator;
    const size_t digestLen = digest_generator.digestLength();

    digest_generator.update(sha_256_hashes[2].input.data(), sha_256_hashes[2].input.size());
    digest_generator.writeDigest(digest);
    ASSERT_EQ(
        0,
        memcmp(sha_256_hashes[2].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
  }
}

TEST(openssl_digest_creator_test, test_multiple_updates_sha2_256) {
  char digest[65]{};
  DigestGeneratorTest_SHA2_256 digest_generator;
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

// SHA3_256 tests.
TEST(openssl_digest_creator_test, test_compute_sha3_256) {
  char digest[65]{};

  DigestGeneratorTest_SHA3_256 digest_generator;
  const size_t digestLen = digest_generator.digestLength();

  ASSERT_TRUE(
      digest_generator.compute(sha3_256_hashes[0].input.c_str(), sha3_256_hashes[0].input.size(), digest, digestLen));
  ASSERT_EQ(
      0,
      memcmp(sha3_256_hashes[0].hash.c_str(), concordUtils::bufferToHex(digest, digestLen, false).c_str(), digestLen));

  ASSERT_TRUE(
      digest_generator.compute(sha3_256_hashes[1].input.c_str(), sha3_256_hashes[1].input.size(), digest, digestLen));
  ASSERT_EQ(
      0,
      memcmp(sha3_256_hashes[1].hash.c_str(), concordUtils::bufferToHex(digest, digestLen, false).c_str(), digestLen));

  ASSERT_TRUE(
      digest_generator.compute(sha3_256_hashes[2].input.c_str(), sha3_256_hashes[2].input.size(), digest, digestLen));
  ASSERT_EQ(
      0, memcmp(sha3_256_hashes[2].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
}

TEST(openssl_digest_creator_test, test_update_sha3_256) {
  char digest[65]{};
  {
    DigestGeneratorTest_SHA3_256 digest_generator;
    const size_t digestLen = digest_generator.digestLength();

    digest_generator.update(sha3_256_hashes[0].input.data(), sha3_256_hashes[0].input.size());
    digest_generator.writeDigest(digest);
    ASSERT_EQ(
        0,
        memcmp(sha3_256_hashes[0].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
  }
  {
    DigestGeneratorTest_SHA3_256 digest_generator;
    const size_t digestLen = digest_generator.digestLength();

    digest_generator.update(sha3_256_hashes[1].input.data(), sha3_256_hashes[1].input.size());
    digest_generator.writeDigest(digest);
    ASSERT_EQ(
        0,
        memcmp(sha3_256_hashes[1].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
  }
  {
    DigestGeneratorTest_SHA3_256 digest_generator;
    const size_t digestLen = digest_generator.digestLength();

    digest_generator.update(sha3_256_hashes[2].input.data(), sha3_256_hashes[2].input.size());
    digest_generator.writeDigest(digest);
    ASSERT_EQ(
        0,
        memcmp(sha3_256_hashes[2].hash.data(), concordUtils::bufferToHex(digest, digestLen, false).data(), digestLen));
  }
}

TEST(openssl_digest_creator_test, test_multiple_updates_sha3_256) {
  char digest[65]{};
  DigestGeneratorTest_SHA3_256 digest_generator{};
  const size_t digestLen = digest_generator.digestLength();

  std::string str = "This is a ";
  digest_generator.update(str.data(), str.size());

  str = "test string.";
  digest_generator.update(str.data(), str.size());
  digest_generator.writeDigest(digest);
  ASSERT_EQ(0,
            memcmp("37b3279f63dc35759dfa667d635f486afdff132d148b23269fb4552eaac5749b",
                   concordUtils::bufferToHex(digest, digestLen, false).data(),
                   digestLen));
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
