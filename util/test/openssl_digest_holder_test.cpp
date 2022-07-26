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
#include "digest_holder.hpp"
#include "openssl_digest_creator.hpp"

using concord::util::SHA2_256;
using concord::util::SHA3_256;
using concord::util::digest::DigestHolder;
using concord::util::digest::OpenSSLDigestCreator;

using Digest256 = DigestHolder<OpenSSLDigestCreator<SHA2_256>>;
using Digest3_256 = DigestHolder<OpenSSLDigestCreator<SHA3_256>>;

// Hashes are generated via https://emn178.github.io/online-tools/sha256.html
struct SHA_Hashes {
  std::string input;
  std::string hash;
};
SHA_Hashes sha256_hashes[] = {{"sha256", "5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e"},
                              {"",       "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                              {"vmware", "592cc302663c0021ffa92186bf3c1a579a97e5e01f8b28f766855e5b121f8bda"},
                              {"5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e", "20a30eabfaa29aff866ce471dd7bc129cb3c368bc49409a499a1fbd5851b36cb"}
                              /* Digest of digest (sha256_hashes[0].input). Change the input type to Hex in online tool. To calculate digestOfDigest. */};

SHA_Hashes sha3_256_hashes[] = {{"sha3_256", "5ae36d2b4b37209703fb327119f63354c93b5a35a07e1397aad3285b37910fe9"},
                               {"",          "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a"},
                               {"vmware",    "8c1f524205612f55c39b1f96ca6ebd07262b0b3fd3b25bc90d7b8694024524b9"},
                               {"5ae36d2b4b37209703fb327119f63354c93b5a35a07e1397aad3285b37910fe9", "7a8bb5781327aa467e630d2a19f6f0a2fefe72b0e334322b70f70f9dab4c67b5"}
                               /* Digest of digest (sha3_256_hashes[0].input). Change the input type to Hex in online tool. To calculate digestOfDigest. */};

namespace {

// SHA256 test cases.
TEST(openssl_digest_holder_test, test_constructors_sha256) {
  {
    Digest256 digest;
    ASSERT_EQ(true, digest.isZero());
  }
  {
    unsigned char c = 'a';
    Digest256 digest(c);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", DIGEST_SIZE));

    digest.makeZero();
    ASSERT_EQ(true, digest.isZero());
  }
  {
    const char* cp = "VMware.";
    Digest256 digest(cp);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "VMware.", strlen(cp)));

    digest.makeZero();
    ASSERT_EQ(true, digest.isZero());
  }
  {
    const char* cp = "This is a string whose length is greater than DIGEST_SIZE.";
    Digest256 digest(cp);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "This is a string whose length is", DIGEST_SIZE));

    digest.makeZero();
    ASSERT_EQ(true, digest.isZero());
  }
  {
    const char* cp = "VMware.";
    Digest256 digest(cp);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "VMware.", strlen(cp)));

    Digest256 digest_1 = digest;  // Copy constructor test.
    ASSERT_EQ(false, digest_1.isZero());
    ASSERT_EQ(0, memcmp(digest_1.content(), "VMware.", strlen(cp)));

    digest_1.makeZero();
    ASSERT_EQ(true, digest_1.isZero());
  }
  {
    Digest256 digest(sha256_hashes[0].input.data(), sha256_hashes[0].input.size());
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0,
              memcmp(sha256_hashes[0].hash.data(),
                     concordUtils::bufferToHex(digest.get(), DIGEST_SIZE, false).data(),
                     DIGEST_SIZE));

    std::string s1 = concordUtils::bufferToHex(digest.getForUpdate(), DIGEST_SIZE, false);
    std::string s2 = digest.toString();

    ASSERT_EQ(s1, s2);
  }
}

TEST(openssl_digest_holder_test, test_overloaded_operators_sha256) {
  {
    const char* cp = "VMware.";
    Digest256 digest_1(cp);
    Digest256 digest_2(cp);

    ASSERT_EQ(true, digest_1 == digest_2);
    ASSERT_EQ(false, digest_1 != digest_2);

    ASSERT_EQ(true, digest_2 == digest_1);
    ASSERT_EQ(false, digest_2 != digest_1);
  }
  {
    const char* cp = "VMware.";
    Digest256 digest_1(cp);
    Digest256 digest_2('c');

    ASSERT_EQ(false, digest_1 == digest_2);
    ASSERT_EQ(true, digest_1 != digest_2);
  }
  {
    const char* cp = "VMware.";
    Digest256 digest_1(cp);
    Digest256 digest_2;
    digest_2 = digest_1;  // Assignment copy operator.

    ASSERT_EQ(true, digest_1 == digest_2);
    ASSERT_EQ(false, digest_1 != digest_2);
  }
}

TEST(openssl_digest_holder_test, test_digest_of_digest_sha256) {
  Digest256 digest(sha256_hashes[0].input.data(), sha256_hashes[0].input.size());
  Digest256 digest_of_digest;

  digest.digestOfDigest(digest_of_digest);

  ASSERT_EQ(true, digest_of_digest.toString() == sha256_hashes[3].hash);
}

// SHA3-256 test cases.
TEST(openssl_digest_holder_test, test_constructors_sha3_256) {
  {
    Digest3_256 digest;
    ASSERT_EQ(true, digest.isZero());
  }
  {
    unsigned char c = 'a';
    Digest3_256 digest(c);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", DIGEST_SIZE));

    digest.makeZero();
    ASSERT_EQ(true, digest.isZero());
  }
  {
    const char* cp = "VMware.";
    Digest3_256 digest(cp);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "VMware.", strlen(cp)));

    digest.makeZero();
    ASSERT_EQ(true, digest.isZero());
  }
  {
    const char* cp = "This is a string whose length is greater than DIGEST_SIZE.";
    Digest3_256 digest(cp);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "This is a string whose length is", DIGEST_SIZE));

    digest.makeZero();
    ASSERT_EQ(true, digest.isZero());
  }
  {
    const char* cp = "VMware.";
    Digest3_256 digest(cp);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "VMware.", strlen(cp)));

    Digest3_256 digest_1 = digest;  // Copy constructor test.
    ASSERT_EQ(false, digest_1.isZero());
    ASSERT_EQ(0, memcmp(digest_1.content(), "VMware.", strlen(cp)));

    digest_1.makeZero();
    ASSERT_EQ(true, digest_1.isZero());
  }
  {
    Digest3_256 digest(sha3_256_hashes[0].input.data(), sha3_256_hashes[0].input.size());
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0,
              memcmp(sha3_256_hashes[0].hash.data(),
                     concordUtils::bufferToHex(digest.get(), DIGEST_SIZE, false).data(),
                     DIGEST_SIZE));

    auto s1 = concordUtils::bufferToHex(digest.getForUpdate(), DIGEST_SIZE, false);
    auto s2 = digest.toString();

    ASSERT_EQ(s1, s2);
  }
}

TEST(openssl_digest_holder_test, test_overloaded_operators_sha3_256) {
  {
    const char* cp = "VMware.";
    Digest3_256 digest_1(cp);
    Digest3_256 digest_2(cp);

    ASSERT_EQ(true, digest_1 == digest_2);
    ASSERT_EQ(false, digest_1 != digest_2);

    ASSERT_EQ(true, digest_2 == digest_1);
    ASSERT_EQ(false, digest_2 != digest_1);
  }
  {
    const char* cp = "VMware.";
    Digest3_256 digest_1(cp);
    Digest3_256 digest_2('c');

    ASSERT_EQ(false, digest_1 == digest_2);
    ASSERT_EQ(true, digest_1 != digest_2);
  }
  {
    const char* cp = "VMware.";
    Digest3_256 digest_1(cp);
    Digest3_256 digest_2;
    digest_2 = digest_1;  // Assignment copy operator.

    ASSERT_EQ(true, digest_1 == digest_2);
    ASSERT_EQ(false, digest_1 != digest_2);
  }
}

TEST(openssl_digest_holder_test, test_digest_of_digest_sha3_256) {
  Digest3_256 digest(sha3_256_hashes[0].input.data(), sha3_256_hashes[0].input.size());
  Digest3_256 digest_of_digest;

  digest.digestOfDigest(digest_of_digest);

  ASSERT_EQ(true, digest_of_digest.toString() == sha3_256_hashes[3].hash);
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
