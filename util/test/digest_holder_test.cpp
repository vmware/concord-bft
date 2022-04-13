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
#include "cryptopp_digest_creator.hpp"

using concord::util::digest::DigestHolder;
using concord::util::digest::CryptoppDigestCreator;

using Digest = DigestHolder<CryptoppDigestCreator>;

// Hashes are generated via https://emn178.github.io/online-tools/sha256.html
struct SHA_Hashes {
  std::string input;
  std::string hash;
};
SHA_Hashes sha256_hashes[] = {{"sha256", "5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e"},
                              {"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                              {"vmware", "592cc302663c0021ffa92186bf3c1a579a97e5e01f8b28f766855e5b121f8bda"},
                              {"5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e", "20a30eabfaa29aff866ce471dd7bc129cb3c368bc49409a499a1fbd5851b36cb"}/* Digest of digest (sha256_hashes[0].input).
                              Change the input type to Hex in online tool. To calculate digestOfDigest. */};

namespace {

TEST(digest_holder_test, test_constructors) {
  {
    Digest digest;
    ASSERT_EQ(true, digest.isZero());
  }
  {
    unsigned char c = 'a';
    Digest digest(c);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", DIGEST_SIZE));

    digest.makeZero();
    ASSERT_EQ(true, digest.isZero());
  }
  {
    const char* cp = "VMware.";
    Digest digest(cp);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "VMware.", strlen(cp)));

    digest.makeZero();
    ASSERT_EQ(true, digest.isZero());
  }
  {
    const char* cp = "This is a string whose length is greater than DIGEST_SIZE.";
    Digest digest(cp);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "This is a string whose length is", DIGEST_SIZE));

    digest.makeZero();
    ASSERT_EQ(true, digest.isZero());
  }
  {
    const char* cp = "VMware.";
    Digest digest(cp);
    ASSERT_EQ(false, digest.isZero());
    ASSERT_EQ(0, memcmp(digest.content(), "VMware.", strlen(cp)));

    Digest digest_1 = digest;  // Copy constructor test.
    ASSERT_EQ(false, digest_1.isZero());
    ASSERT_EQ(0, memcmp(digest_1.content(), "VMware.", strlen(cp)));

    digest_1.makeZero();
    ASSERT_EQ(true, digest_1.isZero());
  }
  {
    Digest digest(sha256_hashes[0].input.data(), sha256_hashes[0].input.size());
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

TEST(digest_holder_test, test_overloaded_operators) {
  {
    const char* cp = "VMware.";
    Digest digest_1(cp);
    Digest digest_2(cp);

    ASSERT_EQ(true, digest_1 == digest_2);
    ASSERT_EQ(false, digest_1 != digest_2);

    ASSERT_EQ(true, digest_2 == digest_1);
    ASSERT_EQ(false, digest_2 != digest_1);
  }
  {
    const char* cp = "VMware.";
    Digest digest_1(cp);
    Digest digest_2('c');

    ASSERT_EQ(false, digest_1 == digest_2);
    ASSERT_EQ(true, digest_1 != digest_2);
  }
  {
    const char* cp = "VMware.";
    Digest digest_1(cp);
    Digest digest_2;
    digest_2 = digest_1;  // Assignment copy operator.

    ASSERT_EQ(true, digest_1 == digest_2);
    ASSERT_EQ(false, digest_1 != digest_2);
  }
}

TEST(digest_holder_test, test_digest_of_digest) {
  Digest digest(sha256_hashes[0].input.data(), sha256_hashes[0].input.size());
  Digest digest_of_digest;

  digest.digestOfDigest(digest_of_digest);

  ASSERT_EQ(true, digest_of_digest.toString() == sha256_hashes[3].hash);
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
