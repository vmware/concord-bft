// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "Crypto.hpp"
#include "gtest/gtest.h"

namespace {
TEST(crypto_test, load_keys_from_pem) {
  ASSERT_NO_THROW(bftEngine::impl::ECDSASigner("/concord-bft/bftengine/tests/testCrypto/privateKey.pem"));
  ASSERT_NO_THROW(bftEngine::impl::ECDSAVerifier("/concord-bft/bftengine/tests/testCrypto/publicKey.pem"));
}

TEST(crypto_test, test_sign_and_verify) {
  auto signer = bftEngine::impl::ECDSASigner("/concord-bft/bftengine/tests/testCrypto/privateKey.pem");
  auto verifier = bftEngine::impl::ECDSAVerifier("/concord-bft/bftengine/tests/testCrypto/publicKey.pem");
  std::string data = "Hello-world";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}
}  // namespace
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}