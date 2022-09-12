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
#include "Logger.hpp"
#include "crypto/factory.hpp"
#include "crypto/openssl/EdDSA.hpp"
#include "crypto/openssl/EdDSASigner.hpp"
#include "crypto/openssl/EdDSAVerifier.hpp"
#include "crypto/crypto.hpp"

namespace {
using concord::crypto::KeyFormat;
using concord::crypto::generateEdDSAKeyPair;
using concord::crypto::EdDSAHexToPem;
using concord::crypto::openssl::EdDSAPrivateKey;
using concord::crypto::openssl::EdDSAPublicKey;
using concord::crypto::Ed25519PrivateKeyByteSize;
using concord::crypto::Ed25519PublicKeyByteSize;
using concord::crypto::openssl::deserializeKey;
using concord::crypto::CurveType;

class EdDSATests : public ::testing::Test {
 public:
  using Signer = concord::crypto::openssl::EdDSASigner<EdDSAPrivateKey>;
  using Verifier = concord::crypto::openssl::EdDSAVerifier<EdDSAPublicKey>;

  void SetUp() override { initSignerVerifier(KeyFormat::HexaDecimalStrippedFormat); }

  void initSignerVerifier(KeyFormat format) {
    auto keys = generateEdDSAKeyPair(format);

    const auto signingKey = deserializeKey<EdDSAPrivateKey>(keys.first, format);
    const auto verificationKey = deserializeKey<EdDSAPublicKey>(keys.second, format);

    signer_ = std::make_unique<Signer>(signingKey.getBytes());
    verifier_ = std::make_unique<Verifier>(verificationKey.getBytes());
  }

  std::vector<concord::Byte> sign(const std::string& data) {
    const auto expectedSigLength = signer_->signatureLength();
    std::vector<concord::Byte> signature(signer_->signatureLength());
    auto sigLength = signer_->sign(data, signature.data());
    EXPECT_EQ(sigLength, expectedSigLength);
    return signature;
  }

  bool verify(const std::string& data, const std::vector<concord::Byte>& signature) {
    return verifier_->verify(data, signature);
  }

  void signVerify(const std::string& data) { ASSERT_TRUE(verify(data, sign(data))); }

  std::unique_ptr<Signer> signer_;
  std::unique_ptr<Verifier> verifier_;
  static constexpr const char* Message = "Hello VMworld";
};

TEST_F(EdDSATests, TestHexFormatKeyLengths) {
  const auto keyPair = generateEdDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  ASSERT_TRUE(concord::crypto::isValidKey("", keyPair.first, Ed25519PrivateKeyByteSize * 2));
  ASSERT_TRUE(concord::crypto::isValidKey("", keyPair.second, Ed25519PublicKeyByteSize * 2));
}

TEST_F(EdDSATests, TestPEMFormatValidity) {
  initSignerVerifier(KeyFormat::PemFormat);
  signVerify(Message);
}

TEST_F(EdDSATests, TestValidSignature) { signVerify(Message); }

TEST_F(EdDSATests, TestInvalidSignature) {
  auto signature = sign(Message);
  signature[0] = ~signature[0];
  ASSERT_FALSE(verify(Message, signature));
}
}  // namespace

TEST(openssl_test, generateECDSA) {
  auto a = concord::crypto::openssl::generateAsymmetricCryptoKeyPairById(NID_secp256k1, "secp256k1");
  std::cout << a.first->serialize() << std::endl;
  std::cout << a.second->serialize() << std::endl;
  auto b = concord::crypto::openssl::generateAsymmetricCryptoKeyPairById(NID_secp384r1, "secp384r1");
  std::cout << b.first->serialize() << std::endl;
  std::cout << b.second->serialize() << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
