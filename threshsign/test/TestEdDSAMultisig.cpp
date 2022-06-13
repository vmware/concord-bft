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
#include <openssl/evp.h>
#include <random>
#include "gtest/gtest.h"
#include "threshsign/eddsa/EdDSAMultisigFactory.h"
#include "threshsign/eddsa/EdDSAMultisigSigner.h"
#include "threshsign/eddsa/EdDSAMultisigVerifier.h"
#include "threshsign/eddsa/SingleEdDSASignature.h"

class EdDSAMultisigTest : public testing::Test {
 public:
  std::string sha3_256(std::string msg) {
    constexpr const size_t sha256ByteCount = 32;
    std::array<uint8_t, sha256ByteCount> digest = {0};
    size_t actualDigestSize = sha256ByteCount;
    auto ret = EVP_Digest(msg.data(),
                          msg.size(),
                          digest.data(),
                          reinterpret_cast<unsigned int*>(&actualDigestSize),
                          EVP_sha3_256(),
                          nullptr);
    EXPECT_EQ(ret, 1u);
    EXPECT_EQ(actualDigestSize, sha256ByteCount);
    return std::string((const char*)digest.data(), actualDigestSize);
  }

  std::string testMsgDigest() {
    const std::string msg = "This is a test message";
    return sha3_256(msg);
  }

 protected:
  EdDSAMultisigFactory factory_;

 private:
  void SetUp() override { EDDSA_MULTISIG_LOG.setLogLevel(log4cplus::DEBUG_LOG_LEVEL); }
};

TEST_F(EdDSAMultisigTest, ValidateSerializeDeserialize) {
  auto [privateKey, publicKey] = factory_.newKeyPair();
  auto eddsaPrivateKey = dynamic_cast<EdDSAThreshsignPrivateKey*>(privateKey.get());
  auto eddsaPublicKey = dynamic_cast<EdDSAThreshsignPublicKey*>(publicKey.get());
  auto privateHexString = eddsaPrivateKey->toString();
  auto publicHexString = eddsaPublicKey->toString();
  ASSERT_EQ(privateHexString, fromHexString<EdDSAThreshsignPrivateKey>(privateHexString).toString());
  ASSERT_EQ(publicHexString, fromHexString<EdDSAThreshsignPublicKey>(publicHexString).toString());
}

TEST_F(EdDSAMultisigTest, TestSingleVerificationUsingFactory) {
  auto [privateKey, publicKey] = factory_.newKeyPair();
  auto eddsaPrivateKey = dynamic_cast<EdDSAThreshsignPrivateKey*>(privateKey.get());
  auto eddsaPublicKey = dynamic_cast<EdDSAThreshsignPublicKey*>(publicKey.get());

  auto* signer = factory_.newSigner(1, eddsaPrivateKey->toString().c_str());
  auto* verifier = factory_.newVerifier(1, 1, "", {"", eddsaPublicKey->toString()});
  auto accumulator = verifier->newAccumulator(false);

  auto msg = testMsgDigest();
  SingleEdDSASignature signature;
  signer->signData(msg.data(), (int)msg.size(), reinterpret_cast<char*>(&signature), (int)sizeof(SingleEdDSASignature));

  accumulator->setExpectedDigest(reinterpret_cast<const unsigned char*>(msg.data()), (int)msg.size());
  accumulator->add(reinterpret_cast<const char*>(&signature), (int)sizeof(SingleEdDSASignature));

  auto multisigBytes = verifier->requiredLengthForSignedData();
  auto multisigBuffer = std::make_unique<char[]>((size_t)multisigBytes);

  accumulator->getFullSignedData(multisigBuffer.get(), multisigBytes);
  ASSERT_EQ(memcmp(multisigBuffer.get(), &signature, sizeof(SingleEdDSASignature)), 0);
  ASSERT_TRUE(verifier->verify(msg.data(), (int)msg.size(), multisigBuffer.get(), multisigBytes));
}

TEST_F(EdDSAMultisigTest, TestSignatureAccumulation) {
  constexpr const uint64_t n_signers = 100;
  auto [signers, verifier] = factory_.newRandomSigners(n_signers, n_signers);
  auto accumulator = verifier->newAccumulator(false);
  const std::string digest = testMsgDigest();

  accumulator->setExpectedDigest(reinterpret_cast<const unsigned char*>(digest.data()), (int)digest.size());
  std::vector<SingleEdDSASignature> signatures(signers.size() - 1);
  for (size_t i = 0; i < signatures.size(); i++) {
    signers[i + 1]->signData(
        digest.data(), (int)digest.size(), reinterpret_cast<char*>(&signatures[i]), sizeof(SingleEdDSASignature));
  }

  auto multisigBytes = verifier->requiredLengthForSignedData();
  auto multisigBuffer = std::make_unique<char[]>((size_t)multisigBytes);

  // Make sure that verification does not depend on order, as signatures do not arrive in order
  std::shuffle(signatures.begin(), signatures.end(), std::mt19937{2022});
  for (auto& signature : signatures) {
    auto signatureCount =
        static_cast<size_t>(accumulator->add(reinterpret_cast<const char*>(&signature), sizeof(SingleEdDSASignature)));
    auto actualMultisigBytes = static_cast<int>(accumulator->getFullSignedData(multisigBuffer.get(), multisigBytes));
    if (signatureCount == signatures.size()) {
      ASSERT_TRUE(
          verifier->verify(digest.data(), static_cast<int>(digest.size()), multisigBuffer.get(), actualMultisigBytes));
    } else {
      ASSERT_FALSE(
          verifier->verify(digest.data(), static_cast<int>(digest.size()), multisigBuffer.get(), actualMultisigBytes));
    }
  }
}

TEST_F(EdDSAMultisigTest, TestValidMultiSignatureSmallThreshold) {
  constexpr const uint64_t n_signers = 7;
  const auto digest = testMsgDigest();

  for (uint64_t threshold = 1; threshold <= n_signers; threshold++) {
    auto [signers, verifier] = factory_.newRandomSigners(n_signers, n_signers);
    auto accumulator = verifier->newAccumulator(false);

    accumulator->setExpectedDigest(reinterpret_cast<const unsigned char*>(digest.data()), (int)digest.size());
    std::vector<SingleEdDSASignature> signatures(signers.size() - 1);
    for (size_t i = 0; i < signatures.size(); i++) {
      signers[i + 1]->signData(digest.data(),
                               static_cast<int>(digest.size()),
                               reinterpret_cast<char*>(&signatures[i]),
                               sizeof(SingleEdDSASignature));
    }
    // Make sure that verification does not depend on order, as signatures do not arrive in order
    std::random_shuffle(signatures.begin(), signatures.end());
    for (auto& signature : signatures) {
      accumulator->add(reinterpret_cast<const char*>(&signature), sizeof(SingleEdDSASignature));
    }

    auto multisigBytes = verifier->requiredLengthForSignedData();
    auto multisigBuffer = std::make_unique<char[]>(static_cast<size_t>(multisigBytes));
    accumulator->getFullSignedData(multisigBuffer.get(), multisigBytes);

    ASSERT_TRUE(verifier->verify(digest.data(), static_cast<int>(digest.size()), multisigBuffer.get(), multisigBytes));
  }
}

TEST_F(EdDSAMultisigTest, TestInvalidMultiSignature) {
  constexpr const uint64_t n_signers = 2;
  auto [signers, verifier] = factory_.newRandomSigners(n_signers, n_signers);
  auto accumulator = verifier->newAccumulator(false);
  const std::string msg = "This is a test message";
  const auto digest = sha3_256(msg);

  accumulator->setExpectedDigest(reinterpret_cast<const unsigned char*>(digest.data()),
                                 static_cast<int>(digest.size()));
  std::vector<SingleEdDSASignature> signatures(signers.size());
  for (size_t i = 1; i < signers.size(); i++) {
    signers[i]->signData(digest.data(),
                         static_cast<int>(digest.size()),
                         reinterpret_cast<char*>(&signatures[i]),
                         sizeof(SingleEdDSASignature));
    auto& currentSig = *reinterpret_cast<SingleEdDSASignature*>(&signatures[i]);
    currentSig.signatureBytes[0] = static_cast<uint8_t>(~currentSig.signatureBytes[0]);
    accumulator->add(reinterpret_cast<const char*>(&signatures[i]), sizeof(SingleEdDSASignature));
  }
  auto multisigBytes = verifier->requiredLengthForSignedData();
  auto multisigBuffer = std::make_unique<char[]>((size_t)multisigBytes);
  accumulator->getFullSignedData(multisigBuffer.get(), multisigBytes);

  ASSERT_FALSE(verifier->verify(digest.data(), static_cast<int>(digest.size()), multisigBuffer.get(), multisigBytes));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
