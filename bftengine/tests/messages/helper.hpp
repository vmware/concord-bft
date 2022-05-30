// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstring>
#include <memory>
#include <set>
#include <string>

#include "gtest/gtest.h"

#include "bftengine/ReplicaConfig.hpp"
#include "Serializable.h"
#include "messages/MessageBase.hpp"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "threshsign/IPublicKey.h"
#include "CryptoManager.hpp"
#include "SigManager.hpp"

using bftEngine::impl::ReplicasInfo;

class IShareSecretKeyDummy : public IShareSecretKey {
 public:
  std::string toString() const override { return "IShareSecretKeyDummy"; }
};

class IShareVerificationKeyDummy : public IShareVerificationKey {
 public:
  std::string toString() const override { return "IShareVerificationKeyDummy"; }
};

class IThresholdSignerDummy : public IThresholdSigner {
 public:
  int requiredLengthForSignedData() const override { return 2048; }
  void signData(const char *hash, int hashLen, char *outSig, int outSigLen) override {
    std::memset(outSig, 'S', outSigLen);
  }

  const IShareSecretKey &getShareSecretKey() const override { return shareSecretKey; }
  const IShareVerificationKey &getShareVerificationKey() const override { return shareVerifyKey; }
  IShareSecretKeyDummy shareSecretKey;
  IShareVerificationKeyDummy shareVerifyKey;
};

class IThresholdAccumulatorDummy : public IThresholdAccumulator {
 public:
  int add(const char *sigShareWithId, int len) override { return 0; }
  void setExpectedDigest(const unsigned char *msg, int len) override {}
  bool hasShareVerificationEnabled() const override { return true; }
  int getNumValidShares() const override { return 0; }
  size_t getFullSignedData(char *outThreshSig, int threshSigLen) override { return 0; }
  std::set<ShareID> getInvalidShareIds() const override { return {}; }
};

class IThresholdVerifierDummy : public IThresholdVerifier {
 public:
  IThresholdAccumulator *newAccumulator(bool withShareVerification) const override {
    return new IThresholdAccumulatorDummy;
  }
  bool verify(const char *msg, int msgLen, const char *sig, int sigLen) const override { return true; }
  int requiredLengthForSignedData() const override { return 2048; }
  const IPublicKey &getPublicKey() const override { return shareVerifyKey; }
  const IShareVerificationKey &getShareVerificationKey(ShareID signer) const override { return shareVerifyKey; }

  IShareVerificationKeyDummy shareVerifyKey;
};

class TestCryptoSystem : public Cryptosystem {
 public:
  TestCryptoSystem() = default;
  IThresholdVerifier *createThresholdVerifier(uint16_t threshold = 0) override { return new IThresholdVerifierDummy; }
  IThresholdSigner *createThresholdSigner() override { return new IThresholdSignerDummy; }
};

bftEngine::ReplicaConfig &createReplicaConfig(uint16_t fVal = 1, uint16_t cVal = 0);

inline void printBody(const char *body, size_t size) {
  for (size_t i = 0; i < size; ++i) {
    std::cout << +body[i];
  }
  std::cout << "|end" << std::endl;
}

template <typename MessageT>
void testMessageBaseMethods(const MessageT &tested, MsgType type, NodeIdType senderId, const std::string &spanContext) {
  EXPECT_EQ(tested.senderId(), senderId);
  EXPECT_EQ(tested.type(), type);
  EXPECT_EQ(tested.template spanContext<MessageT>().data(), spanContext);
  EXPECT_EQ(tested.spanContextSize(), spanContext.size());

  std::unique_ptr<MessageBase> other{tested.cloneObjAndMsg()};
  EXPECT_TRUE(tested.equals(*other));
  EXPECT_NE(tested.body(), other->body());

  std::vector<char> buffer(tested.sizeNeededForObjAndMsgInLocalBuffer() + /*null flag*/ 1);
  auto ptr = buffer.data();
  auto shifted_ptr = ptr;
  MessageBase::serializeMsg(shifted_ptr, &tested);
  EXPECT_EQ(memcmp(tested.body(), ptr + 1 + 10, tested.size()), 0);
  size_t actualSize = 0u;
  std::unique_ptr<MessageBase> deserialized{MessageBase::deserializeMsg(ptr, buffer.size(), actualSize)};
  EXPECT_EQ(tested.size(), deserialized->size());
  EXPECT_EQ(memcmp(tested.body(), deserialized->body(), deserialized->size()), 0);
  EXPECT_TRUE(other->equals(*deserialized));
}

bftEngine::impl::SigManager *createSigManager(size_t myId,
                                              std::string &myPrivateKey,
                                              concord::util::crypto::KeyFormat replicasKeysFormat,
                                              std::set<std::pair<uint16_t, const std::string>> &publicKeysOfReplicas,
                                              ReplicasInfo &replicasInfo);

void loadPrivateAndPublicKeys(std::string &myPrivateKey,
                              std::set<std::pair<uint16_t, const std::string>> &publicKeysOfReplicas,
                              ReplicaId myId,
                              size_t numReplicas);
