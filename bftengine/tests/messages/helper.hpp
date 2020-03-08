#pragma once

#include <cstring>
#include <memory>
#include "gtest/gtest.h"
#include "bftengine/ReplicaConfig.hpp"
#include "Serializable.h"
#include "messages/MessageBase.hpp"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "threshsign/IPublicKey.h"

class IShareSecretKeyDummy : public IShareSecretKey {
 public:
  std::string toString() const override { return "IShareSecretKeyDummy"; }
};

class IShareVerificationKeyDummy : public IShareVerificationKey {
 public:
  std::string toString() const override { return "IShareVerificationKeyDummy"; }
};

class IThresholdSignerDummy : public IThresholdSigner,
                              public concord::serialize::SerializableFactory<IThresholdSignerDummy> {
 public:
  int requiredLengthForSignedData() const override { return 2048; }
  void signData(const char *hash, int hashLen, char *outSig, int outSigLen) override {
    std::memset(outSig, 'S', outSigLen);
  }

  const IShareSecretKey &getShareSecretKey() const override { return shareSecretKey; }
  const IShareVerificationKey &getShareVerificationKey() const override { return shareVerifyKey; }
  const std::string getVersion() const override { return "1"; }
  void serializeDataMembers(std::ostream &outStream) const override {}
  void deserializeDataMembers(std::istream &outStream) override {}
  IShareSecretKeyDummy shareSecretKey;
  IShareVerificationKeyDummy shareVerifyKey;
};

class IThresholdAccumulatorDummy : public IThresholdAccumulator {
 public:
  int add(const char *sigShareWithId, int len) override { return 0; }
  void setExpectedDigest(const unsigned char *msg, int len) override {}
  bool hasShareVerificationEnabled() const override { return true; }
  int getNumValidShares() const override { return 0; }
  void getFullSignedData(char *outThreshSig, int threshSigLen) override {}
  IThresholdAccumulator *clone() override { return nullptr; }
};

class IThresholdVerifierDummy : public IThresholdVerifier,
                                public concord::serialize::SerializableFactory<IThresholdVerifierDummy> {
 public:
  IThresholdAccumulator *newAccumulator(bool withShareVerification) const override {
    return new IThresholdAccumulatorDummy;
  }
  void release(IThresholdAccumulator *acc) override {}
  bool verify(const char *msg, int msgLen, const char *sig, int sigLen) const override { return true; }
  int requiredLengthForSignedData() const override { return 2048; }
  const IPublicKey &getPublicKey() const override { return shareVerifyKey; }
  const IShareVerificationKey &getShareVerificationKey(ShareID signer) const override { return shareVerifyKey; }

  const std::string getVersion() const override { return "1"; }
  void serializeDataMembers(std::ostream &outStream) const override {}
  void deserializeDataMembers(std::istream &outStream) override {}
  IShareVerificationKeyDummy shareVerifyKey;
};

inline bftEngine::ReplicaConfig createReplicaConfig() {
  bftEngine::ReplicaConfig config;
  config.numReplicas = 4;
  config.fVal = 1;
  config.cVal = 0;
  config.replicaId = 3;
  config.numOfClientProxies = 8;
  config.statusReportTimerMillisec = 15;
  config.concurrencyLevel = 5;
  config.viewChangeProtocolEnabled = true;
  config.viewChangeTimerMillisec = 12;
  config.autoPrimaryRotationEnabled = false;
  config.autoPrimaryRotationTimerMillisec = 42;
  config.maxExternalMessageSize = 1024 * 4;
  config.maxNumOfReservedPages = 256;
  config.maxReplyMessageSize = 1024;
  config.sizeOfReservedPage = 2048;
  config.debugStatisticsEnabled = true;

  // config.replicaPrivateKey = replicaPrivateKey;
  // config.publicKeysOfReplicas.insert(IdToKeyPair(0, publicKeyValue1));
  // config.publicKeysOfReplicas.insert(IdToKeyPair(1, publicKeyValue2));
  // config.publicKeysOfReplicas.insert(IdToKeyPair(2, publicKeyValue3));
  // config.publicKeysOfReplicas.insert(IdToKeyPair(3, publicKeyValue4));

  config.thresholdSignerForExecution = nullptr;
  config.thresholdVerifierForExecution = new IThresholdVerifierDummy;
  config.thresholdSignerForSlowPathCommit = new IThresholdSignerDummy;
  config.thresholdVerifierForSlowPathCommit = new IThresholdVerifierDummy;
  config.thresholdSignerForCommit = new IThresholdSignerDummy;
  config.thresholdVerifierForCommit = new IThresholdVerifierDummy;
  config.thresholdSignerForOptimisticCommit = new IThresholdSignerDummy;
  config.thresholdVerifierForOptimisticCommit = new IThresholdVerifierDummy;
  config.singletonFromThis();
  return config;
}

inline void printBody(const char *body, size_t size) {
  for (size_t i = 0; i < size; ++i) {
    std::cout << +body[i];
  }
  std::cout << "|end" << std::endl;
}

inline void testMessageBaseMethods(const MessageBase &tested,
                                   MsgType type,
                                   NodeIdType senderId,
                                   const std::string &spanContext) {
  EXPECT_EQ(tested.senderId(), senderId);
  EXPECT_EQ(tested.type(), type);
  EXPECT_EQ(tested.spanContext(), spanContext);
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
