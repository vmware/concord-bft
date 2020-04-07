#include <iostream>
#include <vector>
#include <cstring>
#include <iostream>
#include <memory>
#include "gtest/gtest.h"
#include "messages/PartialCommitProofMsg.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "messages/MsgCode.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "Digest.hpp"
#include "helper.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(PartialCommitProofMsg, create_and_compare) {
  auto config = createReplicaConfig();
  config.singletonFromThis();

  ReplicasInfo replicaInfo(config, false, false);

  ReplicaId senderId = 1u;
  ViewNum viewNum = 0u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  PartialCommitProofMsg msg(
      senderId, viewNum, seqNum, commitPath, tmpDigest, config.thresholdSignerForOptimisticCommit, spanContext);

  EXPECT_EQ(msg.senderId(), senderId);
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(msg.commitPath(), commitPath);
  EXPECT_EQ(msg.spanContextSize(), spanContext.size());
  EXPECT_EQ(msg.spanContext(), spanContext);
  EXPECT_EQ(msg.thresholSignatureLength(), config.thresholdSignerForOptimisticCommit->requiredLengthForSignedData());

  std::vector<char> signature(config.thresholdSignerForOptimisticCommit->requiredLengthForSignedData());
  config.thresholdSignerForOptimisticCommit->signData(nullptr, 0, signature.data(), signature.size());

  EXPECT_EQ(memcmp(msg.thresholSignature(), signature.data(), signature.size()), 0);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
}

TEST(PartialCommitProofMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicaId senderId = 1u;
  ViewNum viewNum = 1u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  PartialCommitProofMsg msg(
      senderId, viewNum, seqNum, commitPath, tmpDigest, config.thresholdSignerForOptimisticCommit, spanContext);
  testMessageBaseMethods(msg, MsgCode::PartialCommitProof, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
