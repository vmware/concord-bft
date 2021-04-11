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
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);

  ReplicaId senderId = 1u;
  ViewNum viewNum = 0u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  PartialCommitProofMsg msg(senderId,
                            viewNum,
                            seqNum,
                            commitPath,
                            tmpDigest,
                            CryptoManager::instance().thresholdSignerForOptimisticCommit(seqNum),
                            concordUtils::SpanContext{spanContext});

  EXPECT_EQ(msg.senderId(), senderId);
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(msg.commitPath(), commitPath);
  EXPECT_EQ(msg.spanContextSize(), spanContext.size());
  EXPECT_EQ(msg.thresholSignatureLength(),
            CryptoManager::instance().thresholdSignerForOptimisticCommit(seqNum)->requiredLengthForSignedData());

  std::vector<char> signature(
      CryptoManager::instance().thresholdSignerForOptimisticCommit(seqNum)->requiredLengthForSignedData());
  CryptoManager::instance().thresholdSignerForOptimisticCommit(seqNum)->signData(
      nullptr, 0, signature.data(), signature.size());

  EXPECT_EQ(memcmp(msg.thresholSignature(), signature.data(), signature.size()), 0);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
}

TEST(PartialCommitProofMsg, base_methods) {
  ReplicaId senderId = 1u;
  ViewNum viewNum = 1u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  PartialCommitProofMsg msg(senderId,
                            viewNum,
                            seqNum,
                            commitPath,
                            tmpDigest,
                            CryptoManager::instance().thresholdSignerForOptimisticCommit(seqNum),
                            concordUtils::SpanContext{spanContext});
  testMessageBaseMethods(msg, MsgCode::PartialCommitProof, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
