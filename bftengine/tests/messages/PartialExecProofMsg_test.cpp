#include <iostream>
#include <vector>
#include <cstring>
#include <iostream>
#include <memory>
#include "gtest/gtest.h"
#include "messages/PartialExecProofMsg.hpp"
#include "messages/MsgCode.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "Digest.hpp"
#include "helper.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(PartialExecProofMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  PartialExecProofMsg msg(
      senderId, viewNum, seqNum, tmpDigest, config.thresholdSignerForOptimisticCommit, spanContext);
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(msg.thresholSignatureLength(), config.thresholdSignerForOptimisticCommit->requiredLengthForSignedData());

  std::vector<char> signature(config.thresholdSignerForOptimisticCommit->requiredLengthForSignedData());
  config.thresholdSignerForOptimisticCommit->signData(nullptr, 0, signature.data(), signature.size());

  EXPECT_EQ(memcmp(msg.thresholSignature(), signature.data(), signature.size()), 0);
  testMessageBaseMethods(msg, MsgCode::PartialExecProof, senderId, spanContext);
}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
