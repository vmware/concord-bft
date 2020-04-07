#include "gtest/gtest.h"

#include "helper.hpp"
#include "messages/MsgCode.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(FullCommitProofMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicasInfo replicaInfo(config, false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  std::string commit_proof_signature{"commit_proof_signature"};
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  FullCommitProofMsg msg{senderId,
                         viewNum,
                         seqNum,
                         commit_proof_signature.data(),
                         static_cast<uint16_t>(commit_proof_signature.size()),
                         spanContext};
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(commit_proof_signature, std::string(msg.thresholSignature(), msg.thresholSignatureLength()));
  msg.validate(replicaInfo);
  testMessageBaseMethods(msg, MsgCode::FullCommitProof, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
