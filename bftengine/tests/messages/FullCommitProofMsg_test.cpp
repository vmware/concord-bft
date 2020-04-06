#include "gtest/gtest.h"

#include "helper.hpp"
#include "messages/MsgCode.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(FullCommitProofMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicasInfo replica_info(config, false, false);
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum seq_num = 3u;
  std::string commit_proof_signature{"commit_proof_signature"};
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  FullCommitProofMsg msg{replica_id,
                         view_num,
                         seq_num,
                         commit_proof_signature.data(),
                         static_cast<uint16_t>(commit_proof_signature.size()),
                         span_context};
  EXPECT_EQ(msg.viewNumber(), view_num);
  EXPECT_EQ(msg.seqNumber(), seq_num);
  EXPECT_EQ(commit_proof_signature, std::string(msg.thresholSignature(), msg.thresholSignatureLength()));
  msg.validate(replica_info);
  testMessageBaseMethods(msg, MsgCode::FullCommitProof, replica_id, span_context);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
