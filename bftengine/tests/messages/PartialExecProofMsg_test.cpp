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
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum seq_num = 3u;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest tmpDigest;
  PartialExecProofMsg msg(
      replica_id, view_num, seq_num, tmpDigest, config.thresholdSignerForOptimisticCommit, span_context);
  EXPECT_EQ(msg.viewNumber(), view_num);
  EXPECT_EQ(msg.seqNumber(), seq_num);
  EXPECT_EQ(msg.thresholSignatureLength(), config.thresholdSignerForOptimisticCommit->requiredLengthForSignedData());

  std::vector<char> signature(config.thresholdSignerForOptimisticCommit->requiredLengthForSignedData());
  config.thresholdSignerForOptimisticCommit->signData(nullptr, 0, signature.data(), signature.size());

  EXPECT_EQ(memcmp(msg.thresholSignature(), signature.data(), signature.size()), 0);
  testMessageBaseMethods(msg, MsgCode::PartialExecProof, replica_id, span_context);
}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
