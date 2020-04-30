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

#include "gtest/gtest.h"

#include "helper.hpp"
#include "messages/MsgCode.hpp"
#include "messages/FullExecProofMsg.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(FullExecProofMsg_1, base_methods) {
  auto config = createReplicaConfig();
  ReplicasInfo replicaInfo(config, false, false);
  ReplicaId senderId = 1u;
  NodeIdType clientId = 2u;
  ReqId requestId = 100u;
  std::string root{"merle_root_bytes"};
  std::string executionProof{"execution_proof"};
  std::string signature{"signature_bytes"};
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  FullExecProofMsg msg{senderId,
                       clientId,
                       requestId,
                       static_cast<uint16_t>(signature.size()),
                       root.data(),
                       static_cast<uint16_t>(root.size()),
                       executionProof.data(),
                       static_cast<uint16_t>(executionProof.size()),
                       spanContext};
  EXPECT_FALSE(msg.isForReadOnly());  // request id != 0
  EXPECT_EQ(msg.senderId(), senderId);
  EXPECT_EQ(msg.clientId(), clientId);
  EXPECT_EQ(msg.requestId(), requestId);
  EXPECT_EQ(msg.signatureLength(), signature.size());
  EXPECT_EQ(msg.rootLength(), root.size());
  EXPECT_EQ(memcmp(msg.root(), root.data(), root.size()), 0);
  EXPECT_EQ(msg.executionProofLength(), executionProof.size());
  EXPECT_EQ(memcmp(msg.executionProof(), executionProof.data(), executionProof.size()), 0);
  EXPECT_FALSE(msg.isReady());  // there is no signature

  msg.setSignature(signature.data(), signature.size());
  EXPECT_TRUE(msg.isReady());
  EXPECT_EQ(msg.signatureLength(), signature.size());
  EXPECT_EQ(memcmp(msg.signature(), signature.data(), signature.size()), 0);

  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods(msg, MsgCode::FullExecProof, senderId, spanContext);
  destroyReplicaConfig(config);
}

TEST(FullExecProofMsg_2, base_methods) {
  auto config = createReplicaConfig();
  ReplicasInfo replicaInfo(config, false, false);
  ReplicaId senderId = 1u;
  NodeIdType clientId = 2u;
  std::string root{"merle_root_bytes"};
  std::string executionProof{"execution_proof"};
  std::string signature{"signature_bytes"};
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  FullExecProofMsg msg{senderId,
                       clientId,
                       signature.data(),
                       static_cast<uint16_t>(signature.size()),
                       root.data(),
                       static_cast<uint16_t>(root.size()),
                       executionProof.data(),
                       static_cast<uint16_t>(executionProof.size()),
                       spanContext};
  EXPECT_TRUE(msg.isForReadOnly());  // request id == 0
  EXPECT_EQ(msg.senderId(), senderId);
  EXPECT_EQ(msg.clientId(), clientId);
  EXPECT_EQ(msg.requestId(), 0u);
  EXPECT_EQ(msg.signatureLength(), signature.size());
  EXPECT_EQ(msg.rootLength(), root.size());
  EXPECT_EQ(memcmp(msg.root(), root.data(), root.size()), 0);
  EXPECT_EQ(msg.executionProofLength(), executionProof.size());
  EXPECT_EQ(memcmp(msg.executionProof(), executionProof.data(), executionProof.size()), 0);
  EXPECT_TRUE(msg.isReady());  // there is signature

  msg.setSignature(signature.data(), signature.size());
  EXPECT_TRUE(msg.isReady());
  EXPECT_EQ(msg.signatureLength(), signature.size());
  EXPECT_EQ(memcmp(msg.signature(), signature.data(), signature.size()), 0);

  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods(msg, MsgCode::FullExecProof, senderId, spanContext);
  destroyReplicaConfig(config);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
