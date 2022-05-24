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

#include <string>
#include <vector>
#include <cstring>
#include <iostream>
#include <random>
#include <memory>
#include "OpenTracing.hpp"
#include "gtest/gtest.h"
#include "messages/PrePrepareMsg.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "helper.hpp"
#include "ReservedPagesMock.hpp"
#include "EpochManager.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;
using concord::util::digest::DigestUtil;

bftEngine::test::ReservedPagesMock<EpochManager> res_pages_mock_;

ClientRequestMsg create_client_request() {
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};

  return ClientRequestMsg(1u,
                          'F',
                          reqSeqNum,
                          sizeof(request),
                          request,
                          requestTimeoutMilli,
                          correlationId,
                          0,
                          concordUtils::SpanContext{spanContext});
}

std::string getRandomStringOfLength(size_t len) {
  std::vector<char> alphabet{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
                             'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
                             'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                             'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ' '};

  std::mt19937_64 eng{std::random_device{}()};
  std::uniform_int_distribution<> dist{0, static_cast<int>(alphabet.size() - 1)};
  std::string res;
  while (len > 0) {
    res += alphabet[dist(eng)];
    len--;
  }
  return res;
}

void create_random_client_requests(std::vector<std::shared_ptr<ClientRequestMsg>>& crmv, size_t numMsgs) {
  crmv.clear();
  std::mt19937_64 eng{std::random_device{}()};
  std::uniform_int_distribution<> dist{12, 10000};
  std::uniform_int_distribution<> msdist{0, 1000};

  while (numMsgs > 0) {
    uint64_t reqSeqNum = 100u + numMsgs;
    const std::string request = getRandomStringOfLength(dist(eng));
    const uint64_t requestTimeoutMilli = msdist(eng);
    const std::string correlationId = "correlationId";
    const char rawSpanContext[] = {"span_\0context"};
    const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
    crmv.push_back(std::shared_ptr<ClientRequestMsg>{new ClientRequestMsg(1u,
                                                                          'F',
                                                                          reqSeqNum,
                                                                          request.size(),
                                                                          request.c_str(),
                                                                          requestTimeoutMilli,
                                                                          correlationId,
                                                                          0,
                                                                          concordUtils::SpanContext{spanContext})});
    numMsgs--;
  }
}

class PrePrepareMsgTestFixture : public ::testing::Test {
 public:
  PrePrepareMsgTestFixture()
      : config{createReplicaConfig()},
        replicaInfo(config, false, false),
        sigManager(createSigManager(config.replicaId,
                                    config.replicaPrivateKey,
                                    concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                    config.publicKeysOfReplicas,
                                    replicaInfo)) {}

  ReplicaConfig& config;
  ReplicasInfo replicaInfo;
  std::unique_ptr<SigManager> sigManager;
};

TEST_F(PrePrepareMsgTestFixture, finalize_and_validate) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);

  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  std::vector<std::shared_ptr<ClientRequestMsg>> crmv;
  create_random_client_requests(crmv, 200u);
  size_t req_size = 0;
  for (const auto& crm : crmv) {
    req_size += crm->size();
  }

  PrePrepareMsg msg(senderId, viewNum, seqNum, commitPath, concordUtils::SpanContext{spanContext}, req_size);

  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(msg.firstPath(), commitPath);
  EXPECT_EQ(msg.isNull(), false);
  EXPECT_EQ(msg.numberOfRequests(), 0u);

  std::vector<std::string> dv;
  for (const auto& crm : crmv) {
    msg.addRequest(crm->body(), crm->size());
    Digest d;
    DigestUtil::compute(crm->body(), crm->size(), (char*)&d, sizeof(Digest));
    dv.push_back({d.content(), sizeof(Digest)});
  }
  EXPECT_NO_THROW(msg.finishAddingRequests());  // create the digest
  EXPECT_EQ(msg.numberOfRequests(), 200u);

  std::string dod;
  for (const auto& s : dv) {
    dod.append(s);
  }
  Digest d;
  DigestUtil::compute(dod.c_str(), dod.size(), (char*)&d, sizeof(Digest));
  EXPECT_EQ(d, msg.digestOfRequests());
  EXPECT_NO_THROW(msg.validate(replicaInfo));  // validate the same digest
}

TEST_F(PrePrepareMsgTestFixture, create_and_compare) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientRequestMsg client_request = create_client_request();
  PrePrepareMsg msg(
      senderId, viewNum, seqNum, commitPath, concordUtils::SpanContext{spanContext}, client_request.size() * 2);
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(msg.firstPath(), commitPath);
  EXPECT_EQ(msg.isNull(), false);
  EXPECT_EQ(msg.numberOfRequests(), 0u);

  msg.addRequest(client_request.body(), client_request.size());
  msg.addRequest(client_request.body(), client_request.size());
  msg.finishAddingRequests();

  EXPECT_EQ(msg.numberOfRequests(), 2u);
  EXPECT_NO_THROW(msg.updateView(msg.viewNumber() + 1));
  EXPECT_EQ(msg.viewNumber(), 3u);
  EXPECT_EQ(msg.firstPath(), CommitPath::SLOW);
  EXPECT_NO_THROW(msg.validate(replicaInfo));

  RequestsIterator iterator(&msg);
  for (size_t i = 0; i < msg.numberOfRequests(); ++i) {
    char* request = nullptr;
    iterator.getAndGoToNext(request);
    ClientRequestMsg msg((ClientRequestMsgHeader*)request);
    EXPECT_EQ(memcmp(msg.body(), client_request.body(), msg.size()), 0);
  }
  iterator.restart();
}

TEST_F(PrePrepareMsgTestFixture, create_null_message) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  auto null_msg =
      std::make_unique<PrePrepareMsg>(senderId, viewNum, seqNum, commitPath, concordUtils::SpanContext{spanContext}, 0);

  auto& msg = *null_msg;
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(msg.firstPath(), commitPath);
  EXPECT_EQ(msg.isNull(), true);
  EXPECT_EQ(msg.numberOfRequests(), 0u);
}

TEST_F(PrePrepareMsgTestFixture, base_methods) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientRequestMsg client_request = create_client_request();
  PrePrepareMsg msg(
      senderId, viewNum, seqNum, commitPath, concordUtils::SpanContext{spanContext}, client_request.size());
  msg.addRequest(client_request.body(), client_request.size());
  msg.finishAddingRequests();
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods(msg, MsgCode::PrePrepare, senderId, spanContext);
}

TEST_F(PrePrepareMsgTestFixture, test_prePrepare_size) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  // Create random span
  uint span_size = rand() % 1024;
  const std::string spanContext = getRandomStringOfLength(span_size);
  PrePrepareMsg msg(senderId,
                    viewNum,
                    seqNum,
                    commitPath,
                    concordUtils::SpanContext{spanContext},
                    config.getmaxExternalMessageSize());
  std::vector<std::shared_ptr<ClientRequestMsg>> client_request;

  while (true) {
    client_request.clear();
    create_random_client_requests(client_request, 1u);
    if (client_request.front()->size() > msg.remainingSizeForRequests()) break;
    msg.addRequest(client_request.front()->body(), client_request.front()->size());
  }
  msg.finishAddingRequests();
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  ASSERT_TRUE(msg.size() <= config.getmaxExternalMessageSize());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
