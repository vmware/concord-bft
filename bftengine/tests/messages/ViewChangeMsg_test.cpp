#include "gtest/gtest.h"

#include <tuple>
#include "helper.hpp"
#include "DigestType.h"
#include "ViewsManager.hpp"
#include "ReplicasInfo.hpp"
#include "SigManager.hpp"
#include "messages/MsgCode.hpp"
#include "messages/ViewChangeMsg.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(ViewChangeMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum seq_num = 3u;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  ReplicasInfo replicaInfo(config, true, true);
  SigManager sigManager(config.replicaId,
                        config.numReplicas + config.numOfClientProxies,
                        config.replicaPrivateKey,
                        config.publicKeysOfReplicas);
  ViewsManager manager(&replicaInfo, &sigManager, config.thresholdVerifierForSlowPathCommit);
  ViewChangeMsg msg(replica_id, view_num, seq_num, span_context);
  EXPECT_EQ(msg.idOfGeneratedReplica(), replica_id);
  EXPECT_EQ(msg.newView(), view_num);
  EXPECT_EQ(msg.lastStable(), seq_num);
  EXPECT_EQ(msg.numberOfElements(), 0u);
  view_num++;
  msg.setNewViewNumber(view_num);
  EXPECT_EQ(msg.newView(), view_num);
  testMessageBaseMethods(msg, MsgCode::ViewChange, replica_id, span_context);

  typedef std::tuple<SeqNum, Digest, ViewNum, bool, ViewNum, size_t, char*> InputTuple;
  std::vector<InputTuple> inputData;
  Digest digest1(1);
  auto originalViewNum1 = view_num;
  auto view_num1 = ++view_num;
  char certificate1[DIGEST_SIZE] = {1};
  auto seq_num1 = ++seq_num;
  inputData.push_back(
      std::make_tuple(seq_num1, digest1, view_num1, true, originalViewNum1, sizeof(certificate1), certificate1));
  msg.addElement(seq_num1, digest1, view_num1, true, originalViewNum1, sizeof(certificate1), certificate1);
  Digest digest2(2);
  auto originalViewNum2 = view_num;
  auto view_num2 = ++view_num;
  char certificate2[DIGEST_SIZE] = {2};
  auto seq_num2 = ++seq_num;
  inputData.push_back(
      std::make_tuple(seq_num2, digest2, view_num2, true, originalViewNum2, sizeof(certificate2), certificate2));
  msg.addElement(seq_num2, digest2, view_num2, true, originalViewNum2, sizeof(certificate2), certificate2);
  EXPECT_EQ(msg.numberOfElements(), 2);

  msg.setNewViewNumber(++view_num);
  msg.finalizeMessage();
  EXPECT_EQ(msg.numberOfElements(), 2);
  testMessageBaseMethods(msg, MsgCode::ViewChange, replica_id, span_context);

  {
    ViewChangeMsg::ElementsIterator iter(&msg);
    for (size_t i = 0; !iter.end(); ++i) {
      ViewChangeMsg::Element* currentElement = nullptr;
      iter.getCurrent(currentElement);
      ViewChangeMsg::Element* element = nullptr;
      EXPECT_TRUE(iter.getAndGoToNext(element));
      EXPECT_EQ(element, currentElement);
      EXPECT_EQ(element->hasPreparedCertificate, true);
      EXPECT_EQ(element->originView, std::get<2>(inputData[i]));
      EXPECT_EQ(element->hasPreparedCertificate, std::get<3>(inputData[i]));
      EXPECT_EQ(element->prePrepareDigest, std::get<1>(inputData[i]));
      EXPECT_EQ(element->seqNum, std::get<0>(inputData[i]));
    }
  }
  {
    ViewChangeMsg::ElementsIterator iter(&msg);
    size_t i = 0;
    for (; !iter.end(); ++i) {
      iter.gotoNext();
    }
    EXPECT_EQ(i, msg.numberOfElements());
  }
  {
    ViewChangeMsg::ElementsIterator iter(&msg);
    ViewChangeMsg::Element* element = nullptr;
    iter.getCurrent(element);
  }
  {
    ViewChangeMsg::ElementsIterator iter(&msg);
    for (const auto& t : inputData) {
      EXPECT_TRUE(iter.goToAtLeast(std::get<0>(t)));
      ViewChangeMsg::Element* element = nullptr;
      iter.getCurrent(element);
      EXPECT_EQ(element->hasPreparedCertificate, true);
      EXPECT_EQ(element->originView, std::get<2>(t));
      EXPECT_EQ(element->hasPreparedCertificate, std::get<3>(t));
      EXPECT_EQ(element->prePrepareDigest, std::get<1>(t));
      EXPECT_EQ(element->seqNum, std::get<0>(t));
    }
  }
  {
    ViewChangeMsg::ElementsIterator iter(&msg);
    EXPECT_FALSE(iter.goToAtLeast(0xFFFF));
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
