#include <messages/CheckpointMsg.hpp>
#include "gtest/gtest.h"
#include "SimpleClientImp.cpp"
#include "messages/MsgsCertificate.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "CheckpointInfo.hpp"
#include "Digest.hpp"
#include "helper.hpp"
#include "SigManager.hpp"
#include "ReservedPagesMock.hpp"
#include "EpochManager.hpp"

using namespace bftEngine;

namespace {

const uint16_t fval = 1;
const uint16_t cval = 1;
const uint16_t numOfReplicas = 3 * fval + 2 * cval + 1;
const uint16_t numOfRequired = 2 * fval + cval + 1;
const uint16_t selfReplicaId = 0;
char* reply = new char[10]{};
int replyLen = strlen(reply);
bftEngine::test::ReservedPagesMock<EpochManager> res_pages_mock_;
ReplicaId randomReplicaId() { return rand() % numOfReplicas; }

class msgsCertificateTestsFixture : public ::testing::Test {
 public:
  msgsCertificateTestsFixture()
      : config(createReplicaConfig()),
        replicaInfo(config, false, false),
        sigManager(createSigManager(config.replicaId,
                                    config.replicaPrivateKey,
                                    concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                    config.publicKeysOfReplicas,
                                    replicaInfo))

  {}
  ReplicaConfig& config;
  ReplicasInfo replicaInfo;
  std::unique_ptr<SigManager> sigManager;

  void test_state_still_consistent() {
    MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate(
        numOfReplicas, fval, numOfRequired, selfReplicaId);

    uint8_t reqSeqNum = 0;
    ReplicaId replicaId = 0;

    // Add bunch of new messages
    for (; reqSeqNum < 2; reqSeqNum++, replicaId++) {
      auto newMsg = new ClientReplyMsg(replicaId, reqSeqNum, reply, replyLen);
      replysCertificate.addMsg(newMsg, replicaId);
    }

    ASSERT_TRUE(!replysCertificate.isInconsistent());

    replysCertificate.resetAndFree();
  }
  void test_self_msg_added_peer_msg_ignored() {
    bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
    MsgsCertificate<CheckpointMsg, true, true, true, CheckpointMsg> replysCertificate(
        numOfReplicas, fval, numOfRequired, selfReplicaId);

    auto selfMsg = new CheckpointMsg(selfReplicaId, 0, 0, Digest(), Digest(), Digest(), false);
    selfMsg->setEpochNumber(0);
    replysCertificate.addMsg(selfMsg, selfReplicaId);

    ASSERT_TRUE(replysCertificate.selfMsg() != nullptr);

    replysCertificate.resetAndFree();
  }
  void test_exceeded_required_msgs_twice() {
    MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate(
        numOfReplicas, fval, numOfRequired, selfReplicaId);

    for (int i = 0; i < 2; i++) {
      uint8_t numOfReplys = 0;
      NodeIdType replicaId = 0;

      while (numOfReplys < numOfRequired) {
        auto msg = new ClientReplyMsg(replicaId, 0, reply, replyLen);
        replysCertificate.addMsg(msg, replicaId);
        numOfReplys++;
        replicaId++;
      }

      ASSERT_TRUE(replysCertificate.isComplete());
      replysCertificate.resetAndFree();
      ASSERT_FALSE(replysCertificate.isComplete());
    }
  }
  void test_certifiy_best_correct_msg() {
    MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate(
        numOfReplicas, fval, numOfRequired, selfReplicaId);

    uint8_t reqSeqNum = 0;

    // Create two identical messages and add them.

    ReplicaId replicaId = randomReplicaId();
    auto bestMsg = new ClientReplyMsg(replicaId, reqSeqNum, reply, replyLen);
    replysCertificate.addMsg(bestMsg, replicaId);

    replicaId = (replicaId + 1) % numOfReplicas;
    auto identBestMsg = new ClientReplyMsg(replicaId, reqSeqNum, reply, replyLen);
    replysCertificate.addMsg(identBestMsg, replicaId);

    // Add bunch of new messages
    for (; reqSeqNum < 4; reqSeqNum++) {
      replicaId = randomReplicaId();
      auto newMsg = new ClientReplyMsg(replicaId, reqSeqNum, reply, replyLen);
      replysCertificate.addMsg(newMsg, replicaId);
    }

    ASSERT_TRUE(SimpleClientImp::equivalent(replysCertificate.bestCorrectMsg(), bestMsg));

    replysCertificate.resetAndFree();
  }
  void test_delete_extra_msg() {
    MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate(
        numOfReplicas, fval, numOfRequired, selfReplicaId);

    bool isMsgAdded = false;
    NodeIdType replicaId = randomReplicaId();

    for (int i = 0; i < 2; i++) {
      auto msg = new ClientReplyMsg(replicaId, i, reply, replyLen);
      isMsgAdded = replysCertificate.addMsg(msg, replicaId);
    }

    ASSERT_FALSE(isMsgAdded);

    replysCertificate.resetAndFree();
  }
  void test_keep_all_msgs() {
    MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate(
        numOfReplicas, fval, numOfRequired, selfReplicaId);

    uint8_t reqSeqNum = 0;

    // Add bunch of new messages
    for (int replicaId = 0; replicaId < numOfReplicas; replicaId++) {
      auto newMsg = new ClientReplyMsg(replicaId, reqSeqNum, reply, replyLen);
      replysCertificate.addMsg(newMsg, replicaId);
      if (replicaId >= numOfRequired) {
        ASSERT_TRUE(replysCertificate.isComplete());
      }
    }

    ASSERT_TRUE(replysCertificate.isFull());

    replysCertificate.resetAndFree();
  }
};

TEST_F(msgsCertificateTestsFixture, state_still_consistent) { test_state_still_consistent(); }

TEST_F(msgsCertificateTestsFixture, self_msg_added_peer_msg_ignored) { test_self_msg_added_peer_msg_ignored(); }

TEST_F(msgsCertificateTestsFixture, exceeded_required_msgs_twice) { test_exceeded_required_msgs_twice(); }

TEST_F(msgsCertificateTestsFixture, certifiy_best_correct_msg) { test_certifiy_best_correct_msg(); }

TEST_F(msgsCertificateTestsFixture, delete_extra_msg) { test_delete_extra_msg(); }

TEST_F(msgsCertificateTestsFixture, keep_all_msgs) { test_keep_all_msgs(); }

}  // namespace

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  delete[] reply;
  return ret;
}
