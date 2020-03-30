#include <messages/CheckpointMsg.hpp>
#include "gtest/gtest.h"
#include "SimpleClientImp.cpp"
#include "messages/MsgsCertificate.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "CheckpointInfo.hpp"
#include "Digest.hpp"

using namespace bftEngine;

namespace {

    const uint16_t fval = 1;
    const uint16_t cval = 1;
    const uint16_t numOfReplicas = 3 * fval + 2 * cval + 1;
    const uint16_t numOfRequired = 2 * fval + cval + 1;
    const uint16_t selfReplicaId = 0;
    //char reply[] = "1234567890";
    std::string reply{""};
    int replyLen = reply.length();

    ReplicaId randomReplicaeId() {
        return rand() % numOfReplicas;
    }

    class mockCheckpointMsgCmp : public CheckpointInfo{
    public:
        static bool equivalent(CheckpointMsg *a, CheckpointMsg *b) {
            return CheckpointInfo::CheckpointMsgCmp::equivalent(a,b);
        }
    };

    TEST(msgsCertificate_test, obtain_inconsistent_state){

        MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate(
                numOfReplicas, fval, numOfRequired, selfReplicaId);

        uint8_t reqSeqNum = 0;

        // Add bunch of new messages
        for (; reqSeqNum < 3; reqSeqNum++) {
            ReplicaId replicaId = randomReplicaeId();
            auto newMsg = new ClientReplyMsg(replicaId, reqSeqNum, reply, replyLen);
            replysCertificate.addMsg(newMsg, replicaId);
        }

        ASSERT_TRUE(replysCertificate.isInconsistent());

        replysCertificate.resetAndFree();


    }


    TEST(msgsCertificate_test, self_msg_added_peer_msg_ignored) {

        MsgsCertificate<CheckpointMsg, true, true, true, mockCheckpointMsgCmp> replysCertificate(
                numOfReplicas, fval, numOfRequired, selfReplicaId);

        auto selfMsg = new CheckpointMsg(selfReplicaId, 0, Digest(), false);
        replysCertificate.addMsg(selfMsg, selfReplicaId);

        ASSERT_TRUE(replysCertificate.selfMsg() != nullptr);

        replysCertificate.resetAndFree();

    }


    TEST(msgsCertificate_test, exceeded_required_msgs_twice) {

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


    TEST(msgsCertificate_test, certifiy_best_correct_msg) {

        MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate(
                numOfReplicas, fval, numOfRequired, selfReplicaId);

        uint8_t reqSeqNum = 0;

        // Create two identical messages and add them.

        ReplicaId replicaId = randomReplicaeId();
        auto bestMsg = new ClientReplyMsg(replicaId, reqSeqNum, reply, replyLen);
        replysCertificate.addMsg(bestMsg, randomReplicaeId());

        replicaId = randomReplicaeId();
        auto identBestMsg = new ClientReplyMsg(replicaId, reqSeqNum, reply, replyLen);
        replysCertificate.addMsg(identBestMsg, randomReplicaeId());

        // Add bunch of new messages
        for (; reqSeqNum < 4; reqSeqNum++) {
            replicaId = randomReplicaeId();
            auto newMsg = new ClientReplyMsg(replicaId, reqSeqNum, reply, replyLen);
            replysCertificate.addMsg(newMsg, replicaId);
        }

        ASSERT_TRUE(SimpleClientImp::equivalent(replysCertificate.bestCorrectMsg(), bestMsg));

        replysCertificate.resetAndFree();
    }

    TEST(msgsCertificate_test, delete_extra_msg) {

        MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate(
                numOfReplicas, fval, numOfRequired, selfReplicaId);

        bool isMsgAdded = false;
        NodeIdType replicaId = randomReplicaeId();

        for (int i = 0; i < 2; i++) {
            auto msg = new ClientReplyMsg(replicaId, i, reply, replyLen);
            isMsgAdded = replysCertificate.addMsg(msg, replicaId);
        }

        ASSERT_FALSE(isMsgAdded);

        replysCertificate.resetAndFree();
    }
}


int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}