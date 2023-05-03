// Concord
//
// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <optional>
#include <functional>
#include <bitset>

#include "bftengine/Replica.hpp"
#include "messages/StateTransferMsg.hpp"
#include "FullNodeReplica.hpp"

#include "log/logger.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "messages/CheckpointMsg.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "util/kvstream.h"
#include "PersistentStorage.hpp"
#include "MsgsCommunicator.hpp"
#include "SigManager.hpp"
#include "ReconfigurationCmd.hpp"
#include "util/json_output.hpp"
#include "SharedTypes.hpp"
#include "communication/StateControl.hpp"

using concordUtil::Timers;
using namespace std::placeholders;

// Note : The changes in files are inclined with RO replica SateTransfer behavior, all the class functions are inherited
// from ReadOnlyReplica. As we know for timebeing StateTransfer functionality is a temporary solution for FullNode,
// until the ASP/BSP is implemented the functions in this class needs to be changed based on the required accordingly.

namespace bftEngine::impl {

FullNodeReplica::FullNodeReplica(const ReplicaConfig &config,
                                 std::shared_ptr<IRequestsHandler> requests_handler,
                                 IStateTransfer *state_transfer,
                                 std::shared_ptr<MsgsCommunicator> msg_comm,
                                 std::shared_ptr<MsgHandlersRegistrator> msg_handler_reg,
                                 concordUtil::Timers &timers,
                                 MetadataStorage *metadata_storage)
    : ReplicaForStateTransfer(config, requests_handler, state_transfer, msg_comm, msg_handler_reg, true, timers),
      fn_metrics_{metrics_.RegisterCounter("receivedCheckpointMsgs"),
                  metrics_.RegisterCounter("sentAskForCheckpointMsgs"),
                  metrics_.RegisterCounter("receivedInvalidMsgs"),
                  metrics_.RegisterGauge("lastExecutedSeqNum", lastExecutedSeqNum)},
      metadata_storage_{metadata_storage} {
  LOG_INFO(GL, "Initialising Full Node Replica");
  repsInfo = new ReplicasInfo(config, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);

  registerMsgHandlers();
  metrics_.Register();

  SigManager::init(config_.replicaId,
                   config_.replicaPrivateKey,
                   config_.publicKeysOfReplicas,
                   concord::crypto::KeyFormat::HexaDecimalStrippedFormat,
                   ReplicaConfig::instance().getPublicKeysOfClients(),
                   concord::crypto::KeyFormat::PemFormat,
                   {{repsInfo->getIdOfOperator(),
                     ReplicaConfig::instance().getOperatorPublicKey(),
                     concord::crypto::KeyFormat::PemFormat}},
                   *repsInfo);

  // Register status handler for Full Node replica
  registerStatusHandlers();
  bft::communication::StateControl::instance().setGetPeerPubKeyMethod(
      [&](uint32_t id) { return SigManager::instance()->getPublicKeyOfVerifier(id); });
}

void FullNodeReplica::start() {
  ReplicaForStateTransfer::start();
  size_t sendAskForCheckpointMsgPeriodSec = config_.get("concord.bft.ro.sendAskForCheckpointMsgPeriodSec", 30);
  askForCheckpointMsgTimer_ = timers_.add(
      std::chrono::seconds(sendAskForCheckpointMsgPeriodSec), Timers::Timer::RECURRING, [this](Timers::Handle) {
        if (!this->isCollectingState()) {
          sendAskForCheckpointMsg();
        }
      });
  msgsCommunicator_->startMsgsProcessing(config_.replicaId);
}

void FullNodeReplica::stop() {
  timers_.cancel(askForCheckpointMsgTimer_);
  ReplicaForStateTransfer::stop();
}

void FullNodeReplica::onTransferringCompleteImp(uint64_t newStateCheckpoint) {
  last_executed_seq_num_ = newStateCheckpoint * checkpointWindowSize;
  fn_metrics_.last_executed_seq_num_.Get().Set(last_executed_seq_num_);
}

void FullNodeReplica::onReportAboutInvalidMessage(MessageBase *msg, const char *reason) {
  fn_metrics_.received_invalid_msg_++;
  LOG_WARN(GL,
           "Node " << config_.replicaId << " received invalid message from Node " << msg->senderId()
                   << " type=" << msg->type() << " reason: " << reason);
}
void FullNodeReplica::sendAskForCheckpointMsg() {
  fn_metrics_.sent_ask_for_checkpoint_msg_++;
  LOG_INFO(GL, "sending AskForCheckpointMsg");
  AskForCheckpointMsg msg{config_.replicaId};
  for (auto id : repsInfo->idsOfPeerReplicas()) send(&msg, id);
}

template <>
void FullNodeReplica::onMessage<StateTransferMsg>(std::unique_ptr<StateTransferMsg> msg) {
  ReplicaForStateTransfer::onMessage(move(msg));
}

template <>
void FullNodeReplica::onMessage<CheckpointMsg>(std::unique_ptr<CheckpointMsg> msg) {
  if (isCollectingState()) {
    return;
  }
  fn_metrics_.received_checkpoint_msg_++;
  LOG_INFO(GL,
           KVLOG(msg->senderId(),
                 msg->idOfGeneratedReplica(),
                 msg->seqNumber(),
                 msg->epochNumber(),
                 msg->size(),
                 msg->isStableState(),
                 msg->state(),
                 msg->stateDigest(),
                 msg->reservedPagesDigest(),
                 msg->rvbDataDigest()));

  // Reconfiguration cmd block is synced to RO replica via reserved pages
  EpochNum replicas_last_known_epoch_val = 0;
  auto epoch_number_from_res_pages = ReconfigurationCmd::instance().getReconfigurationCommandEpochNumber();
  if (epoch_number_from_res_pages.has_value()) replicas_last_known_epoch_val = epoch_number_from_res_pages.value();

  // not relevant
  if (!msg->isStableState() || msg->seqNumber() <= lastExecutedSeqNum ||
      msg->epochNumber() < replicas_last_known_epoch_val) {
    return;
  }
  // no self certificate
  static std::map<SeqNum, CheckpointInfo<false>> checkpoints_info;
  const auto msg_seq_num = msg->seqNumber();
  const auto id_of_generated_eplica = msg->idOfGeneratedReplica();
  checkpoints_info[msg_seq_num].addCheckpointMsg(msg.release(), id_of_generated_eplica);
  // if enough - invoke state transfer
  if (checkpoints_info[msg_seq_num].isCheckpointCertificateComplete()) {
    persistCheckpointDescriptor(msg_seq_num, checkpoints_info[msg_seq_num]);
    checkpoints_info.clear();
    LOG_INFO(GL, "call to startCollectingState()");
    stateTransfer->startCollectingState();
  }
}

void FullNodeReplica::persistCheckpointDescriptor(const SeqNum &seqnum, const CheckpointInfo<false> &chckpinfo) {
  std::vector<CheckpointMsg *> msgs;
  msgs.reserve(chckpinfo.getAllCheckpointMsgs().size());
  for (const auto &m : chckpinfo.getAllCheckpointMsgs()) {
    msgs.push_back(m.second);
    LOG_INFO(GL,
             KVLOG(m.second->seqNumber(),
                   m.second->epochNumber(),
                   m.second->state(),
                   m.second->stateDigest(),
                   m.second->reservedPagesDigest(),
                   m.second->rvbDataDigest(),
                   m.second->idOfGeneratedReplica()));
  }
  DescriptorOfLastStableCheckpoint desc(ReplicaConfig::instance().getnumReplicas(), msgs);
  const size_t buf_len = DescriptorOfLastStableCheckpoint::maxSize(ReplicaConfig::instance().getnumReplicas());
  concord::serialize::UniquePtrToChar desc_buf(new char[buf_len]);
  char *desc_buf_ptr = desc_buf.get();
  size_t actual_size = 0;
  desc.serialize(desc_buf_ptr, buf_len, actual_size);
  ConcordAssertNE(actual_size, 0);

  // TODO [TK] S3KeyGenerator
  // checkpoints/<BlockId>/<RepId>
  std::ostringstream oss;
  oss << "checkpoints/" << msgs[0]->state() << "/" << config_.replicaId;
  metadata_storage_->atomicWriteArbitraryObject(oss.str(), desc_buf.get(), actual_size);
}

template <>
void FullNodeReplica::onMessage<ClientRequestMsg>(std::unique_ptr<ClientRequestMsg> msg) {
  const NodeIdType sender_id = msg->senderId();
  const NodeIdType client_id = msg->clientProxyId();
  const ReqId req_seq_num = msg->requestSeqNum();
  const uint64_t flags = msg->flags();

  SCOPED_MDC_CID(msg->getCid());
  LOG_DEBUG(CNSUS, KVLOG(client_id, req_seq_num, sender_id) << " flags: " << std::bitset<sizeof(uint64_t) * 8>(flags));

  const auto &span_context = msg->spanContext<std::remove_pointer<ClientRequestMsg>::type>();
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
  span.setTag("rid", config_.getreplicaId());
  span.setTag("cid", msg->getCid());
  span.setTag("seq_num", req_seq_num);

  // TODO: handle reconfiguration request here, refer ReadOnlyReplica class
}

void FullNodeReplica::registerStatusHandlers() {
  auto h = concord::diagnostics::StatusHandler(
      "replica-sequence-numbers", "Last executed sequence number of the full node replica", [this]() {
        concordUtils::BuildJson bj;

        bj.startJson();
        bj.startNested("sequenceNumbers");
        bj.addKv("lastExecutedSeqNum", last_executed_seq_num_);
        bj.endNested();
        bj.endJson();

        return bj.getJson();
      });
  concord::diagnostics::RegistrarSingleton::getInstance().status.registerHandler(h);
}

void FullNodeReplica::registerMsgHandlers() {
  msgHandlers_->registerMsgHandler(MsgCode::Checkpoint,
                                   std::bind(&FullNodeReplica::messageHandler<CheckpointMsg>, this, _1));
  msgHandlers_->registerMsgHandler(MsgCode::ClientRequest,
                                   std::bind(&FullNodeReplica::messageHandler<ClientRequestMsg>, this, _1));
  msgHandlers_->registerMsgHandler(MsgCode::StateTransfer,
                                   std::bind(&FullNodeReplica::messageHandler<StateTransferMsg>, this, _1));
}

}  // namespace bftEngine::impl
