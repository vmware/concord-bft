// Concord
//
// Copyright (c) 2018, 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <bftengine/Replica.hpp>
#include <optional>
#include <functional>
#include <bitset>
#include <messages/StateTransferMsg.hpp>
#include "ReadOnlyReplica.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "messages/CheckpointMsg.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "Logger.hpp"
#include "kvstream.h"
#include "PersistentStorage.hpp"
#include "MsgsCommunicator.hpp"
#include "SigManager.hpp"
#include "ReconfigurationCmd.hpp"
#include "json_output.hpp"
#include "SharedTypes.hpp"
#include "communication/StateControl.hpp"

using concordUtil::Timers;

namespace bftEngine::impl {

ReadOnlyReplica::ReadOnlyReplica(const ReplicaConfig &config,
                                 std::shared_ptr<IRequestsHandler> requestsHandler,
                                 IStateTransfer *stateTransfer,
                                 std::shared_ptr<MsgsCommunicator> msgComm,
                                 std::shared_ptr<MsgHandlersRegistrator> msgHandlerReg,
                                 concordUtil::Timers &timers,
                                 MetadataStorage *metadataStorage)
    : ReplicaForStateTransfer(config, requestsHandler, stateTransfer, msgComm, msgHandlerReg, true, timers),
      ro_metrics_{metrics_.RegisterCounter("receivedCheckpointMsgs"),
                  metrics_.RegisterCounter("sentAskForCheckpointMsgs"),
                  metrics_.RegisterCounter("receivedInvalidMsgs"),
                  metrics_.RegisterGauge("lastExecutedSeqNum", lastExecutedSeqNum)},
      metadataStorage_{metadataStorage} {
  LOG_INFO(GL, "Initialising ReadOnly Replica");
  repsInfo = new ReplicasInfo(config, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);
  msgHandlers_->registerMsgHandler(
      MsgCode::Checkpoint, std::bind(&ReadOnlyReplica::messageHandler<CheckpointMsg>, this, std::placeholders::_1));
  msgHandlers_->registerMsgHandler(
      MsgCode::ClientRequest,
      std::bind(&ReadOnlyReplica::messageHandler<ClientRequestMsg>, this, std::placeholders::_1));
  msgHandlers_->registerMsgHandler(
      MsgCode::StateTransfer,
      std::bind(&ReadOnlyReplica::messageHandler<StateTransferMsg>, this, std::placeholders::_1));
  metrics_.Register();

  SigManager::init(config_.replicaId,
                   config_.replicaPrivateKey,
                   config_.publicKeysOfReplicas,
                   concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                   ReplicaConfig::instance().getPublicKeysOfClients(),
                   concord::util::crypto::KeyFormat::PemFormat,
                   *repsInfo);

  // Register status handler for Read-Only replica
  registerStatusHandlers();
  bft::communication::StateControl::instance().setGetPeerPubKeyMethod(
      [&](uint32_t id) { return SigManager::instance()->getPublicKeyOfVerifier(id); });
}

void ReadOnlyReplica::start() {
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

void ReadOnlyReplica::stop() {
  timers_.cancel(askForCheckpointMsgTimer_);
  ReplicaForStateTransfer::stop();
}

void ReadOnlyReplica::onTransferringCompleteImp(uint64_t newStateCheckpoint) {
  lastExecutedSeqNum = newStateCheckpoint * checkpointWindowSize;

  ro_metrics_.last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
  last_executed_seq_num_ = lastExecutedSeqNum;
}

void ReadOnlyReplica::onReportAboutInvalidMessage(MessageBase *msg, const char *reason) {
  ro_metrics_.received_invalid_msg_++;
  LOG_WARN(GL,
           "Node " << config_.replicaId << " received invalid message from Node " << msg->senderId()
                   << " type=" << msg->type() << " reason: " << reason);
}
void ReadOnlyReplica::sendAskForCheckpointMsg() {
  ro_metrics_.sent_ask_for_checkpoint_msg_++;
  LOG_INFO(GL, "sending AskForCheckpointMsg");
  auto msg = std::make_unique<AskForCheckpointMsg>(config_.replicaId);
  for (auto id : repsInfo->idsOfPeerReplicas()) send(msg.get(), id);
}

template <>
void ReadOnlyReplica::onMessage<StateTransferMsg>(StateTransferMsg *msg) {
  ReplicaForStateTransfer::onMessage(msg);
}

template <>
void ReadOnlyReplica::onMessage<CheckpointMsg>(CheckpointMsg *msg) {
  if (isCollectingState()) {
    delete msg;
    return;
  }
  ro_metrics_.received_checkpoint_msg_++;
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
  EpochNum replicasLastKnownEpochVal = 0;
  auto epochNumberFromResPages = ReconfigurationCmd::instance().getReconfigurationCommandEpochNumber();
  if (epochNumberFromResPages.has_value()) replicasLastKnownEpochVal = epochNumberFromResPages.value();

  // not relevant
  if (!msg->isStableState() || msg->seqNumber() <= lastExecutedSeqNum ||
      msg->epochNumber() < replicasLastKnownEpochVal) {
    delete msg;
    return;
  }
  // no self certificate
  static std::map<SeqNum, CheckpointInfo<false>> checkpointsInfo;
  checkpointsInfo[msg->seqNumber()].addCheckpointMsg(msg, msg->idOfGeneratedReplica());
  // if enough - invoke state transfer
  if (checkpointsInfo[msg->seqNumber()].isCheckpointCertificateComplete()) {
    persistCheckpointDescriptor(msg->seqNumber(), checkpointsInfo[msg->seqNumber()]);
    checkpointsInfo.clear();
    LOG_INFO(GL, "call to startCollectingState()");
    stateTransfer->startCollectingState();
  }
}

void ReadOnlyReplica::persistCheckpointDescriptor(const SeqNum &seqnum, const CheckpointInfo<false> &chckpinfo) {
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
  const size_t bufLen = DescriptorOfLastStableCheckpoint::maxSize(ReplicaConfig::instance().getnumReplicas());
  concord::serialize::UniquePtrToChar descBuf(new char[bufLen]);
  char *descBufPtr = descBuf.get();
  size_t actualSize = 0;
  desc.serialize(descBufPtr, bufLen, actualSize);
  ConcordAssertNE(actualSize, 0);

  // TODO [TK] S3KeyGenerator
  // checkpoints/<BlockId>/<RepId>
  std::ostringstream oss;
  oss << "checkpoints/" << msgs[0]->state() << "/" << config_.replicaId;
  metadataStorage_->atomicWriteArbitraryObject(oss.str(), descBuf.get(), actualSize);
}

template <>
void ReadOnlyReplica::onMessage<ClientRequestMsg>(ClientRequestMsg *m) {
  const NodeIdType senderId = m->senderId();
  const NodeIdType clientId = m->clientProxyId();
  const bool reconfig_flag = (m->flags() & MsgFlag::RECONFIG_FLAG) != 0;
  const ReqId reqSeqNum = m->requestSeqNum();
  const uint64_t flags = m->flags();

  SCOPED_MDC_CID(m->getCid());
  LOG_DEBUG(CNSUS, KVLOG(clientId, reqSeqNum, senderId) << " flags: " << std::bitset<sizeof(uint64_t) * 8>(flags));

  const auto &span_context = m->spanContext<std::remove_pointer<ClientRequestMsg>::type>();
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
  span.setTag("rid", config_.getreplicaId());
  span.setTag("cid", m->getCid());
  span.setTag("seq_num", reqSeqNum);

  // A read only replica can handle only reconfiguration requests. Those requests are signed by the operator and
  // the validation is done in the reconfiguration engine. Thus, we don't need to check the client validity as in
  // the committers

  if (reconfig_flag) {
    LOG_INFO(GL, "ro replica has received a reconfiguration request");
    executeReadOnlyRequest(span, m);
    delete m;
    return;
  }

  delete m;
}

void ReadOnlyReplica::executeReadOnlyRequest(concordUtils::SpanWrapper &parent_span, const ClientRequestMsg &request) {
  auto span = concordUtils::startChildSpan("bft_execute_read_only_request", parent_span);
  // Read only replica does not know who is the primary, so it always return 0. It is the client responsibility to treat
  // the replies accordingly.
  ClientReplyMsg reply(0, request.requestSeqNum(), config_.getreplicaId());

  const uint16_t clientId = request.clientProxyId();

  int executionResult = 0;
  bftEngine::IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  accumulatedRequests.push_back(bftEngine::IRequestsHandler::ExecutionRequest{clientId,
                                                                              static_cast<uint64_t>(lastExecutedSeqNum),
                                                                              request.getCid(),
                                                                              request.flags(),
                                                                              request.requestLength(),
                                                                              request.requestBuf(),
                                                                              "",
                                                                              reply.maxReplyLength(),
                                                                              reply.replyBuf(),
                                                                              request.requestSeqNum(),
                                                                              request.result()});

  // DD: Do we need to take care of Time Service here?
  bftRequestsHandler_->execute(accumulatedRequests, std::nullopt, request.getCid(), span);
  IRequestsHandler::ExecutionRequest &single_request = accumulatedRequests.back();
  executionResult = single_request.outExecutionStatus;
  const uint32_t actualReplyLength = single_request.outActualReplySize;
  const uint32_t actualReplicaSpecificInfoLength = single_request.outReplicaSpecificInfoSize;
  LOG_DEBUG(GL,
            "Executed read only request. " << KVLOG(clientId,
                                                    lastExecutedSeqNum,
                                                    request.requestLength(),
                                                    reply.maxReplyLength(),
                                                    actualReplyLength,
                                                    actualReplicaSpecificInfoLength,
                                                    executionResult));
  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)
  if (!executionResult) {
    if (actualReplyLength > 0) {
      reply.setReplyLength(actualReplyLength);
      reply.setReplicaSpecificInfoLength(actualReplicaSpecificInfoLength);
      send(&reply, clientId);
      return;
    } else {
      LOG_WARN(GL, "Received zero size response. " << KVLOG(clientId));
      strcpy(single_request.outReply, "Executed data is empty");
      single_request.outActualReplySize = strlen(single_request.outReply);
      executionResult = static_cast<uint32_t>(bftEngine::OperationResult::EXEC_DATA_EMPTY);
    }

  } else {
    LOG_ERROR(GL, "Received error while executing RO request. " << KVLOG(clientId, executionResult));
  }
  ClientReplyMsg replyMsg(
      0, request.requestSeqNum(), single_request.outReply, single_request.outActualReplySize, executionResult);
  send(&replyMsg, clientId);
}

void ReadOnlyReplica::registerStatusHandlers() {
  auto h = concord::diagnostics::StatusHandler(
      "replica", "Last executed sequence number of the read-only replica", [this]() {
        concordUtils::BuildJson bj;

        bj.startJson();
        bj.startNested("sequenceNumbers");
        bj.addKv("lastExecutedSeqNum", last_executed_seq_num_);
        bj.endNested();
        bj.endJson();

        char *cstr = new char[bj.getJson().length() + 1];
        std::strcpy(cstr, bj.getJson().c_str());
        return cstr;
      });
  concord::diagnostics::RegistrarSingleton::getInstance().status.registerHandler(h);
}

}  // namespace bftEngine::impl
