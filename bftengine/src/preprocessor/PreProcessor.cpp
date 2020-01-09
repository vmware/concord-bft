// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "PreProcessor.hpp"
#include "Logger.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "messages/ClientPreProcessRequestMsg.hpp"
#include "messages/PreProcessRequestMsg.hpp"
#include "ReplicaConfig.hpp"

namespace preprocessor {

using namespace bftEngine;
using namespace concordUtils;
using namespace std;
using namespace std::placeholders;

//**************** Class PreProcessor ****************//

vector<unique_ptr<PreProcessor>> PreProcessor::preProcessors_;

void PreProcessor::registerMsgHandlers() {
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::ClientPreProcessRequest,
                                              bind(&PreProcessor::onClientPreProcessRequestMsg, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessRequest,
                                              bind(&PreProcessor::onPreProcessRequestMsg, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessReply,
                                              bind(&PreProcessor::onPreProcessReplyMsg, this, _1));
}

PreProcessor::PreProcessor(shared_ptr<MsgsCommunicator> &msgsCommunicator,
                           shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                           shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                           RequestsHandler &requestsHandler,
                           const InternalReplicaApi &replica)
    : requestsHandler_(requestsHandler),
      replica_(replica),
      replicaId_(ReplicaConfigSingleton::GetInstance().GetReplicaId()),
      maxReplyMsgSize_(ReplicaConfigSingleton::GetInstance().GetMaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)),
      idsOfPeerReplicas_(replica.getIdsOfPeerReplicas()),
      numOfReplicas_(ReplicaConfigSingleton::GetInstance().GetNumOfReplicas()),
      numOfClients_(ReplicaConfigSingleton::GetInstance().GetNumOfClientProxies()) {
  msgsCommunicator_ = msgsCommunicator;
  incomingMsgsStorage_ = incomingMsgsStorage;
  msgHandlersRegistrator_ = msgHandlersRegistrator;
  registerMsgHandlers();
  threadPool_.start(numOfClients_);

  // Allocate a buffer for the pre-execution result per client
  for (auto id = 0; id < numOfClients_; id++)
    preProcessResultBuffers_.push_back(Sliver(new char[maxReplyMsgSize_], maxReplyMsgSize_));
}

PreProcessor::~PreProcessor() { threadPool_.stop(); }

ClientPreProcessReqMsgSharedPtr PreProcessor::convertMsgToCorrectType(MessageBase *&inMsg) {
  ClientPreProcessRequestMsg *outMsg = nullptr;
  if (!ClientPreProcessRequestMsg::ToActualMsgType(inMsg, outMsg)) {
    LOG_WARN(GL,
             "Replica " << replicaId_ << " received invalid message "
                        << " type=" << inMsg->type() << " from Node=" << inMsg->senderId());
    delete inMsg;
  }
  ClientPreProcessReqMsgSharedPtr resultMsg(outMsg);
  return resultMsg;
}

bool PreProcessor::checkClientMsgCorrectness(const ClientPreProcessReqMsgSharedPtr &msg, ReqId reqSeqNum) const {
  if (replica_.isCollectingState()) {
    LOG_INFO(GL,
             "ClientPreProcessRequestMsg reqSeqNum="
                 << reqSeqNum << " is ignored because the replica is collecting missing state from other replicas");
    return false;
  }
  if (msg->isReadOnly()) {
    LOG_INFO(GL,
             "ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum << " is ignored because it is signed as read-only");
    return false;
  }
  const bool &invalidClient = !replica_.isValidClient(msg->clientProxyId());
  const bool &sentFromReplicaToNonPrimary = replica_.isIdOfReplica(msg->senderId()) && !replica_.isCurrentPrimary();
  if (invalidClient || sentFromReplicaToNonPrimary) {
    LOG_INFO(GL,
             "ClientPreProcessRequestMsg reqSeqNum="
                 << reqSeqNum << " is ignored as invalid: invalidClient=" << invalidClient
                 << ", sentFromReplicaToNonPrimary=" << sentFromReplicaToNonPrimary);
    return false;
  }
  if (!replica_.currentViewIsActive()) {
    LOG_INFO(GL, "ClientPreProcessRequestMsg is ignored because current view is inactive, reqSeqNum=" << reqSeqNum);
    return false;
  }
  return true;
}

void PreProcessor::onClientPreProcessRequestMsg(MessageBase *msg) {
  auto clientPreProcessReqMsg = convertMsgToCorrectType(msg);
  if (!clientPreProcessReqMsg) return;

  const NodeIdType &senderId = clientPreProcessReqMsg->senderId();
  const NodeIdType &clientId = clientPreProcessReqMsg->clientProxyId();
  const ReqId &reqSeqNum = clientPreProcessReqMsg->requestSeqNum();
  LOG_INFO(GL,
           "Replica " << replicaId_ << " received ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum
                      << " from clientId=" << clientId << ", senderId=" << senderId);
  if (!checkClientMsgCorrectness(clientPreProcessReqMsg, reqSeqNum)) return;
  {
    lock_guard<mutex> lock(ongoingRequestsMutex_);
    if (ongoingRequests_.find(clientId) != ongoingRequests_.end()) {
      LOG_WARN(GL,
               "ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum << " for client=" << clientId
                                                       << " is ignored because previous client request is in process");
      return;
    }
  }
  const ReqId &seqNumberOfLastReply = replica_.seqNumberOfLastReplyToClient(clientId);
  if (seqNumberOfLastReply < reqSeqNum) {
    if (replica_.isCurrentPrimary()) {
      return handleClientPreProcessRequest(clientPreProcessReqMsg);
    } else {  // Not the current primary => pass it to the primary
      sendMsg(msg->body(), replica_.currentPrimary(), msg->type(), msg->size());
      LOG_INFO(GL, "Sending ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum << " to current primary");
      return;
    }
  }
  if (seqNumberOfLastReply == reqSeqNum) {
    LOG_INFO(GL,
             "ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum
                                                     << " has already been executed - let replica complete handling");
    return incomingMsgsStorage_->pushExternalMsg(clientPreProcessReqMsg->convertToClientRequestMsg());
  }
  LOG_INFO(GL, "ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum << " is ignored because request is old");
}

void PreProcessor::sendMsg(char *msg, NodeIdType dest, uint16_t msgType, MsgSize msgSize) {
  int errorCode = msgsCommunicator_->sendAsyncMessage(dest, msg, msgSize);
  if (errorCode != 0) {
    LOG_ERROR(GL, "sendMsg: sendAsyncMessage returned error=" << errorCode << " for message type=" << msgType);
  }
}

void PreProcessor::sendPreProcessRequestToAllReplicas(
    const ClientPreProcessReqMsgSharedPtr &clientPreProcessRequestMsg) {
  auto preProcessRequestMsg = make_shared<PreProcessRequestMsg>(clientPreProcessRequestMsg, replica_.getCurrentView());
  const set<ReplicaId> &idsOfPeerReplicas = replica_.getIdsOfPeerReplicas();
  for (auto destId : idsOfPeerReplicas) {
    auto *preProcessJob = new AsyncPreProcessJob(*this, preProcessRequestMsg, destId);
    LOG_INFO(GL,
             "PreProcessRequestMsg with reqSeqNum=" << clientPreProcessRequestMsg->requestSeqNum()
                                                    << " from client= " << clientPreProcessRequestMsg->senderId()
                                                    << " sent to replica=" << destId);
    threadPool_.add(preProcessJob);
  }
}

void PreProcessor::handleClientPreProcessRequest(const ClientPreProcessReqMsgSharedPtr &clientPreProcessRequestMsg) {
  const uint16_t &clientId = clientPreProcessRequestMsg->clientProxyId();
  const ReqId &requestSeqNum = clientPreProcessRequestMsg->requestSeqNum();
  {
    lock_guard<mutex> lock(ongoingRequestsMutex_);
    // Only one request is supported per client for now
    ongoingRequests_[clientId] = make_unique<RequestProcessingInfo>(numOfReplicas_, requestSeqNum);
    ongoingRequests_[clientId]->saveClientPreProcessRequestMsg(clientPreProcessRequestMsg);
  }
  sendPreProcessRequestToAllReplicas(clientPreProcessRequestMsg);

  // Pre-process the request and calculate a hash of the result
  uint32_t actualResultBufLen = 0;
  requestsHandler_.execute(
      clientId,
      requestSeqNum,  // The sequence number is required to add a block, which is irrelevant in this flow
      PRE_PROCESS_FLAG,
      clientPreProcessRequestMsg->requestLength(),
      clientPreProcessRequestMsg->requestBuf(),
      maxReplyMsgSize_,
      (char *)preProcessResultBuffers_[getClientReplyBufferId(clientId)].data(),
      actualResultBufLen);

  if (!actualResultBufLen)
    throw std::runtime_error("actualResultLength is 0 for clientId=" + to_string(clientId) +
                             ", requestSeqNum=" + to_string(requestSeqNum));
  ongoingRequests_[clientId]->savePreProcessResult(preProcessResultBuffers_[getClientReplyBufferId(clientId)],
                                                   actualResultBufLen);
}

void PreProcessor::onPreProcessRequestMsg(MessageBase *msg) {}

void PreProcessor::onPreProcessReplyMsg(MessageBase *msg) {}

void PreProcessor::addNewPreProcessor(shared_ptr<MsgsCommunicator> &msgsCommunicator,
                                      shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                                      shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                                      bftEngine::RequestsHandler &requestsHandler,
                                      InternalReplicaApi &replica) {
  preProcessors_.push_back(make_unique<PreProcessor>(
      msgsCommunicator, incomingMsgsStorage, msgHandlersRegistrator, requestsHandler, replica));
}

//**************** Class AsyncPreProcessJob ****************//

AsyncPreProcessJob::AsyncPreProcessJob(PreProcessor &preProcessor, shared_ptr<MessageBase> msg, ReplicaId destId)
    : preProcessor_(preProcessor), msg_(msg), destId_(destId) {}

void AsyncPreProcessJob::execute() { preProcessor_.sendMsg(msg_->body(), destId_, msg_->type(), msg_->size()); }

void AsyncPreProcessJob::release() { delete this; }

}  // namespace preprocessor
