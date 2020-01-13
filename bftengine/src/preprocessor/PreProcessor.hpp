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

#pragma once

#include "MsgsCommunicator.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "InternalReplicaApi.hpp"
#include "messages/ClientPreProcessRequestMsg.hpp"
#include "SimpleThreadPool.hpp"
#include "Replica.hpp"
#include "RequestProcessingInfo.hpp"

namespace preprocessor {

class AsyncPreProcessJob;

//**************** Class PreProcessor ****************//

// This class is responsible for the coordination of pre-execution activities on both - primary and non-primary
// replica types. It handles client pre-execution requests, pre-processing requests, and replies.
// On primary replica - it collects pre-execution result hashes from other replicas and decides whether to continue
// or to fail request processing.

class PreProcessor {
 public:
  PreProcessor(std::shared_ptr<MsgsCommunicator> &msgsCommunicator,
               std::shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
               std::shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
               bftEngine::IRequestsHandler &requestsHandler,
               const InternalReplicaApi &replica);

  ~PreProcessor();

  static void addNewPreProcessor(std::shared_ptr<MsgsCommunicator> &msgsCommunicator,
                                 std::shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                                 std::shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                                 bftEngine::IRequestsHandler &requestsHandler,
                                 InternalReplicaApi &replica);

 private:
  friend class AsyncPreProcessJob;

  ClientPreProcessReqMsgSharedPtr convertMsgToCorrectType(MessageBase *&inMsg);
  void onClientPreProcessRequestMsg(MessageBase *msg);
  void onPreProcessRequestMsg(MessageBase *msg);
  void onPreProcessReplyMsg(MessageBase *msg);
  void registerMsgHandlers();
  bool checkClientMsgCorrectness(const ClientPreProcessReqMsgSharedPtr &msg, ReqId reqSeqNum) const;
  void handleClientPreProcessRequest(const ClientPreProcessReqMsgSharedPtr &clientReqMsg);
  void sendMsg(char *msg, NodeIdType dest, uint16_t msgType, MsgSize msgSize);
  void sendPreProcessRequestToAllReplicas(const ClientPreProcessReqMsgSharedPtr &clientPreProcessRequestMsg);
  uint16_t getClientReplyBufferId(uint16_t clientId) const { return clientId - numOfReplicas_; }

 private:
  static std::vector<std::unique_ptr<PreProcessor>> preProcessors_;  // The place holder for PreProcessor objects

  std::shared_ptr<MsgsCommunicator> msgsCommunicator_;
  std::shared_ptr<IncomingMsgsStorage> incomingMsgsStorage_;
  std::shared_ptr<MsgHandlersRegistrator> msgHandlersRegistrator_;
  bftEngine::IRequestsHandler &requestsHandler_;
  const InternalReplicaApi &replica_;
  const ReplicaId replicaId_;
  const uint32_t maxReplyMsgSize_;
  const std::set<ReplicaId> &idsOfPeerReplicas_;
  const uint16_t numOfReplicas_;
  const uint16_t numOfClients_;
  util::SimpleThreadPool threadPool_;
  // One-time allocated buffers (one per client) for the pre-execution results storage
  std::vector<concordUtils::Sliver> preProcessResultBuffers_;
  // clientId -> *RequestProcessingInfo
  std::unordered_map<uint16_t, std::unique_ptr<RequestProcessingInfo>> ongoingRequests_;
  std::mutex ongoingRequestsMutex_;
};

//**************** Class AsyncPreProcessJob ****************//

// This class is used to send messages to other replicas in parallel

class AsyncPreProcessJob : public util::SimpleThreadPool::Job {
 public:
  AsyncPreProcessJob(PreProcessor &preProcessor, std::shared_ptr<MessageBase> msg, ReplicaId replicaId);
  virtual ~AsyncPreProcessJob() = default;

  void execute() override;
  void release() override;

 private:
  PreProcessor &preProcessor_;
  std::shared_ptr<MessageBase> msg_;
  ReplicaId destId_ = 0;
};

}  // namespace preprocessor
