// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <bitset>
#include <limits>
#include <nlohmann/json.hpp>

#include "ReplicaImp.hpp"

#include "log/logger.hpp"
#include "Timers.hpp"
#include "assertUtils.hpp"
#include "ControllerWithSimpleHistory.hpp"
#include "DebugStatistics.hpp"
#include "SysConsts.hpp"
#include "ReplicaConfig.hpp"
#include "MsgsCommunicator.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "ReplicaLoader.hpp"
#include "PersistentStorage.hpp"
#include "OpenTracing.hpp"
#include "diagnostics.h"
#include "TimeUtils.hpp"
#include "json_output.hpp"

#include "messages/ClientRequestMsg.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/CheckpointMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "messages/StartSlowCommitMsg.hpp"
#include "messages/ReqMissingDataMsg.hpp"
#include "messages/SimpleAckMsg.hpp"
#include "messages/ViewChangeMsg.hpp"
#include "messages/NewViewMsg.hpp"
#include "messages/PartialCommitProofMsg.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "messages/ReplicaStatusMsg.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "messages/ReplicaAsksToLeaveViewMsg.hpp"
#include "messages/ReplicaRestartReadyMsg.hpp"
#include "messages/ReplicasRestartReadyProofMsg.hpp"
#include "messages/PreProcessResultMsg.hpp"
#include "messages/ViewChangeIndicatorInternalMsg.hpp"
#include "messages/PrePrepareCarrierInternalMsg.hpp"
#include "messages/ValidatedMessageCarrierInternalMsg.hpp"
#include "messages/StateTransferMsg.hpp"
#include "CryptoManager.hpp"
#include "ControlHandler.hpp"
#include "bftengine/KeyExchangeManager.hpp"
#include "secrets/secrets_manager_plain.h"
#include "bftengine/EpochManager.hpp"
#include "RequestThreadPool.hpp"
#include "DbCheckpointManager.hpp"
#include "communication/StateControl.hpp"

#define getName(var) #var

using concordUtil::Timers;
using namespace std;
using namespace std::chrono;
using namespace concord::diagnostics;

namespace bftEngine::impl {

void ReplicaImp::registerMsgHandlers() {
  msgHandlers_->registerMsgHandler(MsgCode::Checkpoint,
                                   bind(&ReplicaImp::messageHandler<CheckpointMsg>, this, placeholders::_1),
                                   bind(&ReplicaImp::validatedMessageHandler<CheckpointMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::CommitPartial,
      bind(&ReplicaImp::messageHandler<CommitPartialMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<CommitPartialMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(MsgCode::CommitFull,
                                   bind(&ReplicaImp::messageHandler<CommitFullMsg>, this, placeholders::_1),
                                   bind(&ReplicaImp::validatedMessageHandler<CommitFullMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::FullCommitProof,
      bind(&ReplicaImp::messageHandler<FullCommitProofMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<FullCommitProofMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(MsgCode::NewView,
                                   bind(&ReplicaImp::messageHandler<NewViewMsg>, this, placeholders::_1),
                                   bind(&ReplicaImp::validatedMessageHandler<NewViewMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(MsgCode::PrePrepare,
                                   bind(&ReplicaImp::messageHandler<PrePrepareMsg>, this, placeholders::_1),
                                   bind(&ReplicaImp::validatedMessageHandler<PrePrepareMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::PartialCommitProof,
      bind(&ReplicaImp::messageHandler<PartialCommitProofMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<PartialCommitProofMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::PreparePartial,
      bind(&ReplicaImp::messageHandler<PreparePartialMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<PreparePartialMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(MsgCode::PrepareFull,
                                   bind(&ReplicaImp::messageHandler<PrepareFullMsg>, this, placeholders::_1),
                                   bind(&ReplicaImp::validatedMessageHandler<PrepareFullMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::ReqMissingData,
      bind(&ReplicaImp::messageHandler<ReqMissingDataMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<ReqMissingDataMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(MsgCode::SimpleAck,
                                   bind(&ReplicaImp::messageHandler<SimpleAckMsg>, this, placeholders::_1),
                                   bind(&ReplicaImp::validatedMessageHandler<SimpleAckMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::StartSlowCommit,
      bind(&ReplicaImp::messageHandler<StartSlowCommitMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<StartSlowCommitMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(MsgCode::ViewChange,
                                   bind(&ReplicaImp::messageHandler<ViewChangeMsg>, this, placeholders::_1),
                                   bind(&ReplicaImp::validatedMessageHandler<ViewChangeMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::ClientRequest,
      bind(&ReplicaImp::messageHandler<ClientRequestMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<ClientRequestMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::PreProcessResult,
      bind(&ReplicaImp::messageHandler<preprocessor::PreProcessResultMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<preprocessor::PreProcessResultMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::ReplicaStatus,
      bind(&ReplicaImp::messageHandler<ReplicaStatusMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<ReplicaStatusMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::AskForCheckpoint,
      bind(&ReplicaImp::messageHandler<AskForCheckpointMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<AskForCheckpointMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::ReplicaAsksToLeaveView,
      bind(&ReplicaImp::messageHandler<ReplicaAsksToLeaveViewMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<ReplicaAsksToLeaveViewMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::ReplicaRestartReady,
      bind(&ReplicaImp::messageHandler<ReplicaRestartReadyMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<ReplicaRestartReadyMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(
      MsgCode::ReplicasRestartReadyProof,
      bind(&ReplicaImp::messageHandler<ReplicasRestartReadyProofMsg>, this, placeholders::_1),
      bind(&ReplicaImp::validatedMessageHandler<ReplicasRestartReadyProofMsg>, this, placeholders::_1));

  msgHandlers_->registerMsgHandler(MsgCode::StateTransfer,
                                   bind(&ReplicaImp::messageHandler<StateTransferMsg>, this, placeholders::_1));

  msgHandlers_->registerInternalMsgHandler([this](InternalMessage &&msg) { onInternalMsg(std::move(msg)); });
}

template <typename T>
void ReplicaImp::messageHandler(std::unique_ptr<MessageBase> msg) {
  auto trueTypeObj = std::make_unique<T>(msg.get());
  msg.reset();
  if (isCollectingState()) {
    // Extract only required information and handover to ST thread
    if (validateMessage(trueTypeObj.get())) {
      peekConsensusMessage<T>(trueTypeObj.get());
    }
  }
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) {
    if constexpr (!std::is_same_v<T, ClientRequestMsg>) {
      LOG_INFO(GL, "Received protocol message while pruning, ignoring the message");
      return;
    }
  }
  if constexpr (std::is_same_v<T, PrePrepareMsg>) {
    if (getReplicaConfig().prePrepareFinalizeAsyncEnabled) {
      if (!isCollectingState()) {
        validatePrePrepareMsg(std::move(trueTypeObj));
      }
      return;
    }
  }
  if constexpr (std::is_same_v<T, StateTransferMsg>) {
    if (validateMessage(trueTypeObj.get())) {
      onMessage<T>(std::move(trueTypeObj));
    }
    return;
  }
  if (!isCollectingState()) {
    // The message validation of few messages require time-consuming processes like
    // digest calculation, signature verification etc. Such messages are identified
    // by review. During the review we also check if asynchronous message validation
    // is feasible or not for the given message. After identification, true returning
    // shouldValidateAsync() member function is added to these message which indicates
    // that Asynchronous validation is possible for these messages.
    // In this function we check shouldValidateAsync() and decide if asynchronous
    // validation will happen or synchronous validation will happen.
    // prePrepareFinalizeAsyncEnabled flag will be removed in 1.7, when this entire
    // async behaviour will be assumed to be default for the chosen messages.
    if (getReplicaConfig().prePrepareFinalizeAsyncEnabled && trueTypeObj->shouldValidateAsync()) {
      asyncValidateMessage<T>(std::move(trueTypeObj));
      return;
    } else {
      if (validateMessage(trueTypeObj.get())) {
        onMessage<T>(std::move(trueTypeObj));
        return;
      }
    }
  }
}

/**
 * validatedMessageHandler<T> This is a family of validated message callback.
 * As validation is assumed to happen, this function calls the onMessage<T> directly.
 *
 * @param msg : This is the carrier message which holds some message which was originally
 *              external and now validated.
 * @return : returns nothing
 */
template <typename T>
void ReplicaImp::validatedMessageHandler(CarrierMesssage *msg) {
  // The Carrier message is allocated by this replica and after validation its given back
  // to itself without going through the network stack (as an internal message).
  // So the carrier message is quickly reinterpreted and used instead of fresh memory allocation.
  // So this reinterpret_cast is intended.
  ValidatedMessageCarrierInternalMsg<T> *validatedTrueTypeObj =
      reinterpret_cast<ValidatedMessageCarrierInternalMsg<T> *>(msg);
  T *trueTypeObj = validatedTrueTypeObj->returnMessageToOwner();
  delete msg;

  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) {
    if constexpr (!std::is_same_v<T, ClientRequestMsg>) {
      LOG_INFO(GL, "Received protocol message while pruning, ignoring the message");
      delete trueTypeObj;
      return;
    }
  }
  if (!isCollectingState()) {
    onMessage<T>(std::unique_ptr<T>(trueTypeObj));
  } else {
    delete trueTypeObj;
  }
}

/**
 * validateMessage This is synchronous validate message.
 *
 * @param msg : Message that can validate itself as quick as possible.
 * @return : returns true if message validation succeeds else return false.
 */
bool ReplicaImp::validateMessage(MessageBase *msg) {
  if (config_.debugStatisticsEnabled) {
    DebugStatistics::onReceivedExMessage(msg->type());
  }
  try {
    msg->validate(*repsInfo);
    return true;
  } catch (std::exception &e) {
    onReportAboutInvalidMessage(msg, e.what());
    return false;
  }
}

/**
 * asyncValidateMessage<T> This is a family of asynchronous message which just schedules
 * the validation in a thread bag and returns peacefully. This will also translate the message
 * into internal message.
 *
 * @param msg : This is the message whose validation is complex and should happen asynchronously.
 *
 * @return : returns nothing
 */
template <typename MSG>
void ReplicaImp::asyncValidateMessage(std::unique_ptr<MSG> msg) {
  // The thread pool is initialized once and kept with this function.
  // This function is called in a single thread as the queue by dispatcher will not allow multiple threads together.
  try {
    static auto &threadPool = RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::STARTING);

    threadPool.async(
        [this](auto *unValidatedMsg, auto *replicaInfo, auto *incomingMessageQueue) {
          try {
            unValidatedMsg->validate(*replicaInfo);
            CarrierMesssage *validatedCarrierMsg = new ValidatedMessageCarrierInternalMsg<MSG>(unValidatedMsg);
            incomingMessageQueue->pushInternalMsg(validatedCarrierMsg);
          } catch (std::exception &e) {
            onReportAboutInvalidMessage(unValidatedMsg, e.what());
            delete unValidatedMsg;
            return;
          }
        },
        msg.release(),
        repsInfo,
        &getIncomingMsgsStorage());
  } catch (std::out_of_range &ex) {
    LOG_ERROR(GL, "The request threadpool selector is selecting some non-existent threadpool");
    return;
  }
}

/**
 * validatePrePrepareMsg initiates the validation and waits for completion of validation.
 * After validation, it routes the preprepare message as an internal message.
 *
 * @param msg : This is the PrePrepare message received by the replica, which is about
 * to get handled.
 * @return : returns nothing
 */
void ReplicaImp::validatePrePrepareMsg(PrePrepareMsgUPtr ppm) {
  // The thread pool is initialized once and kept with this function.
  // This function is called in a single thread as the queue by dispatcher will not allow multiple threads together.
  try {
    static auto &threadPool = RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::STARTING);
    threadPool.async(
        [this](auto *prePrepareMsg, auto *replicaInfo, auto *incomingMessageQueue, auto viewNum) {
          try {
            prePrepareMsg->validate(*replicaInfo);
            if (!validatePreProcessedResults(prePrepareMsg, viewNum)) {
              InternalMessage viewChangeIndicatorIm = ViewChangeIndicatorInternalMsg(
                  ReplicaAsksToLeaveViewMsg::Reason::PrimarySentBadPreProcessResult, viewNum);
              incomingMessageQueue->pushInternalMsg(std::move(viewChangeIndicatorIm));
              delete prePrepareMsg;
              return;
            }
            InternalMessage prePrepareCarrierIm = PrePrepareCarrierInternalMsg(prePrepareMsg);
            incomingMessageQueue->pushInternalMsg(std::move(prePrepareCarrierIm));
          } catch (std::exception &e) {
            onReportAboutInvalidMessage(prePrepareMsg, e.what());
            delete prePrepareMsg;
            return;
          }
        },
        ppm.release(),
        repsInfo,
        &getIncomingMsgsStorage(),
        getCurrentView());
  } catch (std::out_of_range &ex) {
    LOG_ERROR(GL, "The request threadpool selector is selecting some non-existent threadpool");
    return;
  }
}

std::function<bool(MessageBase *)> ReplicaImp::getMessageValidator() {
  return [this](MessageBase *message) { return validateMessage(message); };
}

void ReplicaImp::send(MessageBase *m, NodeIdType dest) {
  if (clientsManager->isInternal(dest)) {
    LOG_DEBUG(GL, "Not sending reply to internal client id - " << dest);
    return;
  }
  TimeRecorder scoped_timer(*histograms_.send);
  ReplicaBase::send(m, dest);
}

void ReplicaImp::sendAndIncrementMetric(MessageBase *m, NodeIdType id, CounterHandle &counterMetric) {
  send(m, id);
  counterMetric++;
}

void ReplicaImp::onReportAboutInvalidMessage(MessageBase *msg, const char *reason) {
  LOG_WARN(CNSUS, "Received invalid message. " << KVLOG(msg->senderId(), msg->type(), reason));

  // TODO(GG): logic that deals with invalid messages (e.g., a node that sends invalid messages may have a problem
  // (old version,bug,malicious,...)).
}

template <>
void ReplicaImp::onMessage<StateTransferMsg>(std::unique_ptr<StateTransferMsg> msg) {
  if (activeExecutions_ > 0) {
    pushDeferredMessage(msg.release());
    return;
  } else {
    ReplicaForStateTransfer::onMessage<StateTransferMsg>(std::move(msg));
  }
}

template <>
void ReplicaImp::onMessage<ClientRequestMsg>(std::unique_ptr<ClientRequestMsg> m) {
  metric_received_client_requests_++;
  const NodeIdType senderId = m->senderId();
  const NodeIdType clientId = m->clientProxyId();
  const bool readOnly = m->isReadOnly();
  const ReqId reqSeqNum = m->requestSeqNum();
  const uint64_t flags = m->flags();
  const auto &cid = m->getCid();

  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(CNSUS, KVLOG(clientId, reqSeqNum, senderId) << " flags: " << std::bitset<sizeof(uint64_t) * 8>(flags));

  auto msg = m.release();
  const auto &span_context = msg->spanContext<std::remove_pointer<decltype(msg)>::type>();
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
  span.setTag("rid", config_.getreplicaId());
  span.setTag("cid", cid);
  span.setTag("seq_num", reqSeqNum);

  // Drop external msgs off:
  // -  in case replica keys haven't been exchanged for all replicas, and it's not a key exchange msg then don't accept
  // the msgs.
  // -  the public keys of the clients haven't been published yet.
  if (!KeyExchangeManager::instance().exchanged() ||
      (!KeyExchangeManager::instance().clientKeysPublished() && repsInfo->isIdOfClientProxy(senderId))) {
    if (!(flags & KEY_EXCHANGE_FLAG) && !(flags & CLIENTS_PUB_KEYS_FLAG)) {
      LOG_INFO(KEY_EX_LOG, "Didn't complete yet, dropping msg");
      delete msg;
      return;
    }
  }

  // check message validity
  const bool invalidClient =
      !isValidClient(clientId) && !((repsInfo->isIdOfReplica(clientId) || repsInfo->isIdOfPeerRoReplica(clientId)) &&
                                    (flags & RECONFIG_FLAG || flags & INTERNAL_FLAG));
  const bool sentFromReplicaToNonPrimary =
      !(flags & RECONFIG_FLAG || flags & INTERNAL_FLAG) && repsInfo->isIdOfReplica(senderId) && !isCurrentPrimary();

  if (invalidClient) {
    ++numInvalidClients;
  }

  // TODO(GG): more conditions (make sure that a request of client A cannot be generated by client B!=A)
  if (invalidClient || sentFromReplicaToNonPrimary) {
    std::ostringstream oss("ClientRequestMsg is invalid. ");
    oss << KVLOG(invalidClient, sentFromReplicaToNonPrimary);
    onReportAboutInvalidMessage(msg, oss.str().c_str());
    delete msg;
    return;
  }

  if (readOnly) {
    if (activeExecutions_ > 0) {
      if (deferredRORequests_.size() < maxQueueSize_) {
        // We should handle span and deleting the message when we handle the deferred message
        deferredRORequests_.push_back(msg);
        deferredRORequestsMetric_++;
      } else {
        delete msg;
      }
    } else {
      executeReadOnlyRequest(span, msg);
      delete msg;
    }
    return;
  }

  if ((isCurrentPrimary() && isSeqNumToStopAt(primaryLastUsedSeqNum + 1)) ||
      isSeqNumToStopAt(lastExecutedSeqNum + activeExecutions_ + 1)) {
    LOG_INFO(CNSUS,
             "Ignoring ClientRequest because system is stopped at checkpoint pending control state operation (upgrade, "
             "etc...)");
    delete msg;
    return;
  }

  if (!currentViewIsActive()) {
    delete msg;
    return;
  }

  if (!isReplyAlreadySentToClient(clientId, reqSeqNum)) {
    if (isCurrentPrimary()) {
      histograms_.requestsQueueOfPrimarySize->record(requestsQueueOfPrimary.size());
      // TODO(GG): use config/parameter
      if (requestsQueueOfPrimary.size() >= maxPrimaryQueueSize) {
        LOG_WARN(CNSUS,
                 "ClientRequestMsg dropped. Primary request queue is full. "
                     << KVLOG(clientId, reqSeqNum, requestsQueueOfPrimary.size()));
        delete msg;
        return;
      }
      if (clientsManager->canBecomePending(clientId, reqSeqNum)) {
        LOG_DEBUG(CNSUS, "Pushing to primary queue, request " << KVLOG(reqSeqNum, clientId, senderId));
        if (time_to_collect_batch_ == MinTime) time_to_collect_batch_ = getMonotonicTime();
        metric_primary_batching_duration_.addStartTimeStamp(cid);
        requestsQueueOfPrimary.push(msg);
        primaryCombinedReqSize += msg->size();
        primary_queue_size_.Get().Set(requestsQueueOfPrimary.size());
        tryToSendPrePrepareMsg(true);
        return;
      } else {
        LOG_INFO(CNSUS,
                 "ClientRequestMsg is ignored because: request is old, or primary is currently working on it"
                     << KVLOG(clientId, reqSeqNum));
      }
    } else {  // not the current primary
      if (clientsManager->canBecomePending(clientId, reqSeqNum)) {
        clientsManager->addPendingRequest(clientId, reqSeqNum, cid);

        // Adding the message to a queue for future retransmission.
        if (requestsOfNonPrimary.size() < NonPrimaryCombinedReqSize) {
          if (requestsOfNonPrimary.count(msg->requestSeqNum())) {
            delete std::get<1>(requestsOfNonPrimary.at(msg->requestSeqNum()));
          }
          requestsOfNonPrimary[msg->requestSeqNum()] = std::make_pair(getMonotonicTime(), msg);
        }
        send(msg, currentPrimary());
        LOG_INFO(CNSUS,
                 "Forwarding ClientRequestMsg to the current primary." << KVLOG(reqSeqNum, clientId, currentPrimary()));
        return;
      }
      if (clientsManager->isPending(clientId, reqSeqNum)) {
        // As long as this request is not committed, we want to continue and alert the primary about it
        send(msg, currentPrimary());
      } else {
        LOG_INFO(CNSUS,
                 "ClientRequestMsg is ignored because: request is old, or primary is currently working on it"
                     << KVLOG(clientId, reqSeqNum));
      }
    }
  } else {  // Reply has already been sent to the client for this request
    auto repMsg = clientsManager->allocateReplyFromSavedOne(clientId, reqSeqNum, currentPrimary());
    LOG_DEBUG(
        CNSUS,
        "ClientRequestMsg has already been executed: retransmitting reply to client." << KVLOG(reqSeqNum, clientId));
    if (repMsg) {
      send(repMsg.get(), clientId);
    }
  }
  delete msg;
}  // namespace bftEngine::impl

template <>
void ReplicaImp::onMessage<preprocessor::PreProcessResultMsg>(std::unique_ptr<preprocessor::PreProcessResultMsg> msg) {
  LOG_DEBUG(GL,
            "Handling PreProcessResultMsg via ClientRequestMsg handler "
                << KVLOG(msg->clientProxyId(), msg->getCid(), msg->requestSeqNum()));
  return onMessage<ClientRequestMsg>(std::unique_ptr<ClientRequestMsg>(msg.release()));
}

template <>
void ReplicaImp::onMessage<ReplicaAsksToLeaveViewMsg>(std::unique_ptr<ReplicaAsksToLeaveViewMsg> message) {
  if (activeExecutions_ > 0) {
    pushDeferredMessage(message.release());
    return;
  }
  MDC_PUT(MDC_SEQ_NUM_KEY, std::to_string(getCurrentView()));
  if (message->viewNumber() == getCurrentView()) {
    LOG_INFO(VC_LOG,
             "Received ReplicaAsksToLeaveViewMsg "
                 << KVLOG(message->viewNumber(), message->senderId(), message->idOfGeneratedReplica()));
    viewsManager->storeComplaint(std::move(message));
    tryToGoToNextView();
  } else {
    LOG_WARN(VC_LOG,
             "Ignoring ReplicaAsksToLeaveViewMsg " << KVLOG(getCurrentView(),
                                                            currentViewIsActive(),
                                                            message->viewNumber(),
                                                            message->senderId(),
                                                            message->idOfGeneratedReplica()));
  }
}

bool ReplicaImp::checkSendPrePrepareMsgPrerequisites() {
  if (!isCurrentPrimary()) {
    LOG_WARN(GL, "Called in a non-primary replica; won't send PrePrepareMsgs!");
    return false;
  }

  if (isCollectingState()) {
    LOG_WARN(GL, "Called during state transfer; won't send PrePrepareMsgs!");
    return false;
  }

  if (!currentViewIsActive()) {
    LOG_INFO(GL, "View " << getCurrentView() << " is not active yet. Won't send PrePrepareMsg-s.");
    return false;
  }

  if (isSeqNumToStopAt(primaryLastUsedSeqNum + numOfTransientPreprepareMsgs_ + 1)) {
    LOG_INFO(GL,
             "Not sending PrePrepareMsg because system is stopped at checkpoint pending control state operation "
             "(upgrade, etc...)");
    return false;
  }

  if (primaryLastUsedSeqNum + numOfTransientPreprepareMsgs_ + 1 > lastStableSeqNum + kWorkWindowSize) {
    LOG_INFO(GL,
             "Will not send PrePrepare since next sequence number ["
                 << primaryLastUsedSeqNum + numOfTransientPreprepareMsgs_ + 1 << "] exceeds window threshold ["
                 << lastStableSeqNum + kWorkWindowSize << "]");
    return false;
  }

  if (primaryLastUsedSeqNum + numOfTransientPreprepareMsgs_ + 1 >
      lastExecutedSeqNum + config_.getconcurrencyLevel() + activeExecutions_) {
    LOG_INFO(GL,
             "Will not send PrePrepare since next sequence number ["
                 << primaryLastUsedSeqNum + numOfTransientPreprepareMsgs_ + 1 << "] exceeds concurrency threshold ["
                 << lastExecutedSeqNum + config_.getconcurrencyLevel() + activeExecutions_ << "]");
    return false;
  }
  metric_concurrency_level_.Get().Set(primaryLastUsedSeqNum + numOfTransientPreprepareMsgs_ + 1 - lastExecutedSeqNum);
  ConcordAssertGE(primaryLastUsedSeqNum, lastExecutedSeqNum + activeExecutions_);
  // Because maxConcurrentAgreementsByPrimary <  MaxConcurrentFastPaths
  ConcordAssertLE((primaryLastUsedSeqNum + 1), lastExecutedSeqNum + MaxConcurrentFastPaths + activeExecutions_);

  if (requestsQueueOfPrimary.empty()) LOG_DEBUG(GL, "requestsQueueOfPrimary is empty");

  return (!requestsQueueOfPrimary.empty());
}

void ReplicaImp::removeDuplicatedRequestsFromRequestsQueue() {
  TimeRecorder scoped_timer(*histograms_.removeDuplicatedRequestsFromQueue);
  // Remove duplicated requests that are result of client retrials from the head of the requestsQueueOfPrimary
  ClientRequestMsg *first = (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
  while (first != nullptr && !clientsManager->canBecomePending(first->clientProxyId(), first->requestSeqNum())) {
    primaryCombinedReqSize -= first->size();
    requestsQueueOfPrimary.pop();
    delete first;
    first = (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
  }
  primary_queue_size_.Get().Set(requestsQueueOfPrimary.size());
}

// The preprepare message can be nullptr if the finalisation is happening in a separate thread.
// So, the second value of the pair provides the real indication of success of failure.
PrePrepareMsgCreationResult ReplicaImp::buildPrePrepareMsgBatchByOverallSize(uint32_t requiredBatchSizeInBytes) {
  if (primaryCombinedReqSize < requiredBatchSizeInBytes) {
    LOG_DEBUG(GL,
              "Not sufficient messages size in the primary replica queue to fill a batch"
                  << KVLOG(primaryCombinedReqSize, requiredBatchSizeInBytes));
    return std::make_pair(nullptr, false);
  }
  if (!checkSendPrePrepareMsgPrerequisites()) return std::make_pair(nullptr, false);

  removeDuplicatedRequestsFromRequestsQueue();
  return buildPrePrepareMessageByBatchSize(requiredBatchSizeInBytes);
}

// The preprepare message can be nullptr if the finalisation is happening in a separate thread.
// So, the second value of the pair provides the real indication of success of failure.
PrePrepareMsgCreationResult ReplicaImp::buildPrePrepareMsgBatchByRequestsNum(uint32_t requiredRequestsNum) {
  ConcordAssertGT(requiredRequestsNum, 0);

  if (requestsQueueOfPrimary.size() < requiredRequestsNum) {
    LOG_DEBUG(GL,
              "Not enough messages in the primary replica queue to fill a batch"
                  << KVLOG(requestsQueueOfPrimary.size(), requiredRequestsNum));
    return std::make_pair(nullptr, false);
  }
  if (!checkSendPrePrepareMsgPrerequisites()) return std::make_pair(nullptr, false);

  removeDuplicatedRequestsFromRequestsQueue();
  return buildPrePrepareMessageByRequestsNum(requiredRequestsNum);
}

bool ReplicaImp::tryToSendPrePrepareMsg(bool batchingLogic) {
  if (!checkSendPrePrepareMsgPrerequisites()) return false;

  removeDuplicatedRequestsFromRequestsQueue();
  PrePrepareMsgUPtr pp;
  bool isSent = false;
  if (batchingLogic) {
    auto batchedReq = reqBatchingLogic_.batchRequests();
    isSent = batchedReq.second;
    if (isSent) {
      pp = std::move(batchedReq.first);
      batch_closed_on_logic_on_++;
      accumulating_batch_time_.add(
          std::chrono::duration_cast<std::chrono::microseconds>(getMonotonicTime() - time_to_collect_batch_).count());
      accumulating_batch_avg_time_.Get().Set((uint64_t)accumulating_batch_time_.avg());
      if (accumulating_batch_time_.numOfElements() == 1000) {
        accumulating_batch_time_.reset();  // We reset the average on every 1000 samples
      }
      time_to_collect_batch_ = MinTime;
    }
  } else {
    auto builtReq = buildPrePrepareMessage();
    isSent = builtReq.second;
    if (isSent) {
      pp = std::move(builtReq.first);
      batch_closed_on_logic_off_++;
      time_to_collect_batch_ = MinTime;
    }
  }
  if (!pp) return isSent;
  startConsensusProcess(pp.release());
  return true;
}

PrePrepareMsgUPtr ReplicaImp::createPrePrepareMessage() {
  CommitPath firstPath = controller->getCurrentFirstPath();
  ConcordAssertOR((config_.getcVal() != 0), (firstPath != CommitPath::FAST_WITH_THRESHOLD));
  if (requestsQueueOfPrimary.empty()) {
    LOG_INFO(GL, "PrePrepareMessage has not created - requestsQueueOfPrimary is empty");
    return nullptr;
  }

  if (!getReplicaConfig().prePrepareFinalizeAsyncEnabled) {
    controller->onSendingPrePrepare((primaryLastUsedSeqNum + 1), firstPath);
  }

  return make_unique<PrePrepareMsg>(config_.getreplicaId(),
                                    getCurrentView(),
                                    (primaryLastUsedSeqNum + 1),
                                    firstPath,
                                    requestsQueueOfPrimary.front()->spanContext<ClientRequestMsg>(),
                                    primaryCombinedReqSize);
}

ClientRequestMsg *ReplicaImp::addRequestToPrePrepareMessage(ClientRequestMsg *&nextRequest,
                                                            PrePrepareMsg &prePrepareMsg,
                                                            uint32_t maxStorageForRequests) {
  if (nextRequest->size() <= prePrepareMsg.remainingSizeForRequests()) {
    SCOPED_MDC_CID(nextRequest->getCid());
    if (clientsManager->canBecomePending(nextRequest->clientProxyId(), nextRequest->requestSeqNum())) {
      prePrepareMsg.addRequest(nextRequest->body(), nextRequest->size());
      clientsManager->addPendingRequest(
          nextRequest->clientProxyId(), nextRequest->requestSeqNum(), nextRequest->getCid());
      metric_primary_batching_duration_.finishMeasurement(nextRequest->getCid());
    }
  } else if (nextRequest->size() > maxStorageForRequests) {  // The message is too big
    LOG_WARN(GL,
             "Request was dropped because it exceeds maximum allowed size" << KVLOG(
                 prePrepareMsg.seqNumber(), nextRequest->senderId(), nextRequest->size(), maxStorageForRequests));
  }
  primaryCombinedReqSize -= nextRequest->size();
  requestsQueueOfPrimary.pop();
  delete nextRequest;
  primary_queue_size_.Get().Set(requestsQueueOfPrimary.size());
  return (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
}

// Finalize the preprepare message by adding digest at a point when no more changes will happen.
// The digest calculation can happen in separate thread or in the same thread depending on the configuration.
// This function will return failure if there are no client request messages, which is marked by the
// second value in the returned pair.
// The first value of the pair can be nullptr depending on the availability of the result. So the
// first value cannot represent failure.
PrePrepareMsgCreationResult ReplicaImp::finishAddingRequestsToPrePrepareMsg(PrePrepareMsgUPtr prePrepareMsg,
                                                                            uint32_t maxSpaceForReqs,
                                                                            uint32_t requiredRequestsSize,
                                                                            uint32_t requiredRequestsNum) {
  if (prePrepareMsg->numberOfRequests() == 0) {
    LOG_INFO(GL, "No client requests added to the PrePrepare batch, delete the message");
    return std::make_pair(nullptr, false);
  }
  {
    if (getReplicaConfig().prePrepareFinalizeAsyncEnabled) {
      // The thread pool is initialized once and kept with this function.
      // This function is called in a single thread as the queue by dispatcher will not allow multiple threads together.
      try {
        static auto &threadPool = RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::STARTING);
        // The UINT16 MAX is the upper bound of numOfTransientPreprepareMsgs_
        // This upper limit should never be reached and thus this check is added. We cannot do ConcordAssert here,
        // since there is a way to move forward if we stop just before touching this upper bound.
        if ((numOfTransientPreprepareMsgs_ + 1) < std::numeric_limits<decltype(numOfTransientPreprepareMsgs_)>::max()) {
          numOfTransientPreprepareMsgs_++;
        } else {
          LOG_ERROR(GL, "The number of transient preprepare messages are very large so they will have to be retried.");
          return std::make_pair(nullptr, false);
        }
        // prePrepareMsg is about to be moved. save here values for future use
        auto seqNum = prePrepareMsg->seqNumber();
        auto cid = prePrepareMsg->getCid();
        auto requestsSize = prePrepareMsg->requestsSize();
        auto numberOfRequests = prePrepareMsg->numberOfRequests();
        threadPool.async(
            [](auto ppm, auto *iq, auto *hist) {
              {
                TimeRecorder scoped_timer(*(hist->finishAddingRequestsToPrePrepareMsg));
                ppm->finishAddingRequests();
              }
              iq->pushInternalMsg(ppm.release());
            },
            std::move(prePrepareMsg),
            &(getIncomingMsgsStorage()),
            &histograms_);
        LOG_DEBUG(GL,
                  "Finishing adding requests in the thread : " << KVLOG(seqNum,
                                                                        cid,
                                                                        maxSpaceForReqs,
                                                                        requiredRequestsSize,
                                                                        requestsSize,
                                                                        requiredRequestsNum,
                                                                        numberOfRequests));
        return std::make_pair(nullptr, true);
      } catch (std::out_of_range &ex) {
        LOG_ERROR(GL, "The request threadpool selector is selecting some non-existent threadpool");
        return std::make_pair(nullptr, false);
      }

    } else {
      {
        TimeRecorder scoped_timer(*histograms_.finishAddingRequestsToPrePrepareMsg);
        prePrepareMsg->finishAddingRequests();
      }
      LOG_DEBUG(GL,
                "Finishing  adding requests without thread : " << KVLOG(prePrepareMsg->seqNumber(),
                                                                        prePrepareMsg->getCid(),
                                                                        maxSpaceForReqs,
                                                                        requiredRequestsSize,
                                                                        prePrepareMsg->requestsSize(),
                                                                        requiredRequestsNum,
                                                                        prePrepareMsg->numberOfRequests()));
      return std::make_pair(std::move(prePrepareMsg), true);
    }
  }
}

// The preprepare message can be nullptr if the finalisation is happening in a separate thread.
// So the second value of the pair provides the real indication of success of failure.
PrePrepareMsgCreationResult ReplicaImp::buildPrePrepareMessage() {
  TimeRecorder scoped_timer(*histograms_.buildPrePrepareMessage);
  auto prePrepareMsg = createPrePrepareMessage();
  if (!prePrepareMsg) return std::make_pair(nullptr, false);

  if (!getReplicaConfig().prePrepareFinalizeAsyncEnabled) {
    SCOPED_MDC("pp_msg_cid", prePrepareMsg->getCid());
  }

  uint32_t maxSpaceForReqs = prePrepareMsg->remainingSizeForRequests();
  {
    TimeRecorder scoped_timer1(*histograms_.addAllRequestsToPrePrepare);
    ClientRequestMsg *nextRequest = (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
    while (nextRequest != nullptr)
      nextRequest = addRequestToPrePrepareMessage(nextRequest, *prePrepareMsg.get(), maxSpaceForReqs);
  }
  return finishAddingRequestsToPrePrepareMsg(std::move(prePrepareMsg), maxSpaceForReqs, 0, 0);
}

// The preprepare message can be nullptr if the finalisation is happening in a separate thread.
// So the second value of the pair provides the real indication of success of failure.
PrePrepareMsgCreationResult ReplicaImp::buildPrePrepareMessageByRequestsNum(uint32_t requiredRequestsNum) {
  auto prePrepareMsg = createPrePrepareMessage();
  if (!prePrepareMsg) return std::make_pair(nullptr, false);
  if (!getReplicaConfig().prePrepareFinalizeAsyncEnabled) {
    SCOPED_MDC("pp_msg_cid", prePrepareMsg->getCid());
  }

  uint32_t maxSpaceForReqs = prePrepareMsg->remainingSizeForRequests();
  ClientRequestMsg *nextRequest = requestsQueueOfPrimary.front();
  while (nextRequest != nullptr && prePrepareMsg->numberOfRequests() < requiredRequestsNum)
    nextRequest = addRequestToPrePrepareMessage(nextRequest, *prePrepareMsg.get(), maxSpaceForReqs);

  return finishAddingRequestsToPrePrepareMsg(std::move(prePrepareMsg), maxSpaceForReqs, 0, requiredRequestsNum);
}

// The preprepare message can be nullptr if the finalisation is happening in a separate thread.
// So the second value of the pair provides the real indication of success of failure.
PrePrepareMsgCreationResult ReplicaImp::buildPrePrepareMessageByBatchSize(uint32_t requiredBatchSizeInBytes) {
  auto prePrepareMsg = createPrePrepareMessage();
  if (!prePrepareMsg) return std::make_pair(nullptr, false);
  if (!getReplicaConfig().prePrepareFinalizeAsyncEnabled) {
    SCOPED_MDC("pp_msg_cid", prePrepareMsg->getCid());
  }

  uint32_t maxSpaceForReqs = prePrepareMsg->remainingSizeForRequests();
  ClientRequestMsg *nextRequest = requestsQueueOfPrimary.front();
  while (nextRequest != nullptr &&
         (maxSpaceForReqs - prePrepareMsg->remainingSizeForRequests() < requiredBatchSizeInBytes))
    nextRequest = addRequestToPrePrepareMessage(nextRequest, *prePrepareMsg.get(), maxSpaceForReqs);

  return finishAddingRequestsToPrePrepareMsg(std::move(prePrepareMsg), maxSpaceForReqs, requiredBatchSizeInBytes, 0);
}

void ReplicaImp::startConsensusProcess(PrePrepareMsg *pp) {
  static constexpr bool createdEarlier = false;
  startConsensusProcess(pp, createdEarlier);
}

void ReplicaImp::startConsensusProcess(PrePrepareMsg *pp, bool isCreatedEarlier) {
  if (!isCurrentPrimary()) {
    delete pp;
    return;
  }
  TimeRecorder scoped_timer(*histograms_.startConsensusProcess);
  if (getReplicaConfig().timeServiceEnabled) {
    pp->setTime(time_service_manager_->getClockTimePoint());
  }

  auto firstPath = pp->firstPath();
  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onSendPrePrepareMessage(pp->numberOfRequests(), requestsQueueOfPrimary.size());
  }
  metric_bft_batch_size_.Get().Set(pp->numberOfRequests());
  primaryLastUsedSeqNum++;

  // guaranteed to be in primary
  metric_consensus_duration_.addStartTimeStamp(primaryLastUsedSeqNum);

  if (isCreatedEarlier) {
    controller->onSendingPrePrepare(primaryLastUsedSeqNum, firstPath);
    pp->setSeqNumber(primaryLastUsedSeqNum);
    pp->setCid(primaryLastUsedSeqNum);
  }
  metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
  SCOPED_MDC_SEQ_NUM(std::to_string(primaryLastUsedSeqNum));
  if (getReplicaConfig().prePrepareFinalizeAsyncEnabled) {
    SCOPED_MDC("pp_msg_cid", pp->getCid());
  }
  SCOPED_MDC_PATH(CommitPathToMDCString(firstPath));

  LOG_INFO(CNSUS,
           "Sending PrePrepare message" << KVLOG(pp->numberOfRequests())
                                        << " correlation ids: " << pp->getBatchCorrelationIdAsString());

  consensus_times_.start(primaryLastUsedSeqNum);

  SeqNumInfo &seqNumInfo = mainLog->get(primaryLastUsedSeqNum);
  {
    TimeRecorder scoped_timer1(*histograms_.addSelfMsgPrePrepare);
    seqNumInfo.addSelfMsg(pp);
  }

  if (ps_) {
    TimeRecorder scoped_timer1(*histograms_.prePrepareWriteTransaction);
    ps_->beginWriteTran();
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
    ps_->setPrePrepareMsgInSeqNumWindow(primaryLastUsedSeqNum, pp);
    if (firstPath == CommitPath::SLOW) ps_->setSlowStartedInSeqNumWindow(primaryLastUsedSeqNum, true);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }

  {
    TimeRecorder scoped_timer1(*histograms_.broadcastPrePrepare);
    if (!retransmissionsLogicEnabled) {
      sendToAllOtherReplicas(pp);
    } else {
      for (ReplicaId x : repsInfo->idsOfPeerReplicas()) {
        sendRetransmittableMsgToReplica(pp, x, primaryLastUsedSeqNum);
      }
    }
  }

  if (firstPath == CommitPath::SLOW) {
    startSlowPath(seqNumInfo);
    TimeRecorder scoped_timer1(*histograms_.sendPreparePartialToSelf);
    sendPreparePartial(seqNumInfo);
  } else {
    TimeRecorder scoped_timer1(*histograms_.sendPreparePartialToSelf);
    sendPartialProof(seqNumInfo);
  }
}

bool ReplicaImp::isSeqNumToStopAt(SeqNum seq_num) {
  if (ControlStateManager::instance().getPruningProcessStatus()) return true;
  if (ControlStateManager::instance().isWedged()) return true;
  auto seq_num_to_stop_at = ControlStateManager::instance().getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value()) {
    if (seq_num_to_stop_at < seq_num) return true;
  }
  return false;
}

template <typename T>
bool ReplicaImp::relevantMsgForActiveView(const T *msg) {
  const SeqNum msgSeqNum = msg->seqNumber();
  const ViewNum msgViewNum = msg->viewNumber();

  const bool isCurrentViewActive = currentViewIsActive();
  if (isCurrentViewActive && (msgViewNum == getCurrentView()) && (msgSeqNum > strictLowerBoundOfSeqNums) &&
      (mainLog->insideActiveWindow(msgSeqNum))) {
    ConcordAssertGT(msgSeqNum, lastStableSeqNum);
    ConcordAssertLE(msgSeqNum, lastStableSeqNum + kWorkWindowSize);

    return true;
  } else if (!isCurrentViewActive) {
    LOG_INFO(CNSUS,
             "My current view is not active, ignoring msg."
                 << KVLOG(getCurrentView(), isCurrentViewActive, msg->senderId(), msgSeqNum, msgViewNum));
    return false;
  } else {
    const SeqNum activeWindowStart = mainLog->currentActiveWindow().first;
    const SeqNum activeWindowEnd = mainLog->currentActiveWindow().second;
    const bool myReplicaMayBeBehind = (getCurrentView() < msgViewNum) || (msgSeqNum > activeWindowEnd);
    if (myReplicaMayBeBehind) {
      onReportAboutAdvancedReplica(msg->senderId(), msgSeqNum, msgViewNum);
      LOG_INFO(CNSUS,
               "Msg is not relevant for my current view. The sending replica may be in advance."
                   << KVLOG(getCurrentView(),
                            isCurrentViewActive,
                            msg->senderId(),
                            msgSeqNum,
                            msgViewNum,
                            activeWindowStart,
                            activeWindowEnd));
    } else {
      const bool msgReplicaMayBeBehind =
          (getCurrentView() > msgViewNum) || (msgSeqNum + kWorkWindowSize < activeWindowStart);

      if (msgReplicaMayBeBehind) {
        onReportAboutLateReplica(msg->senderId(), msgSeqNum, msgViewNum);
        LOG_INFO(
            CNSUS,
            "Msg is not relevant for my current view. The sending replica may be behind." << KVLOG(getCurrentView(),
                                                                                                   isCurrentViewActive,
                                                                                                   msg->senderId(),
                                                                                                   msgSeqNum,
                                                                                                   msgViewNum,
                                                                                                   activeWindowStart,
                                                                                                   activeWindowEnd));
      }
    }
    return false;
  }
}

bool ReplicaImp::validatePreProcessedResults(const PrePrepareMsg *msg, const ViewNum registeredView) const {
  RequestsIterator reqIter(msg);
  char *requestBody = nullptr;
  std::vector<std::future<void>> tasks;
  std::vector<std::optional<std::string>> errors(msg->numberOfRequests());
  size_t error_id = 0;
  // The thread pool is initialized once and kept with this function.
  // This function is called in a single thread as the queue by dispatcher will not allow multiple threads together.
  try {
    static auto &threadPool = RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::FIRSTLEVEL);

    while (reqIter.getAndGoToNext(requestBody)) {
      const MessageBase::Header *hdr = (MessageBase::Header *)requestBody;
      if (hdr->msgType == MsgCode::PreProcessResult) {
        tasks.push_back(threadPool.async(
            [&errors, requestBody, error_id](auto replicaId, auto fVal) {
              preprocessor::PreProcessResultMsg req((ClientRequestMsgHeader *)requestBody);
              errors[error_id] = req.validatePreProcessResultSignatures(replicaId, fVal);
            },
            getReplicaConfig().replicaId,
            getReplicaConfig().fVal));
        error_id++;
      }
    }
    for (const auto &t : tasks) {
      t.wait();
    }
    errors.resize(error_id);
    for (auto err : errors) {
      if (err) {
        // Indicate a view change
        LOG_WARN(VC_LOG, "Replica will ask to leave view" << KVLOG(*err, registeredView));
        return false;
      }
    }
    return true;
  } catch (std::out_of_range &ex) {
    LOG_FATAL(VC_LOG, "Bad threadpool level for Request thread pools");
    return false;
  }
}

template <>
void ReplicaImp::onMessage<PrePrepareMsg>(PrePrepareMsgUPtr message) {
  if (isSeqNumToStopAt(message->seqNumber())) {
    LOG_INFO(GL,
             "Ignoring PrePrepareMsg because system is stopped at checkpoint pending control state operation (upgrade, "
             "etc...)");
    return;
  }

  if (!getReplicaConfig().prePrepareFinalizeAsyncEnabled) {
    if (!validatePreProcessedResults(message.get(), getCurrentView())) {
      // trigger view change
      LOG_WARN(VC_LOG, "PreProcessResult Signature failure. Ask to leave view" << KVLOG(getCurrentView()));
      askToLeaveView(ReplicaAsksToLeaveViewMsg::Reason::PrimarySentBadPreProcessResult);
      return;
    }
  }

  metric_received_pre_prepares_++;
  const SeqNum msgSeqNum = message->seqNumber();
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_INFO(CNSUS, "Received PrePrepareMsg" << KVLOG(message->senderId(), msgSeqNum, message->size()));

  auto *msg = message.release();
  if (!currentViewIsActive() && viewsManager->waitingForMsgs() && msgSeqNum > lastStableSeqNum) {
    ConcordAssert(!msg->isNull());  // we should never send (and never accept) null PrePrepare message

    if (viewsManager->addPotentiallyMissingPP(msg, lastStableSeqNum)) {
      LOG_INFO(CNSUS, "PrePrepare added to views manager. " << KVLOG(lastStableSeqNum));
      tryToEnterView();
    } else {
      LOG_INFO(CNSUS, "PrePrepare discarded.");
      delete msg;
    }
    return;
  }

  bool msgAdded = false;
  if (relevantMsgForActiveView(msg) && (msg->senderId() == currentPrimary())) {
    sendAckIfNeeded(msg, msg->senderId(), msgSeqNum);
    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);
    const bool slowStarted = (msg->firstPath() == CommitPath::SLOW || seqNumInfo.slowPathStarted());

    bool time_is_ok = true;
    if (config_.timeServiceEnabled) {
      auto ppmTime = msg->getTime();
      if (ppmTime <= 0) {
        LOG_WARN(CNSUS, "No timePoint in the prePrepare, PrePrepare will be ignored" << KVLOG(ppmTime));
        delete msg;
        return;
      }
      if (msgSeqNum > maxSeqNumTransferredFromPrevViews /* not transferred from the previous view*/) {
        time_is_ok = time_service_manager_->isPrimarysTimeWithinBounds(ppmTime);
      }
    }
    // For MDC it doesn't matter which type of fast path
    SCOPED_MDC_PATH(CommitPathToMDCString(slowStarted ? CommitPath::SLOW : CommitPath::OPTIMISTIC_FAST));
    // All pre-prepare messages are added in seqNumInfo even if replica detects incorrect time
    if (seqNumInfo.addMsg(msg, false, time_is_ok)) {
      msgAdded = true;

      // Start tracking all client requests with in this pp message
      RequestsIterator reqIter(msg);
      char *requestBody = nullptr;
      while (reqIter.getAndGoToNext(requestBody)) {
        ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader *>(requestBody));
        if (!clientsManager->isValidClient(req.clientProxyId())) continue;
        clientsManager->removeRequestsOutOfBatchBounds(req.clientProxyId(), req.requestSeqNum());
        if (clientsManager->canBecomePending(req.clientProxyId(), req.requestSeqNum()))
          clientsManager->addPendingRequest(req.clientProxyId(), req.requestSeqNum(), req.getCid());
        if (requestsOfNonPrimary.count(req.requestSeqNum())) {
          delete std::get<1>(requestsOfNonPrimary.at(req.requestSeqNum()));
          requestsOfNonPrimary.erase(req.requestSeqNum());
        }
      }
      if (ps_) {
        ps_->beginWriteTran();
        ps_->setPrePrepareMsgInSeqNumWindow(msgSeqNum, msg);
        if (slowStarted) ps_->setSlowStartedInSeqNumWindow(msgSeqNum, true);
        ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
      }
      if (!slowStarted)  // TODO(GG): make sure we correctly handle a situation where StartSlowCommitMsg is handled
                         // before PrePrepareMsg
      {
        sendPartialProof(seqNumInfo);
      } else {
        startSlowPath(seqNumInfo);
        sendPreparePartial(seqNumInfo);
      }
      // if time is not ok, we do not continue with consensus flow
    }
  }

  if (!msgAdded) delete msg;
}

void ReplicaImp::tryToStartSlowPaths() {
  if (!isCurrentPrimary() || isCollectingState() || !currentViewIsActive())
    return;  // TODO(GG): consider to stop the related timer when this method is not needed (to avoid useless
  // invocations)

  const SeqNum minSeqNum = lastExecutedSeqNum + activeExecutions_ + 1;
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));

  if (minSeqNum > lastStableSeqNum + kWorkWindowSize) {
    LOG_INFO(GL,
             "Try to start slow path: minSeqNum > lastStableSeqNum + kWorkWindowSize."
                 << KVLOG(minSeqNum, lastStableSeqNum, kWorkWindowSize));
    return;
  }

  const SeqNum maxSeqNum = primaryLastUsedSeqNum;

  ConcordAssertLE(maxSeqNum, lastStableSeqNum + kWorkWindowSize);
  ConcordAssertLE(minSeqNum, maxSeqNum + 1);

  if (minSeqNum > maxSeqNum) return;

  sendCheckpointIfNeeded();  // TODO(GG): TBD - do we want it here ?

  const Time currTime = getMonotonicTime();

  for (SeqNum i = minSeqNum; i <= maxSeqNum; i++) {
    SeqNumInfo &seqNumInfo = mainLog->get(i);

    if (seqNumInfo.hasFastPathFullCommitProof() ||                       // already has a full proof
        seqNumInfo.slowPathStarted() ||                                  // slow path has already  started
        seqNumInfo.getFastPathSelfPartialCommitProofMsg() == nullptr ||  // did not start a fast path
        (!seqNumInfo.hasPrePrepareMsg()))
      continue;  // slow path is not needed

    const Time timeOfPartProof = seqNumInfo.getFastPathTimeOfSelfPartialProof();

    if (currTime - timeOfPartProof < milliseconds(controller->timeToStartSlowPathMilli())) break;
    SCOPED_MDC_SEQ_NUM(std::to_string(i));
    SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));
    LOG_INFO(CNSUS,
             "Primary initiates slow path for seqNum="
                 << i << " (currTime=" << duration_cast<microseconds>(currTime.time_since_epoch()).count()
                 << " timeOfPartProof=" << duration_cast<microseconds>(timeOfPartProof.time_since_epoch()).count()
                 << " threshold for degradation [" << controller->timeToStartSlowPathMilli() << "ms]");

    controller->onStartingSlowCommit(i);

    startSlowPath(seqNumInfo);

    if (ps_) {
      ps_->beginWriteTran();
      ps_->setSlowStartedInSeqNumWindow(i, true);
      ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
    }

    // send StartSlowCommitMsg to all replicas
    StartSlowCommitMsg *startSlow = new StartSlowCommitMsg(config_.getreplicaId(), getCurrentView(), i);

    if (!retransmissionsLogicEnabled) {
      sendToAllOtherReplicas(startSlow);
    } else {
      for (ReplicaId x : repsInfo->idsOfPeerReplicas()) {
        sendRetransmittableMsgToReplica(startSlow, x, i);
      }
    }

    delete startSlow;

    sendPreparePartial(seqNumInfo);
  }
}

void ReplicaImp::tryToAskForMissingInfo() {
  if (!currentViewIsActive() || isCollectingState()) return;

  ConcordAssertLE(maxSeqNumTransferredFromPrevViews, lastStableSeqNum + kWorkWindowSize);

  const bool recentViewChange = (maxSeqNumTransferredFromPrevViews > lastStableSeqNum);

  SeqNum minSeqNum = 0;
  SeqNum maxSeqNum = 0;
  const int16_t searchWindow = 32;  // TODO(GG): TBD - read from configuration

  if (!recentViewChange) {
    minSeqNum = lastExecutedSeqNum + activeExecutions_ + 1;
    maxSeqNum = std::min(minSeqNum + searchWindow - 1, lastStableSeqNum + kWorkWindowSize);
  } else {
    minSeqNum = lastStableSeqNum + 1;
    while (minSeqNum <= lastStableSeqNum + kWorkWindowSize) {
      SeqNumInfo &seqNumInfo = mainLog->get(minSeqNum);
      if (!seqNumInfo.isCommitted__gg()) break;
      minSeqNum++;
    }
    maxSeqNum = std::min(minSeqNum + searchWindow - 1, lastStableSeqNum + kWorkWindowSize);
  }

  if (minSeqNum > lastStableSeqNum + kWorkWindowSize) return;

  const Time curTime = getMonotonicTime();

  SeqNum lastRelatedSeqNum = 0;

  // TODO(GG): improve/optimize the following loops

  for (SeqNum i = minSeqNum; i <= maxSeqNum; i++) {
    ConcordAssert(mainLog->insideActiveWindow(i));

    const SeqNumInfo &seqNumInfo = mainLog->get(i);

    Time t = seqNumInfo.getTimeOfFirstRelevantInfoFromPrimary();

    const Time lastInfoRequest = seqNumInfo.getTimeOfLastInfoRequest();

    if ((t < lastInfoRequest)) t = lastInfoRequest;

    if (t != MinTime && (t < curTime)) {
      auto diffMilli = duration_cast<milliseconds>(curTime - t);
      if (diffMilli.count() >= dynamicUpperLimitOfRounds->upperLimit()) lastRelatedSeqNum = i;
    }
  }

  for (SeqNum i = minSeqNum; i <= lastRelatedSeqNum; i++) {
    if (!recentViewChange) {
      tryToSendReqMissingDataMsg(i);
    } else {
      if (isCurrentPrimary()) {
        tryToSendReqMissingDataMsg(i, true);  // This Replica is Primary, need to ask everyone else
      } else {
        tryToSendReqMissingDataMsg(i, true, currentPrimary());  // Ask the Primary
      }
    }
  }
}

template <>
void ReplicaImp::onMessage<StartSlowCommitMsg>(std::unique_ptr<StartSlowCommitMsg> message) {
  metric_received_start_slow_commits_++;
  auto *msg = message.release();
  const SeqNum msgSeqNum = msg->seqNumber();
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_INFO(CNSUS, "Received StartSlowCommitMsg " << KVLOG(msgSeqNum, msg->senderId()));

  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, currentPrimary(), msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    if (!seqNumInfo.slowPathStarted() && !seqNumInfo.isPrepared()) {
      LOG_INFO(CNSUS, "Start slow path.");

      startSlowPath(seqNumInfo);

      if (ps_) {
        ps_->beginWriteTran();
        ps_->setSlowStartedInSeqNumWindow(msgSeqNum, true);
        ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
      }

      if (seqNumInfo.hasPrePrepareMsg() == false)
        tryToSendReqMissingDataMsg(msgSeqNum);
      else
        sendPreparePartial(seqNumInfo);
    }
  }
  delete msg;
}

void ReplicaImp::sendPartialProof(SeqNumInfo &seqNumInfo) {
  if (!seqNumInfo.hasPrePrepareMsg()) return;

  PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
  Digest &ppDigest = pp->digestOfRequests();
  const SeqNum seqNum = pp->seqNumber();

  if (!seqNumInfo.hasFastPathFullCommitProof()) {
    // send PartialCommitProofMsg to all collectors
    LOG_INFO(CNSUS, "Sending PartialCommitProofMsg, sequence number:" << pp->seqNumber());

    PartialCommitProofMsg *part = seqNumInfo.getFastPathSelfPartialCommitProofMsg();

    if (part == nullptr) {
      std::shared_ptr<IThresholdSigner> commitSigner;
      CommitPath commitPath = pp->firstPath();

      ConcordAssertOR((config_.getcVal() != 0), (commitPath != CommitPath::FAST_WITH_THRESHOLD));

      if ((commitPath == CommitPath::FAST_WITH_THRESHOLD) && (config_.getcVal() > 0))
        commitSigner = CryptoManager::instance().thresholdSignerForCommit(seqNum);
      else
        commitSigner = CryptoManager::instance().thresholdSignerForOptimisticCommit(seqNum);

      Digest tmpDigest;
      ppDigest.calcCombination(getCurrentView(), seqNum, tmpDigest);

      const auto &span_context = pp->spanContext<std::remove_pointer<decltype(pp)>::type>();
      part = new PartialCommitProofMsg(
          config_.getreplicaId(), getCurrentView(), seqNum, commitPath, tmpDigest, commitSigner, span_context);
      seqNumInfo.addFastPathSelfPartialCommitMsgAndDigest(part, tmpDigest);
    }

    seqNumInfo.setFastPathTimeOfSelfPartialProof(getMonotonicTime());

    if (!seqNumInfo.isTimeCorrect()) return;
    // send PartialCommitProofMsg (only if, from my point of view, at most MaxConcurrentFastPaths are in progress)
    if (seqNum <= lastExecutedSeqNum + MaxConcurrentFastPaths) {
      // TODO(GG): improve the following code (use iterators instead of a simple array)
      int8_t numOfRouters = 0;
      ReplicaId routersArray[2];

      repsInfo->getCollectorsForPartialProofs(getCurrentView(), seqNum, &numOfRouters, routersArray);

      for (int i = 0; i < numOfRouters; i++) {
        ReplicaId router = routersArray[i];
        if (router != config_.getreplicaId()) {
          sendRetransmittableMsgToReplica(part, router, seqNum);
        }
      }
    }
  }
}

void ReplicaImp::sendPreparePartial(SeqNumInfo &seqNumInfo) {
  ConcordAssert(currentViewIsActive());

  if (seqNumInfo.getSelfPreparePartialMsg() == nullptr && seqNumInfo.hasPrePrepareMsg() && !seqNumInfo.isPrepared()) {
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();

    ConcordAssertNE(pp, nullptr);
    SCOPED_MDC_PATH(CommitPathToMDCString(pp->firstPath()));
    LOG_DEBUG(CNSUS, "Sending PreparePartialMsg, sequence number:" << pp->seqNumber());

    const auto &span_context = pp->spanContext<std::remove_pointer<decltype(pp)>::type>();
    PreparePartialMsg *p =
        PreparePartialMsg::create(getCurrentView(),
                                  pp->seqNumber(),
                                  config_.getreplicaId(),
                                  pp->digestOfRequests(),
                                  CryptoManager::instance().thresholdSignerForSlowPathCommit(pp->seqNumber()),
                                  span_context);
    seqNumInfo.addSelfMsg(p);

    if (!isCurrentPrimary() && seqNumInfo.isTimeCorrect()) {
      sendRetransmittableMsgToReplica(p, currentPrimary(), pp->seqNumber());
    }
  }
}

void ReplicaImp::sendCommitPartial(const SeqNum s) {
  ConcordAssert(currentViewIsActive());
  ConcordAssert(mainLog->insideActiveWindow(s));
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(s));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  SeqNumInfo &seqNumInfo = mainLog->get(s);
  PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();

  ConcordAssert(seqNumInfo.isPrepared());
  ConcordAssertNE(pp, nullptr);
  ConcordAssertEQ(pp->seqNumber(), s);

  if (seqNumInfo.committedOrHasCommitPartialFromReplica(config_.getreplicaId())) return;  // not needed

  LOG_INFO(CNSUS, "Sending CommitPartialMsg, sequence number:" << pp->seqNumber());

  Digest digest;
  pp->digestOfRequests().digestOfDigest(digest);

  auto prepareFullMsg = seqNumInfo.getValidPrepareFullMsg();

  CommitPartialMsg *c =
      CommitPartialMsg::create(getCurrentView(),
                               s,
                               config_.getreplicaId(),
                               digest,
                               CryptoManager::instance().thresholdSignerForSlowPathCommit(s),
                               prepareFullMsg->spanContext<std::remove_pointer<decltype(prepareFullMsg)>::type>());
  seqNumInfo.addSelfCommitPartialMsgAndDigest(c, digest);

  if (!isCurrentPrimary()) sendRetransmittableMsgToReplica(c, currentPrimary(), s);
}

template <>
void ReplicaImp::onMessage<PartialCommitProofMsg>(std::unique_ptr<PartialCommitProofMsg> message) {
  metric_received_partial_commit_proofs_++;
  auto *msg = message.release();
  const SeqNum msgSeqNum = msg->seqNumber();
  const SeqNum msgView = msg->viewNumber();
  const NodeIdType msgSender = msg->senderId();
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(msg->commitPath()));
  ConcordAssert(repsInfo->isIdOfPeerReplica(msgSender));
  ConcordAssert(repsInfo->isCollectorForPartialProofs(msgView, msgSeqNum));

  LOG_INFO(CNSUS, "Received PartialCommitProofMsg. " << KVLOG(msgSender, msgSeqNum, msg->size()));
  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);
    if (msgSeqNum > lastExecutedSeqNum + activeExecutions_) {
      SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);
      if (seqNumInfo.addFastPathPartialCommitMsg(msg)) {
        return;
      }
    }
  }
  delete msg;
}

template <>
void ReplicaImp::onMessage<FullCommitProofMsg>(std::unique_ptr<FullCommitProofMsg> message) {
  auto *msg = message.release();
  pm_->Delay<concord::performance::SlowdownPhase::ConsensusFullCommitMsgProcess>(
      reinterpret_cast<char *>(msg),
      msg->sizeNeededForObjAndMsgInLocalBuffer(),
      [&s = getIncomingMsgsStorage()](char *msg, size_t &size) { s.pushExternalMsgRaw(msg, size); });

  metric_received_full_commit_proofs_++;
  const SeqNum msgSeqNum = msg->seqNumber();
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msg->seqNumber()));
  LOG_INFO(CNSUS, "Received FullCommitProofMsg message. " << KVLOG(msg->senderId(), msgSeqNum, msg->size()));

  if (relevantMsgForActiveView(msg)) {
    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);
    if (seqNumInfo.hasFastPathFullCommitProof()) {
      const auto fullProofCollectorId = seqNumInfo.getFastPathFullCommitProofMsg()->senderId();
      LOG_INFO(CNSUS,
               "FullCommitProof for seq num " << msgSeqNum << " was already received from replica "
                                              << fullProofCollectorId << " and has been processed."
                                              << " Ignoring the FullCommitProof from replica " << msg->senderId());
    } else if (seqNumInfo.addFastPathFullCommitMsg(msg)) {
      auto commitPath = seqNumInfo.getFastPathSelfPartialCommitProofMsg()->commitPath();
      SCOPED_MDC_PATH(CommitPathToMDCString(commitPath));
      LOG_INFO(CNSUS, "Added FullCommitProof to verification queue" << KVLOG(msg->senderId(), msgSeqNum, msg->size()));
      return;  // We've added the msg -- don't delete!
    }
  }
  delete msg;
}

/**
 * onCarrierMessage : This is the Validated Message Callback dispatcher. This dispatching
 * is happening without any queue as its already done by the IncomingMsgsStorageImp
 *
 * @param msg : This is the Carrier Message that will be dispatched.
 *
 * @return : returns nothing
 */
void ReplicaImp::onCarrierMessage(CarrierMesssage *msg) {
  auto validatedMsgCallBack = msgHandlers_->getValidatedMsgCallback(msg->getMsgType());
  ConcordAssert(validatedMsgCallBack != nullptr);
  validatedMsgCallBack(msg);
}

void ReplicaImp::onInternalMsg(InternalMessage &&msg) {
  metric_received_internal_msgs_++;

  if (auto *t = std::get_if<FinishPrePrepareExecutionInternalMsg>(&msg)) {
    ConcordAssert(t->prePrepareMsg != nullptr);
    return finishExecutePrePrepareMsg(t->prePrepareMsg, t->pAccumulatedRequests);
  }

  if (auto *rpferMsg = std::get_if<RemovePendingForExecutionRequest>(&msg)) {
    clientsManager->removePendingForExecutionRequest(rpferMsg->clientProxyId, rpferMsg->requestSeqNum);
    return;
  }

  // Handle vaidated messages
  if (auto *vldMsg = std::get_if<CarrierMesssage *>(&msg)) {
    return onCarrierMessage(*vldMsg);
  }

  // Handle a PrePrepare sent by self
  if (auto *ppm = std::get_if<PrePrepareMsg *>(&msg)) {
    // This condition will always be true but keeping this to ensure that we never decrement this variable when it is
    // at its lower bound.
    // Why we do not use ConcordAssert here?
    // Ans: 0 is a correct value for this variable, as this is unsigned, it will never become negative. So check for
    // lower bound and then decrement is the correct idiom for this case.
    if (numOfTransientPreprepareMsgs_ > 0) {
      numOfTransientPreprepareMsgs_--;
    }
    if (isCollectingState() || bftEngine::ControlStateManager::instance().getPruningProcessStatus()) {
      LOG_INFO(GL, "Received PrePrepareMsg while pruning or state transfer, so ignoring the message");
      delete *ppm;
      ppm = nullptr;
      return;
    } else {
      return startConsensusProcess(*ppm, true);
    }
  }

  // Handle a PrePrepare carrier
  if (auto *ppcim = std::get_if<PrePrepareCarrierInternalMsg>(&msg)) {
    if (isCollectingState() || bftEngine::ControlStateManager::instance().getPruningProcessStatus()) {
      LOG_INFO(GL, "Received PrePrepareCarrierInternalMsg while pruning or state transfer, so ignoring the message");
      delete ppcim->ppm_;
      ppcim->ppm_ = nullptr;
      return;
    } else {
      return onMessage(PrePrepareMsgUPtr(ppcim->ppm_));
    }
  }

  // Handle a view change indicator and trigger a view change
  if (auto *vciim = std::get_if<ViewChangeIndicatorInternalMsg>(&msg)) {
    if ((vciim->fromView_ != getCurrentView()) || isCollectingState() ||
        bftEngine::ControlStateManager::instance().getPruningProcessStatus()) {
      LOG_INFO(GL,
               "Ignoring the Received ViewChangeIndicatorInternalMsg while pruning or state transfer or view is "
               "already changed"
                   << KVLOG(vciim->fromView_, getCurrentView()));
      return;
    } else {
      // trigger view change
      LOG_WARN(VC_LOG, "PreProcessResult Signature failure. Ask to leave view" << KVLOG(getCurrentView()));
      askToLeaveView(vciim->reasonToLeave_);
      return;
    }
  }
  // Handle prepare related internal messages
  if (auto *csf = std::get_if<CombinedSigFailedInternalMsg>(&msg)) {
    return onPrepareCombinedSigFailed(csf->seqNumber, csf->view, csf->replicasWithBadSigs);
  }
  if (auto *css = std::get_if<CombinedSigSucceededInternalMsg>(&msg)) {
    return onPrepareCombinedSigSucceeded(
        css->seqNumber, css->view, css->combinedSig.data(), css->combinedSig.size(), css->span_context_);
  }
  if (auto *vcs = std::get_if<VerifyCombinedSigResultInternalMsg>(&msg)) {
    return onPrepareVerifyCombinedSigResult(vcs->seqNumber, vcs->view, vcs->isValid);
  }

  // Handle Commit related internal messages
  if (auto *ccss = std::get_if<CombinedCommitSigSucceededInternalMsg>(&msg)) {
    return onCommitCombinedSigSucceeded(
        ccss->seqNumber, ccss->view, ccss->combinedSig.data(), ccss->combinedSig.size(), ccss->span_context_);
  }
  if (auto *ccsf = std::get_if<CombinedCommitSigFailedInternalMsg>(&msg)) {
    return onCommitCombinedSigFailed(ccsf->seqNumber, ccsf->view, CommitPath::SLOW, ccsf->replicasWithBadSigs);
  }
  if (auto *vccs = std::get_if<VerifyCombinedCommitSigResultInternalMsg>(&msg)) {
    return onCommitVerifyCombinedSigResult(vccs->seqNumber, vccs->view, vccs->isValid);
  }

  // Handle Fast Path Commit related internal messages
  if (auto *ccss = std::get_if<FastPathCombinedCommitSigSucceededInternalMsg>(&msg)) {
    return onFastPathCommitCombinedSigSucceeded(ccss->seqNumber,
                                                ccss->view,
                                                ccss->commitPath,
                                                ccss->combinedSig.data(),
                                                ccss->combinedSig.size(),
                                                ccss->span_context_);
  }
  if (auto *ccsf = std::get_if<FastPathCombinedCommitSigFailedInternalMsg>(&msg)) {
    return onCommitCombinedSigFailed(ccsf->seqNumber, ccsf->view, ccsf->commitPath, ccsf->replicasWithBadSigs);
  }
  if (auto *vccs = std::get_if<FastPathVerifyCombinedCommitSigResultInternalMsg>(&msg)) {
    return onFastPathCommitVerifyCombinedSigResult(vccs->seqNumber, vccs->view, vccs->commitPath, vccs->isValid);
  }

  // Handle a response from a RetransmissionManagerJob
  if (auto *rpr = std::get_if<RetranProcResultInternalMsg>(&msg)) {
    onRetransmissionsProcessingResults(rpr->lastStableSeqNum, rpr->view, rpr->suggestedRetransmissions);
    return retransmissionsManager->OnProcessingComplete();
  }

  // Handle a status request for the diagnostics subsystem
  if (auto *get_status = std::get_if<GetStatus>(&msg)) {
    return onInternalMsg(*get_status);
  }

  if (auto *tick = std::get_if<TickInternalMsg>(&msg)) {
    return ticks_gen_->onInternalTick(*tick);
  }

  ConcordAssert(false);
}

std::string ReplicaImp::getReplicaLastStableSeqNum() const {
  nlohmann::json j;
  j["sequenceNumbers"]["lastStableSeqNum"] = lastStableSeqNum;
  return j.dump();
}

std::string ReplicaImp::getReplicaSequenceNumbers() const {
  nlohmann::json j;
  j["sequenceNumbers"]["lastStableSeqNum"] = std::to_string(lastStableSeqNum);
  j["sequenceNumbers"]["lastExecutedSeqNum"] = std::to_string(getLastExecutedSeqNum());
  return j.dump();
}

std::string ReplicaImp::getReplicaState() const {
  auto primary = getReplicasInfo().primaryOfView(getCurrentView());
  concordUtils::BuildJson bj;

  bj.startJson();

  bj.addKv("replicaID", std::to_string(getReplicasInfo().myId()));
  bj.addKv("primary", std::to_string(primary));

  bj.startNested("viewChange");
  bj.addKv(getName(viewChangeProtocolEnabled), viewChangeProtocolEnabled);
  bj.addKv(getName(autoPrimaryRotationEnabled), autoPrimaryRotationEnabled);
  bj.addKv("curView", getCurrentView());
  bj.addKv(getName(timeOfLastViewEntrance), utcstr(timeOfLastViewEntrance));
  bj.addKv(getName(lastAgreedView), lastAgreedView);
  bj.addKv(getName(timeOfLastAgreedView), utcstr(timeOfLastAgreedView));
  bj.addKv(getName(viewChangeTimerMilli), viewChangeTimerMilli);
  bj.addKv(getName(autoPrimaryRotationTimerMilli), autoPrimaryRotationTimerMilli);
  bj.endNested();

  bj.startNested("sequenceNumbers");
  bj.addKv(getName(primaryLastUsedSeqNum), primaryLastUsedSeqNum);
  bj.addKv(getName(lastStableSeqNum), lastStableSeqNum);
  bj.addKv("lastStableCheckpoint", lastStableSeqNum / checkpointWindowSize);
  bj.addKv(getName(strictLowerBoundOfSeqNums), strictLowerBoundOfSeqNums);
  bj.addKv(getName(maxSeqNumTransferredFromPrevViews), maxSeqNumTransferredFromPrevViews);
  bj.addKv(getName(mainLog->currentActiveWindow().first), mainLog->currentActiveWindow().first);
  bj.addKv(getName(mainLog->currentActiveWindow().second), mainLog->currentActiveWindow().second);
  bj.addKv(getName(lastViewThatTransferredSeqNumbersFullyExecuted), lastViewThatTransferredSeqNumbersFullyExecuted);
  bj.endNested();

  bj.startNested("Other");
  bj.addKv(getName(restarted_), restarted_);
  bj.addKv(getName(requestsQueueOfPrimary.size()), requestsQueueOfPrimary.size());
  bj.addKv(getName(requestsBatcg312her_->getMaxNumberOfPendingRequestsInRecentHistory()),
           reqBatchingLogic_.getMaxNumberOfPendingRequestsInRecentHistory());
  bj.addKv(getName(reqBatchingLogic_->getBatchingFactor()), reqBatchingLogic_.getBatchingFactor());
  bj.addKv(getName(lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas),
           utcstr(lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas));
  bj.addKv(getName(timeOfLastStateSynch), utcstr(timeOfLastStateSynch));
  bj.addKv(getName(recoveringFromExecutionOfRequests), recoveringFromExecutionOfRequests);
  bj.addKv(getName(checkpointsLog->currentActiveWindow().first), checkpointsLog->currentActiveWindow().first);
  bj.addKv(getName(checkpointsLog->currentActiveWindow().second), checkpointsLog->currentActiveWindow().second);
  bj.addKv(getName(clientsManager->numberOfRequiredReservedPages()), clientsManager->numberOfRequiredReservedPages());
  bj.addKv(getName(numInvalidClients), numInvalidClients);
  bj.addKv(getName(numValidNoOps), numValidNoOps);
  bj.endNested();

  bj.endJson();

  return bj.getJson();
}

void ReplicaImp::onInternalMsg(GetStatus &status) const {
  if (status.key == "replica-sequence-numbers") {  // TODO: change this key name (coordinate with deployment)
    return status.output.set_value(getReplicaSequenceNumbers());
  }

  if (status.key == "replica-state") {
    return status.output.set_value(getReplicaState());
  }

  if (status.key == "state-transfer") {
    return status.output.set_value(stateTransfer->getStatus());
  }

  if (status.key == "key-exchange") {
    return status.output.set_value(KeyExchangeManager::instance().getStatus());
  }

  if (status.key == "pre-execution") {
    return status.output.set_value(replStatusHandlers_.preExecutionStatus(getAggregator()));
  }

  // We must always return something to unblock the future.
  return status.output.set_value("** - Invalid Key - **");
}

template <>
void ReplicaImp::onMessage<PreparePartialMsg>(std::unique_ptr<PreparePartialMsg> message) {
  metric_received_prepare_partials_++;
  auto *msg = message.release();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  bool msgAdded = false;
  if (relevantMsgForActiveView(msg)) {
    ConcordAssert(isCurrentPrimary());

    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_INFO(CNSUS, "Received relevant PreparePartialMsg" << KVLOG(msgSender, msgSeqNum, msg->size()));

    controller->onMessage(msg);

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();

    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

    PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();

    if (fcp != nullptr) {
      send(fcp, msgSender);
    } else if (commitFull != nullptr) {
      send(commitFull, msgSender);
    } else if (preFull != nullptr) {
      send(preFull, msgSender);
    } else {
      msgAdded = seqNumInfo.addMsg(msg);
    }
  }

  if (!msgAdded) {
    LOG_INFO(CNSUS,
             "Node " << config_.getreplicaId() << " ignored the PreparePartialMsg from node " << msgSender
                     << " (seqNumber " << msgSeqNum << ")");
    delete msg;
  }
}

template <>
void ReplicaImp::onMessage<CommitPartialMsg>(std::unique_ptr<CommitPartialMsg> message) {
  metric_received_commit_partials_++;
  auto *msg = message.release();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  bool msgAdded = false;
  if (relevantMsgForActiveView(msg)) {
    ConcordAssert(isCurrentPrimary());

    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_INFO(CNSUS, "Received CommitPartialMsg. " << KVLOG(msgSender, msgSeqNum, msg->size()));

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();

    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

    if (fcp != nullptr) {
      send(fcp, msgSender);
    } else if (commitFull != nullptr) {
      send(commitFull, msgSender);
    } else {
      msgAdded = seqNumInfo.addMsg(msg);
    }
  }

  if (!msgAdded) {
    LOG_INFO(CNSUS, "Ignored CommitPartialMsg. " << KVLOG(msgSender));
    delete msg;
  }
}

template <>
void ReplicaImp::onMessage<PrepareFullMsg>(std::unique_ptr<PrepareFullMsg> message) {
  metric_received_prepare_fulls_++;
  auto *msg = message.release();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  bool msgAdded = false;
  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_INFO(CNSUS, "Received PrepareFullMsg. " << KVLOG(msgSender, msgSeqNum, msg->size()));

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();

    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

    PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();

    if (fcp != nullptr) {
      send(fcp, msgSender);
    } else if (commitFull != nullptr) {
      send(commitFull, msgSender);
    } else if (preFull != nullptr) {
      // nop
    } else {
      msgAdded = seqNumInfo.addMsg(msg);
    }
  }

  if (!msgAdded) {
    LOG_INFO(CNSUS, "Ignored PrepareFullMsg." << KVLOG(msgSender));
    delete msg;
  }
}
template <>
void ReplicaImp::onMessage<CommitFullMsg>(std::unique_ptr<CommitFullMsg> message) {
  metric_received_commit_fulls_++;
  auto *msg = message.release();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  bool msgAdded = false;
  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_INFO(CNSUS, "Received CommitFullMsg. " << KVLOG(msgSender, msgSeqNum, msg->size()));

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();

    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

    if (fcp != nullptr) {
      send(fcp,
           msgSender);  // TODO(GG): do we really want to send this message ? (msgSender already has a CommitFullMsg
      // for the same seq number)
    } else if (commitFull != nullptr) {
      // nop
    } else {
      msgAdded = seqNumInfo.addMsg(msg);
    }
  }

  if (!msgAdded) {
    LOG_INFO(CNSUS,
             "Node " << config_.getreplicaId() << " ignored the CommitFullMsg from node " << msgSender << " (seqNumber "
                     << msgSeqNum << ")");
    delete msg;
  }
}

void ReplicaImp::onPrepareCombinedSigFailed(SeqNum seqNumber,
                                            ViewNum view,
                                            const std::set<uint16_t> &replicasWithBadSigs) {
  LOG_WARN(THRESHSIGN_LOG, KVLOG(seqNumber, view));
  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetPrepareSignatures();
    LOG_INFO(CNSUS, "Collecting state, reset prepare signatures");
    return;
  }
  if ((!currentViewIsActive()) || (getCurrentView() != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_WARN(CNSUS, "Dropping irrelevant signature." << KVLOG(seqNumber, view));

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfPrepareSignaturesProcessing(seqNumber, view, replicasWithBadSigs);

  // TODO(GG): add logic that handles bad replicas ...
}

void ReplicaImp::onPrepareCombinedSigSucceeded(SeqNum seqNumber,
                                               ViewNum view,
                                               const char *combinedSig,
                                               uint16_t combinedSigLen,
                                               const concordUtils::SpanContext &span_context) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));
  LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, combinedSigLen));

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetPrepareSignatures();
    LOG_INFO(CNSUS, "Collecting state, reset prepare signatures");
    return;
  }
  if ((!currentViewIsActive()) || (getCurrentView() != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(CNSUS, "Not sending prepare full: Invalid view, or sequence number." << KVLOG(view, getCurrentView()));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfPrepareSignaturesProcessing(seqNumber, view, combinedSig, combinedSigLen, span_context);

  FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();

  PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();

  ConcordAssertNE(preFull, nullptr);

  if (fcp != nullptr) return;  // don't send if we already have FullCommitProofMsg
  LOG_INFO(CNSUS, "Sending prepare full" << KVLOG(view, seqNumber));
  if (ps_) {
    ps_->beginWriteTran();
    ps_->setPrepareFullMsgInSeqNumWindow(seqNumber, preFull);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }

  if (!retransmissionsLogicEnabled) {
    sendToAllOtherReplicas(preFull);
  } else {
    for (ReplicaId x : repsInfo->idsOfPeerReplicas()) {
      sendRetransmittableMsgToReplica(preFull, x, seqNumber);
    }
  }

  ConcordAssert(seqNumInfo.isPrepared());

  sendCommitPartial(seqNumber);
}

void ReplicaImp::onPrepareVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));

  LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, isValid));

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetPrepareSignatures();
    LOG_INFO(CNSUS, "Collecting state, reset prepare signatures");
    return;
  }

  if ((!currentViewIsActive()) || (getCurrentView() != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(CNSUS,
             "Not sending commit partial: Invalid view, or sequence number."
                 << KVLOG(seqNumber, view, getCurrentView(), mainLog->insideActiveWindow(seqNumber)));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCombinedPrepareSigVerification(seqNumber, view, isValid);

  if (!isValid) return;  // TODO(GG): we should do something about the replica that sent this invalid message

  FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();

  if (fcp != nullptr) return;  // don't send if we already have FullCommitProofMsg

  ConcordAssert(seqNumInfo.isPrepared());

  if (ps_) {
    PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();
    ConcordAssertNE(preFull, nullptr);
    ps_->beginWriteTran();
    ps_->setPrepareFullMsgInSeqNumWindow(seqNumber, preFull);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }

  sendCommitPartial(seqNumber);
}

void ReplicaImp::onCommitCombinedSigFailed(SeqNum seqNumber,
                                           ViewNum view,
                                           CommitPath cPath,
                                           const std::set<uint16_t> &replicasWithBadSigs) {
  LOG_WARN(THRESHSIGN_LOG,
           "Commit combined sig failed (" << CommitPathToStr(cPath) << ") "
                                          << KVLOG(seqNumber, viewChangeProtocolEnabled, replicasWithBadSigs.size()));

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetCommitSignatures(cPath);
    LOG_INFO(CNSUS, "Collecting state, reset commit signatures");
    return;
  }

  if ((!currentViewIsActive()) || (getCurrentView() != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(CNSUS, "Invalid view, or sequence number." << KVLOG(seqNumber, view, getCurrentView()));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCommitSignaturesProcessing(seqNumber, view, cPath, replicasWithBadSigs);

  // TODO(GG): add logic that handles bad replicas ...
}

void ReplicaImp::onCommitCombinedSigSucceeded(SeqNum seqNumber,
                                              ViewNum view,
                                              const char *combinedSig,
                                              uint16_t combinedSigLen,
                                              const concordUtils::SpanContext &span_context) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));
  LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, combinedSigLen));

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetCommitSignatures(CommitPath::SLOW);
    LOG_INFO(CNSUS, "Collecting state, reset commit signatures");
    return;
  }

  if ((!currentViewIsActive()) || (getCurrentView() != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(CNSUS,
             "Not sending full commit: Invalid view, or sequence number."
                 << KVLOG(view, getCurrentView(), mainLog->insideActiveWindow(seqNumber)));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCommitSignaturesProcessing(
      seqNumber, view, CommitPath::SLOW, combinedSig, combinedSigLen, span_context);

  FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();
  CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

  ConcordAssertNE(commitFull, nullptr);
  if (fcp != nullptr) return;  // ignore if we already have FullCommitProofMsg
  LOG_INFO(CNSUS, "Sending full commit." << KVLOG(view, seqNumber));
  if (ps_) {
    ps_->beginWriteTran();
    ps_->setCommitFullMsgInSeqNumWindow(seqNumber, commitFull);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }

  if (!retransmissionsLogicEnabled) {
    sendToAllOtherReplicas(commitFull);
  } else {
    for (ReplicaId x : repsInfo->idsOfPeerReplicas()) {
      sendRetransmittableMsgToReplica(commitFull, x, seqNumber);
    }
  }

  ConcordAssert(seqNumInfo.isCommitted__gg());

  bool askForMissingInfoAboutCommittedItems =
      (seqNumber > lastExecutedSeqNum + config_.getconcurrencyLevel() + activeExecutions_);

  auto span = concordUtils::startChildSpanFromContext(
      commitFull->spanContext<std::remove_pointer<decltype(commitFull)>::type>(), "bft_execute_committed_reqs");
  updateCommitMetrics(CommitPath::SLOW);
  startExecution(seqNumber, span, askForMissingInfoAboutCommittedItems);
}

void ReplicaImp::onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  if (!isValid) {
    LOG_WARN(THRESHSIGN_LOG, KVLOG(seqNumber, view, isValid));
  } else {
    LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, isValid));
  }

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetCommitSignatures(CommitPath::SLOW);
    LOG_INFO(CNSUS, "Collecting state, reset commit signatures");
    return;
  }

  if ((!currentViewIsActive()) || (getCurrentView() != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(
        CNSUS,
        "Invalid view, or sequence number." << KVLOG(view, getCurrentView(), mainLog->insideActiveWindow(seqNumber)));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCombinedCommitSigVerification(seqNumber, view, CommitPath::SLOW, isValid);

  if (!isValid) return;  // TODO(GG): we should do something about the replica that sent this invalid message

  ConcordAssert(seqNumInfo.isCommitted__gg());

  CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();
  ConcordAssertNE(commitFull, nullptr);
  if (ps_) {
    ps_->beginWriteTran();
    ps_->setCommitFullMsgInSeqNumWindow(seqNumber, commitFull);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }
  LOG_INFO(CNSUS, "Request committed, proceeding to try to execute" << KVLOG(view));

  auto span = concordUtils::startChildSpanFromContext(
      commitFull->spanContext<std::remove_pointer<decltype(commitFull)>::type>(), "bft_execute_committed_reqs");
  bool askForMissingInfoAboutCommittedItems =
      (seqNumber > lastExecutedSeqNum + config_.getconcurrencyLevel() + activeExecutions_);
  updateCommitMetrics(CommitPath::SLOW);
  startExecution(seqNumber, span, askForMissingInfoAboutCommittedItems);
}

void ReplicaImp::onFastPathCommitCombinedSigSucceeded(SeqNum seqNumber,
                                                      ViewNum view,
                                                      CommitPath cPath,
                                                      const char *combinedSig,
                                                      uint16_t combinedSigLen,
                                                      const concordUtils::SpanContext &span_context) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
  SCOPED_MDC_PATH(CommitPathToMDCString(cPath));
  LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, combinedSigLen));

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetCommitSignatures(cPath);
    LOG_INFO(CNSUS, "Collecting state, reset commit signatures");
    return;
  }

  if ((!currentViewIsActive()) || (getCurrentView() != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(CNSUS,
             "Not sending fast path full commit: Invalid view, or sequence number."
                 << KVLOG(view, getCurrentView(), mainLog->insideActiveWindow(seqNumber)));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();
  // One replica produces FullCommitProofMsg Immediately
  ConcordAssert(fcp == nullptr || config_.numReplicas != 1);

  seqNumInfo.onCompletionOfCommitSignaturesProcessing(
      seqNumber, view, cPath, combinedSig, combinedSigLen, span_context);

  ConcordAssert(seqNumInfo.hasPrePrepareMsg());
  seqNumInfo.forceComplete();  // TODO(GG): remove forceComplete() (we know that  seqNumInfo is committed because
  // of the  FullCommitProofMsg message)

  fcp = seqNumInfo.getFastPathFullCommitProofMsg();
  ConcordAssert(fcp != nullptr);

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setFullCommitProofMsgInSeqNumWindow(seqNumber, fcp);
    ps_->setForceCompletedInSeqNumWindow(seqNumber, true);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }

  // We've created the full commit proof (we're the collector) - send to other replicas
  ConcordAssert(fcp->senderId() == config_.getreplicaId());
  sendToAllOtherReplicas(fcp);

  const bool askForMissingInfoAboutCommittedItems =
      (seqNumber >
       lastExecutedSeqNum + config_.getconcurrencyLevel() + activeExecutions_);  // TODO(GG): check/improve this logic

  auto span = concordUtils::startChildSpanFromContext(fcp->spanContext<std::remove_pointer<decltype(fcp)>::type>(),
                                                      "bft_execute_committed_reqs");
  updateCommitMetrics(cPath);
  pm_->Delay<concord::performance::SlowdownPhase::ConsensusFullCommitMsgProcess>();
  startExecution(seqNumber, span, askForMissingInfoAboutCommittedItems);
}

void ReplicaImp::onFastPathCommitVerifyCombinedSigResult(SeqNum seqNumber,
                                                         ViewNum view,
                                                         CommitPath cPath,
                                                         bool isValid) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
  SCOPED_MDC_PATH(CommitPathToMDCString(cPath));

  if (!isValid) {
    LOG_WARN(
        THRESHSIGN_LOG,
        "Commit combined sig verify failed (" << CommitPathToStr(cPath) << ") " << KVLOG(seqNumber, view, isValid));
  } else {
    LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, isValid));
  }

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetCommitSignatures(cPath);
    LOG_INFO(CNSUS, "Collecting state, reset fast path commit signatures");
    return;
  }

  if ((!currentViewIsActive()) || (getCurrentView() != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(
        CNSUS,
        "Invalid view, or sequence number." << KVLOG(view, getCurrentView(), mainLog->insideActiveWindow(seqNumber)));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCombinedCommitSigVerification(seqNumber, view, cPath, isValid);

  if (!isValid) return;  // TODO(GG): we should do something about the replica that sent this invalid message

  ConcordAssert(seqNumInfo.hasPrePrepareMsg());
  seqNumInfo.forceComplete();  // TODO(GG): remove forceComplete() (we know that  seqNumInfo is committed because
  // of the  FullCommitProofMsg message)

  FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();
  ConcordAssert(fcp != nullptr);

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setFullCommitProofMsgInSeqNumWindow(seqNumber, fcp);
    ps_->setForceCompletedInSeqNumWindow(seqNumber, true);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }

  // We've received a full commit proof from a collector - don't send to other replicas
  ConcordAssert(fcp->senderId() != config_.getreplicaId());

  const bool askForMissingInfoAboutCommittedItems =
      (seqNumber >
       lastExecutedSeqNum + config_.getconcurrencyLevel() + activeExecutions_);  // TODO(GG): check/improve this logic

  auto span = concordUtils::startChildSpanFromContext(fcp->spanContext<std::remove_pointer<decltype(fcp)>::type>(),
                                                      "bft_execute_committed_reqs");
  updateCommitMetrics(cPath);
  pm_->Delay<concord::performance::SlowdownPhase::ConsensusFullCommitMsgProcess>();
  startExecution(seqNumber, span, askForMissingInfoAboutCommittedItems);
}

template <>
void ReplicaImp::onMessage<CheckpointMsg>(std::unique_ptr<CheckpointMsg> message) {
  auto *msg = message.release();
  if (activeExecutions_ > 0) {
    pushDeferredMessage(msg);
    return;
  }
  metric_received_checkpoints_++;
  const ReplicaId msgSenderId = msg->senderId();
  const ReplicaId msgGenReplicaId = msg->idOfGeneratedReplica();
  const SeqNum msgSeqNum = msg->seqNumber();
  const EpochNum msgEpochNum = msg->epochNumber();
  const CheckpointMsg::State msgState = msg->state();
  const Digest stateDigest = msg->stateDigest();
  const Digest reservedPagesDigest = msg->reservedPagesDigest();
  const Digest rvbDataDigest = msg->rvbDataDigest();
  const bool msgIsStable = msg->isStableState();
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_INFO(GL,
           "Received checkpoint message from node. " << KVLOG(msgSenderId,
                                                              msgGenReplicaId,
                                                              msgSeqNum,
                                                              msgEpochNum,
                                                              msg->size(),
                                                              msgIsStable,
                                                              msgState,
                                                              stateDigest,
                                                              reservedPagesDigest,
                                                              rvbDataDigest));
  LOG_INFO(GL, KVLOG(lastStableSeqNum, lastExecutedSeqNum, getSelfEpochNumber()));
  if ((msgSeqNum > lastStableSeqNum) && (msgSeqNum <= lastStableSeqNum + kWorkWindowSize) &&
      (msgEpochNum >= getSelfEpochNumber())) {
    ConcordAssert(mainLog->insideActiveWindow(msgSeqNum));
    auto &checkInfo = checkpointsLog->get(msgSeqNum);
    bool msgAdded = checkInfo.addCheckpointMsg(msg, msgGenReplicaId);

    if (msgAdded) {
      LOG_DEBUG(GL, "Added checkpoint message: " << KVLOG(msgGenReplicaId));
    }

    if (checkInfo.isCheckpointCertificateComplete()) {  // 2f + 1
      ConcordAssertNE(checkInfo.selfCheckpointMsg(), nullptr);
      onSeqNumIsStable(msgSeqNum);

      return;
    }
  } else if (checkpointsLog->insideActiveWindow(msgSeqNum)) {
    auto &checkInfo = checkpointsLog->get(msgSeqNum);
    bool msgAdded = checkInfo.addCheckpointMsg(msg, msgGenReplicaId);
    if (msgAdded && checkInfo.isCheckpointSuperStable()) {
      onSeqNumIsSuperStable(msgSeqNum);
    }
  } else {
    delete msg;
  }

  bool askForStateTransfer = false;
  std::string_view startStReason;
  if (msgIsStable && ((msgEpochNum == getSelfEpochNumber() && msgSeqNum > lastExecutedSeqNum + activeExecutions_) ||
                      msgEpochNum > getSelfEpochNumber())) {
    auto pos = tableOfStableCheckpoints.find(msgGenReplicaId);
    if (pos == tableOfStableCheckpoints.end() || pos->second->seqNumber() <= msgSeqNum ||
        msgEpochNum >= pos->second->epochNumber()) {
      // <= to allow repeating checkpoint message since state transfer may not kick in when we are inside active
      // window
      auto stableCheckpointMsg = std::make_shared<CheckpointMsg>(
          msgGenReplicaId, msgSeqNum, msgState, stateDigest, reservedPagesDigest, rvbDataDigest, msgIsStable);
      stableCheckpointMsg->setEpochNumber(msgEpochNum);
      tableOfStableCheckpoints[msgGenReplicaId] = stableCheckpointMsg;
      LOG_INFO(GL,
               "Added stable Checkpoint message to tableOfStableCheckpoints: " << KVLOG(msgSenderId, msgGenReplicaId));
      for (auto &[r, cp] : tableOfStableCheckpoints) {
        if (cp->seqNumber() == msgSeqNum && (cp->reservedPagesDigest() != stableCheckpointMsg->reservedPagesDigest() ||
                                             cp->rvbDataDigest() != stableCheckpointMsg->rvbDataDigest())) {
          metric_indicator_of_non_determinism_++;
          LOG_ERROR(GL,
                    "Detect non determinism, for checkpoint: "
                        << msgSeqNum << " [replica: " << r << ", reservedPagesDigest: " << cp->reservedPagesDigest()
                        << ", rvbDataDigest: " << cp->rvbDataDigest() << "] Vs [replica: " << msgGenReplicaId
                        << ", sender: " << msgSenderId
                        << ", reservedPagesDigest: " << stableCheckpointMsg->reservedPagesDigest()
                        << ", rvbDataDigest: " << stableCheckpointMsg->rvbDataDigest() << "]");
        }
      }
      if ((uint16_t)tableOfStableCheckpoints.size() >= config_.getfVal() + 1) {
        uint16_t numRelevant = 0;
        uint16_t numRelevantAboveWindow = 0;
        auto tableItrator = tableOfStableCheckpoints.begin();
        while (tableItrator != tableOfStableCheckpoints.end()) {
          if (tableItrator->second->seqNumber() <= lastExecutedSeqNum &&
              tableItrator->second->epochNumber() == getSelfEpochNumber()) {
            tableItrator = tableOfStableCheckpoints.erase(tableItrator);
          } else {
            numRelevant++;
            if (tableItrator->second->seqNumber() > lastStableSeqNum + kWorkWindowSize ||
                tableItrator->second->epochNumber() > getSelfEpochNumber())
              numRelevantAboveWindow++;
            tableItrator++;
          }
        }
        ConcordAssertEQ(numRelevant, tableOfStableCheckpoints.size());

        LOG_DEBUG(GL, KVLOG(numRelevant, numRelevantAboveWindow));

        if (numRelevantAboveWindow >= config_.getfVal() + 1) {
          LOG_INFO(GL, "Number of stable checkpoints above window: " << numRelevantAboveWindow);
          askForStateTransfer = true;
          startStReason = "Replica is out of consensus window";

          // For debug
          std::ostringstream oss;
          oss << KVLOG(lastStableSeqNum, kWorkWindowSize, lastExecutedSeqNum, config_.getfVal());
          for (const auto &[replicaId, cp] : tableOfStableCheckpoints) {
            const auto &seqNo = cp->seqNumber();
            const auto &epochNum = cp->epochNumber();
            oss << "[" << KVLOG(replicaId, seqNo, epochNum) << "],";
          }
          LOG_INFO(GL, oss.str());

        } else if (numRelevant >= config_.getfVal() + 1) {
          static uint32_t maxTimeSinceLastExecutionInMainWindowMs =
              config_.get<uint32_t>("concord.bft.st.maxTimeSinceLastExecutionInMainWindowMs", 5000);

          Time timeOfLastEcecution = MinTime;
          if (mainLog->insideActiveWindow(lastExecutedSeqNum))
            timeOfLastEcecution = mainLog->get(lastExecutedSeqNum).lastUpdateTimeOfCommitMsgs();
          if ((getMonotonicTime() - timeOfLastEcecution) > (milliseconds(maxTimeSinceLastExecutionInMainWindowMs))) {
            LOG_INFO(GL,
                     "Number of stable checkpoints in current window: "
                         << numRelevant << " time since last execution: "
                         << (getMonotonicTime() - timeOfLastEcecution).count() << " ms");
            askForStateTransfer = true;
            startStReason = "Too much time has passed since last execution";
          }
        }
      }
    }
  }

  if (askForStateTransfer && !isCollectingState()) {
    if (activeExecutions_ > 0)
      isStartCollectingState_ = true;
    else {
      startCollectingState(startStReason);
    }
  } else if (msgSenderId == msgGenReplicaId) {
    if (msgSeqNum > lastStableSeqNum + kWorkWindowSize) {
      onReportAboutAdvancedReplica(msgGenReplicaId, msgSeqNum);
    } else if (msgSeqNum + kWorkWindowSize < lastStableSeqNum) {
      onReportAboutLateReplica(msgGenReplicaId, msgSeqNum);
    }
  }
}
/**
 * Is sent from a read-only replica
 */
template <>
void ReplicaImp::onMessage<AskForCheckpointMsg>(std::unique_ptr<AskForCheckpointMsg> msg) {
  // metric_received_checkpoints_++; // TODO [TK]
  LOG_INFO(GL, "Received AskForCheckpoint message:" << KVLOG(msg->senderId(), lastStableSeqNum));
  const auto &checkpointInfo = checkpointsLog->get(lastStableSeqNum);
  CheckpointMsg *checkpointMsg = checkpointInfo.selfCheckpointMsg();

  if (checkpointMsg == nullptr) {
    LOG_INFO(GL, "This replica does not have the current checkpoint." << KVLOG(msg->senderId(), lastStableSeqNum));
  } else {
    // TODO [TK] check if already sent within a configurable time period
    auto destination = msg->senderId();
    LOG_INFO(GL, "Sending CheckpointMsg:" << KVLOG(destination));
    send(checkpointMsg, msg->senderId());
  }
}

void ReplicaImp::startExecution(SeqNum seqNumber,
                                concordUtils::SpanWrapper &span,
                                bool askForMissingInfoAboutCommittedItems) {
  if (isCurrentPrimary()) {
    metric_consensus_duration_.finishMeasurement(seqNumber);
    metric_post_exe_duration_.addStartTimeStamp(seqNumber);
    metric_consensus_end_to_core_exe_duration_.addStartTimeStamp(seqNumber);
  }

  consensus_times_.end(seqNumber);
  tryToRemovePendingRequestsForSeqNum(seqNumber);  // TODO(LG) Should verify if needed
  LOG_INFO(CNSUS, "Starting execution of seqNumber:" << seqNumber);
  if (config_.enablePostExecutionSeparation) {
    tryToStartOrFinishExecution(askForMissingInfoAboutCommittedItems);
  } else {
    executeNextCommittedRequests(span, askForMissingInfoAboutCommittedItems);
  }
}

void ReplicaImp::pushDeferredMessage(MessageBase *m) {
  if (deferredMessages_.size() < maxQueueSize_) {
    deferredMessages_.push_back(m);
    deferredMessagesMetric_++;
  } else {
    delete m;
  }
}

bool ReplicaImp::handledByRetransmissionsManager(const ReplicaId sourceReplica,
                                                 const ReplicaId destReplica,
                                                 const ReplicaId primaryReplica,
                                                 const SeqNum seqNum,
                                                 const uint16_t msgType) {
  ConcordAssert(retransmissionsLogicEnabled);

  if (sourceReplica == destReplica) return false;

  const bool sourcePrimary = (sourceReplica == primaryReplica);

  if (sourcePrimary && ((msgType == MsgCode::PrePrepare) || (msgType == MsgCode::StartSlowCommit))) return true;

  const bool dstPrimary = (destReplica == primaryReplica);

  if (dstPrimary && ((msgType == MsgCode::PreparePartial) || (msgType == MsgCode::CommitPartial))) return true;

  //  TODO(GG): do we want to use acks for FullCommitProofMsg ?

  if (msgType == MsgCode::PartialCommitProof) {
    const bool destIsCollector =
        repsInfo->getCollectorsForPartialProofs(destReplica, getCurrentView(), seqNum, nullptr, nullptr);
    if (destIsCollector) return true;
  }

  return false;
}

void ReplicaImp::sendAckIfNeeded(MessageBase *msg, const NodeIdType sourceNode, const SeqNum seqNum) {
  if (!retransmissionsLogicEnabled) return;

  if (!repsInfo->isIdOfPeerReplica(sourceNode)) return;

  if (handledByRetransmissionsManager(sourceNode, config_.getreplicaId(), currentPrimary(), seqNum, msg->type())) {
    SimpleAckMsg *ackMsg = new SimpleAckMsg(seqNum, getCurrentView(), config_.getreplicaId(), msg->type());

    send(ackMsg, sourceNode);

    delete ackMsg;
  }
}

void ReplicaImp::sendRetransmittableMsgToReplica(MessageBase *msg,
                                                 ReplicaId destReplica,
                                                 SeqNum s,
                                                 bool ignorePreviousAcks) {
  send(msg, destReplica);

  if (!retransmissionsLogicEnabled) return;

  if (handledByRetransmissionsManager(config_.getreplicaId(), destReplica, currentPrimary(), s, msg->type()))
    retransmissionsManager->onSend(destReplica, s, msg->type(), ignorePreviousAcks);
}

void ReplicaImp::onRetransmissionsTimer(Timers::Handle timer) {
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;
  ConcordAssert(retransmissionsLogicEnabled);

  retransmissionsManager->tryToStartProcessing();
}

void ReplicaImp::onRetransmissionsProcessingResults(SeqNum relatedLastStableSeqNum,
                                                    const ViewNum relatedViewNumber,
                                                    const std::forward_list<RetSuggestion> &suggestedRetransmissions) {
  ConcordAssert(retransmissionsLogicEnabled);

  if (isCollectingState() || (relatedViewNumber != getCurrentView()) || (!currentViewIsActive())) return;
  if (relatedLastStableSeqNum + kWorkWindowSize <= lastStableSeqNum) return;

  const uint16_t myId = config_.getreplicaId();
  const uint16_t primaryId = currentPrimary();

  for (const RetSuggestion &s : suggestedRetransmissions) {
    if ((s.msgSeqNum <= lastStableSeqNum) || (s.msgSeqNum > lastStableSeqNum + kWorkWindowSize)) continue;

    ConcordAssertNE(s.replicaId, myId);

    ConcordAssert(handledByRetransmissionsManager(myId, s.replicaId, primaryId, s.msgSeqNum, s.msgType));

    switch (s.msgType) {
      case MsgCode::PrePrepare: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PrePrepareMsg *msgToSend = seqNumInfo.getSelfPrePrepareMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(CNSUS,
                  "Replica " << myId << " retransmits to replica " << s.replicaId << " PrePrepareMsg with seqNumber "
                             << s.msgSeqNum);
      } break;
      case MsgCode::PartialCommitProof: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PartialCommitProofMsg *msgToSend = seqNumInfo.getFastPathSelfPartialCommitProofMsg();
        ConcordAssertNE(msgToSend, nullptr);
        if (seqNumInfo.isTimeCorrect()) {
          sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
          LOG_DEBUG(CNSUS,
                    "Replica " << myId << " retransmits to replica " << s.replicaId
                               << " PartialCommitProofMsg with seqNumber " << s.msgSeqNum);
        }
      } break;
        //  TODO(GG): do we want to use acks for FullCommitProofMsg ?
      case MsgCode::StartSlowCommit: {
        StartSlowCommitMsg *msgToSend = new StartSlowCommitMsg(myId, getCurrentView(), s.msgSeqNum);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        delete msgToSend;
        LOG_DEBUG(CNSUS,
                  "Replica " << myId << " retransmits to replica " << s.replicaId
                             << " StartSlowCommitMsg with seqNumber " << s.msgSeqNum);
      } break;
      case MsgCode::PreparePartial: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PreparePartialMsg *msgToSend = seqNumInfo.getSelfPreparePartialMsg();
        ConcordAssertNE(msgToSend, nullptr);
        if (seqNumInfo.isTimeCorrect()) {
          sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
          LOG_DEBUG(CNSUS,
                    "Replica " << myId << " retransmits to replica " << s.replicaId
                               << " PreparePartialMsg with seqNumber " << s.msgSeqNum);
        }
      } break;
      case MsgCode::PrepareFull: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PrepareFullMsg *msgToSend = seqNumInfo.getValidPrepareFullMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(CNSUS,
                  "Replica " << myId << " retransmits to replica " << s.replicaId << " PrepareFullMsg with seqNumber "
                             << s.msgSeqNum);
      } break;

      case MsgCode::CommitPartial: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        CommitPartialMsg *msgToSend = seqNumInfo.getSelfCommitPartialMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(CNSUS,
                  "Replica " << myId << " retransmits to replica " << s.replicaId << " CommitPartialMsg with seqNumber "
                             << s.msgSeqNum);
      } break;

      case MsgCode::CommitFull: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        CommitFullMsg *msgToSend = seqNumInfo.getValidCommitFullMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_INFO(CNSUS, "Retransmit CommitFullMsg: " << KVLOG(s.msgSeqNum, s.replicaId));
      } break;

      default:
        ConcordAssert(false);
    }
  }
}

template <>
void ReplicaImp::onMessage<ReplicaStatusMsg>(std::unique_ptr<ReplicaStatusMsg> message) {
  metric_received_replica_statuses_++;
  // TODO(GG): we need filter for msgs (to avoid denial of service attack) + avoid sending messages at a high rate.
  // TODO(GG): for some communication modules/protocols, we can also utilize information about
  // connection/disconnection.

  auto *msg = message.release();
  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgLastStable = msg->getLastStableSeqNum();
  const ViewNum msgViewNum = msg->getViewNumber();

  LOG_DEBUG(CNSUS, KVLOG(msgSenderId, msgLastStable, msgViewNum, lastStableSeqNum));

  /////////////////////////////////////////////////////////////////////////
  // Checkpoints
  /////////////////////////////////////////////////////////////////////////

  if ((lastStableSeqNum > msgLastStable + kWorkWindowSize) ||
      (!currentViewIsActive() && (lastStableSeqNum > msgLastStable))) {
    CheckpointMsg *checkMsg = checkpointsLog->get(lastStableSeqNum).selfCheckpointMsg();

    if (checkMsg == nullptr || !checkMsg->isStableState()) {
      LOG_WARN(
          CNSUS,
          "Misalignment in lastStableSeqNum and my CheckpointMsg for it" << KVLOG(lastStableSeqNum, msgLastStable));
    }

    auto &checkpointInfo = checkpointsLog->get(lastStableSeqNum);
    for (const auto &it : checkpointInfo.getAllCheckpointMsgs()) {
      if (msgSenderId != it.first) {
        sendAndIncrementMetric(it.second, msgSenderId, metric_sent_checkpoint_msg_due_to_status_);
      }
    }
  } else if (msgLastStable > lastStableSeqNum + kWorkWindowSize) {
    tryToSendStatusReport();  // ask for help
  } else {
    if (lastStableSeqNum != 0 && msgLastStable == lastStableSeqNum) {
      // Maybe the sender needs to get to an n/n checkpoint
      CheckpointMsg *checkMsg = checkpointsLog->get(msgLastStable).selfCheckpointMsg();
      if (checkMsg != nullptr) {
        sendAndIncrementMetric(checkMsg, msgSenderId, metric_sent_checkpoint_msg_due_to_status_);
      }
    }
    // Send checkpoints that may be useful for msgSenderId
    const SeqNum beginRange =
        std::max(checkpointsLog->currentActiveWindow().first, msgLastStable + checkpointWindowSize);
    const SeqNum endRange = std::min(checkpointsLog->currentActiveWindow().second, msgLastStable + kWorkWindowSize);

    ConcordAssertEQ(beginRange % checkpointWindowSize, 0);

    if (beginRange <= endRange) {
      ConcordAssertLE(endRange - beginRange, kWorkWindowSize);

      for (SeqNum i = beginRange; i <= endRange; i = i + checkpointWindowSize) {
        CheckpointMsg *checkMsg = checkpointsLog->get(i).selfCheckpointMsg();
        if (checkMsg != nullptr) {
          sendAndIncrementMetric(checkMsg, msgSenderId, metric_sent_checkpoint_msg_due_to_status_);
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId in older view
  /////////////////////////////////////////////////////////////////////////

  if (msgViewNum < getCurrentView()) {
    ViewChangeMsg *myVC = viewsManager->getMyLatestViewChangeMsg();
    ConcordAssertNE(myVC, nullptr);  // because curView>0
    sendAndIncrementMetric(myVC, msgSenderId, metric_sent_viewchange_msg_due_to_status_);
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId needs an information to enter view curView
  /////////////////////////////////////////////////////////////////////////

  else if ((msgViewNum == getCurrentView()) && (!msg->currentViewIsActive())) {
    auto sendViewChangeMsgs = [&msg, &msgSenderId, this]() {
      // Send all View Change messages we have. We only have ViewChangeMsg for View > 0
      if (getCurrentView() > 0 && msg->hasListOfMissingViewChangeMsgForViewChange()) {
        for (auto *vcMsg : viewsManager->getViewChangeMsgsForView(getCurrentView())) {
          if (msg->isMissingViewChangeMsgForViewChange(vcMsg->idOfGeneratedReplica())) {
            sendAndIncrementMetric(vcMsg, msgSenderId, metric_sent_viewchange_msg_due_to_status_);
          }
        }
      }
    };

    if (isCurrentPrimary() || (repsInfo->primaryOfView(getCurrentView()) == msgSenderId))  // if the primary is involved
    {
      if (isCurrentPrimary())  // I am the primary of curView
      {
        // send NewViewMsg for View > 0
        if (getCurrentView() > 0 && !msg->currentViewHasNewViewMessage() &&
            viewsManager->viewIsActive(getCurrentView())) {
          NewViewMsg *nv = viewsManager->getMyNewViewMsgForCurrentView();
          ConcordAssertNE(nv, nullptr);
          sendAndIncrementMetric(nv, msgSenderId, metric_sent_newview_msg_due_to_status_);
        }
      }
      // Send all VC msgs that can help to make  progress (needed because the original senders may not send
      // the ViewChangeMsg msgs used by the primary). If viewsManager->viewIsActive(getCurrentView()), we can send only
      // the VC msgs which are really needed for curView (see in ViewsManager).
      sendViewChangeMsgs();

      if (viewsManager->viewIsActive(getCurrentView())) {
        if (msg->hasListOfMissingPrePrepareMsgForViewChange()) {
          for (SeqNum i = msgLastStable + 1; i <= msgLastStable + kWorkWindowSize; i++) {
            if (mainLog->insideActiveWindow(i) && msg->isMissingPrePrepareMsgForViewChange(i)) {
              PrePrepareMsg *prePrepareMsg = mainLog->get(i).getPrePrepareMsg();
              if (prePrepareMsg != nullptr) {
                sendAndIncrementMetric(prePrepareMsg, msgSenderId, metric_sent_preprepare_msg_due_to_status_);
              }
            }
          }
        }
      } else  // if I am also not in curView --- In this case we take messages from viewsManager
      {
        if (msg->hasListOfMissingPrePrepareMsgForViewChange()) {
          for (SeqNum i = msgLastStable + 1; i <= msgLastStable + kWorkWindowSize; i++) {
            if (msg->isMissingPrePrepareMsgForViewChange(i)) {
              PrePrepareMsg *prePrepareMsg =
                  viewsManager->getPrePrepare(i);  // TODO(GG): we can avoid sending misleading message by using the
              // digest of the expected pre prepare message
              if (prePrepareMsg != nullptr) {
                sendAndIncrementMetric(prePrepareMsg, msgSenderId, metric_sent_preprepare_msg_due_to_status_);
              }
            }
          }
        }
      }
    } else {
      sendViewChangeMsgs();
    }
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId is also in view curView
  /////////////////////////////////////////////////////////////////////////

  else if ((msgViewNum == getCurrentView()) && msg->currentViewIsActive()) {
    if (isCurrentPrimary()) {
      if (viewsManager->viewIsActive(getCurrentView())) {
        SeqNum beginRange =
            std::max(lastStableSeqNum + 1,
                     msg->getLastExecutedSeqNum() + 1);  // Notice that after a view change, we don't have to pass the
        // PrePrepare messages from the previous view. TODO(GG): verify
        SeqNum endRange = std::min(lastStableSeqNum + kWorkWindowSize, msgLastStable + kWorkWindowSize);

        for (SeqNum i = beginRange; i <= endRange; i++) {
          if (msg->isPrePrepareInActiveWindow(i)) continue;

          PrePrepareMsg *prePrepareMsg = mainLog->get(i).getSelfPrePrepareMsg();
          if (prePrepareMsg != nullptr) {
            sendAndIncrementMetric(prePrepareMsg, msgSenderId, metric_sent_preprepare_msg_due_to_status_);
          }
        }
        // help from Inactive Window
        if (mainLog->isPressentInHistory(msg->getLastExecutedSeqNum() + 1)) {
          SeqNum endRange = std::min(lastStableSeqNum, msgLastStable + kWorkWindowSize);
          for (SeqNum i = msg->getLastExecutedSeqNum() + 1; i <= endRange; i++) {
            PrePrepareMsg *prePrepareMsg = mainLog->getFromHistory(i).getSelfPrePrepareMsg();
            if (prePrepareMsg != nullptr && !msg->isPrePrepareInActiveWindow(i)) {
              sendAndIncrementMetric(prePrepareMsg, msgSenderId, metric_sent_preprepare_msg_due_to_status_);
            }
          }
        }
      } else {
        tryToSendStatusReport();
      }
    } else {
      // TODO(GG): TBD
    }
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId is in a newer view curView
  /////////////////////////////////////////////////////////////////////////
  else {
    ConcordAssertGT(msgViewNum, getCurrentView());
    tryToSendStatusReport();
  }

  /////////////////////////////////////////////////////////////////////////
  // We are in the same View as msgSenderId, send complaints he is missing
  /////////////////////////////////////////////////////////////////////////

  if (msg->getViewNumber() == getCurrentView()) {
    for (const auto &it : viewsManager->getAllMsgsFromComplainedReplicas()) {
      if (!msg->hasComplaintFromReplica(it->idOfGeneratedReplica())) {
        sendAndIncrementMetric(it.get(), msgSenderId, metric_sent_replica_asks_to_leave_view_msg_due_to_status_);
      }
    }
  }

  delete msg;
}

void ReplicaImp::tryToSendStatusReport(bool onTimer) {
  // TODO(GG): in some cases, we can limit the amount of such messages by using information about
  // connection/disconnection (from the communication module)
  // TODO(GG): explain that the current "Status Report" approch is relatively simple (it can be more efficient and
  // sophisticated).

  const Time currentTime = getMonotonicTime();

  const milliseconds milliSinceLastTime =
      duration_cast<milliseconds>(currentTime - lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas);

  if (milliSinceLastTime < milliseconds(minTimeBetweenStatusRequestsMilli)) return;

  const int64_t dynamicMinTimeBetweenStatusRequestsMilli =
      (int64_t)((double)dynamicUpperLimitOfRounds->upperLimit() * factorForMinTimeBetweenStatusRequestsMilli);

  if (milliSinceLastTime < milliseconds(dynamicMinTimeBetweenStatusRequestsMilli)) return;

  // TODO(GG): handle restart/pause !! (restart/pause may affect time measurements...)
  lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas = currentTime;

  const bool viewIsActive = currentViewIsActive();
  const bool hasNewChangeMsg = viewsManager->hasNewViewMessage(getCurrentView());
  const bool listOfPPInActiveWindow = viewIsActive;
  const bool listOfMissingVCMsg = !viewIsActive && !viewsManager->viewIsPending(getCurrentView());
  const bool listOfMissingPPMsg = !viewIsActive && viewsManager->viewIsPending(getCurrentView());

  ReplicaStatusMsg msg(config_.getreplicaId(),
                       getCurrentView(),
                       lastStableSeqNum,
                       lastExecutedSeqNum,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPPInActiveWindow,
                       listOfMissingVCMsg,
                       listOfMissingPPMsg);

  viewsManager->addComplaintsToStatusMessage(msg);

  if (listOfPPInActiveWindow) {
    const SeqNum start = lastStableSeqNum + 1;
    const SeqNum end = lastStableSeqNum + kWorkWindowSize;

    for (SeqNum i = start; i <= end; i++) {
      if (mainLog->get(i).hasPrePrepareMsg()) msg.setPrePrepareInActiveWindow(i);
    }
  }

  // Fill missing pre-prepare and view change messages.
  viewsManager->fillPropertiesOfStatusMessage(msg, repsInfo, lastStableSeqNum);

  sendToAllOtherReplicas(&msg);
  if (!onTimer) metric_sent_status_msgs_not_due_timer_++;
}

template <>
void ReplicaImp::onMessage<ViewChangeMsg>(std::unique_ptr<ViewChangeMsg> message) {
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
  auto *msg = message.release();
  if (!viewChangeProtocolEnabled) {
    delete msg;
    return;
  }
  if (activeExecutions_ > 0) {
    pushDeferredMessage(msg);
    return;
  }
  metric_received_view_changes_++;

  const ReplicaId generatedReplicaId =
      msg->idOfGeneratedReplica();  // Notice that generatedReplicaId may be != msg->senderId()
  ConcordAssertNE(generatedReplicaId, config_.getreplicaId());

  bool msgAdded = viewsManager->add(msg);

  if (!msgAdded) return;

  LOG_INFO(VC_LOG,
           "Received ViewChangeMsg: " << KVLOG(
               generatedReplicaId, msg->newView(), msg->lastStable(), msg->numberOfElements(), msgAdded));

  viewsManager->processComplaintsFromViewChangeMessage(msg, getMessageValidator());

  if (viewsManager->hasQuorumToLeaveView()) {
    if (activeExecutions_ > 0)
      shouldGoToNextView_ = true;
    else
      goToNextView();
  } else if (viewsManager->tryToJumpToHigherViewAndMoveComplaintsOnQuorum(msg)) {
    MoveToHigherView(msg->newView());
  }

  // The container has to be empty for the next View change message.
  viewsManager->clearComplaintsForHigherView();

  // if the current primary wants to leave view
  if (generatedReplicaId == currentPrimary() && msg->newView() > getCurrentView()) {
    LOG_INFO(VC_LOG, "Primary asks to leave view: " << KVLOG(generatedReplicaId, getCurrentView()));
    MoveToHigherView(getCurrentView() + 1);
  }

  ViewNum maxKnownCorrectView = 0;
  ViewNum maxKnownAgreedView = 0;
  viewsManager->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
  LOG_INFO(VC_LOG, "View Number details: " << KVLOG(maxKnownCorrectView, maxKnownAgreedView));

  if (maxKnownCorrectView > getCurrentView()) {
    // we have at least f+1 view-changes with view number >= maxKnownCorrectView
    MoveToHigherView(maxKnownCorrectView);

    // update maxKnownCorrectView and maxKnownAgreedView
    // TODO(GG): consider to optimize (this part is not always needed)
    viewsManager->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
    LOG_INFO(
        VC_LOG,
        "Computed new view numbers: " << KVLOG(
            maxKnownCorrectView, maxKnownAgreedView, viewsManager->viewIsActive(getCurrentView()), lastAgreedView));
  }

  if (viewsManager->viewIsActive(getCurrentView())) return;  // return, if we are still in the previous view

  if (maxKnownAgreedView != getCurrentView()) return;  // return, if we can't move to the new view yet

  // Replica now has at least 2f+2c+1 ViewChangeMsg messages with view  >= curView

  if (lastAgreedView < getCurrentView()) {
    lastAgreedView = getCurrentView();
    metric_last_agreed_view_.Get().Set(lastAgreedView);
    timeOfLastAgreedView = getMonotonicTime();
  }

  tryToEnterView();
}

template <>
void ReplicaImp::onMessage<NewViewMsg>(std::unique_ptr<NewViewMsg> message) {
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
  auto *msg = message.release();
  if (!viewChangeProtocolEnabled) {
    delete msg;
    return;
  }
  if (activeExecutions_ > 0) {
    pushDeferredMessage(msg);
    return;
  }
  metric_received_new_views_++;

  const ReplicaId senderId = msg->senderId();

  ConcordAssertNE(senderId, config_.getreplicaId());  // should be verified in ViewChangeMsg

  bool msgAdded = viewsManager->add(msg);

  if (!msgAdded) return;

  LOG_INFO(VC_LOG,
           "Received NewViewMsg: " << KVLOG(
               senderId, msg->newView(), msgAdded, getCurrentView(), viewsManager->viewIsActive(getCurrentView())));

  if (viewsManager->viewIsActive(getCurrentView())) return;  // return, if we are still in the previous view

  tryToEnterView();
}

void ReplicaImp::MoveToHigherView(ViewNum nextView) {
  ConcordAssert(viewChangeProtocolEnabled);
  ConcordAssertLT(getCurrentView(), nextView);

  // Once we move to higher view we would prefer tp avoid retransmitting clients request from previous view
  for (auto &[_, msg] : requestsOfNonPrimary) {
    (void)_;
    delete std::get<1>(msg);
  }
  requestsOfNonPrimary.clear();

  const bool wasInPrevViewNumber = viewsManager->viewIsActive(getCurrentView());

  LOG_INFO(VC_LOG, "Moving to higher view: " << KVLOG(getCurrentView(), nextView, wasInPrevViewNumber));

  ViewChangeMsg *pVC = nullptr;

  if (!wasInPrevViewNumber) {
    time_in_active_view_.end();
    pVC = viewsManager->prepareViewChangeMsgAndSetHigherView(nextView, wasInPrevViewNumber);
  } else {
    std::vector<ViewsManager::PrevViewInfo> prevViewInfo;
    for (SeqNum i = lastStableSeqNum + 1; i <= lastStableSeqNum + kWorkWindowSize; i++) {
      SeqNumInfo &seqNumInfo = mainLog->get(i);

      if (seqNumInfo.getPrePrepareMsg() != nullptr && seqNumInfo.isTimeCorrect()) {
        ViewsManager::PrevViewInfo x;

        seqNumInfo.getAndReset(x.prePrepare, x.prepareFull);
        x.hasAllRequests = true;

        ConcordAssertNE(x.prePrepare, nullptr);
        ConcordAssertEQ(x.prePrepare->viewNumber(), getCurrentView());
        // (x.prepareFull!=nullptr) ==> (x.hasAllRequests==true)
        ConcordAssertOR(x.prepareFull == nullptr, x.hasAllRequests);
        // (x.prepareFull!=nullptr) ==> (x.prepareFull->viewNumber() == curView)
        ConcordAssertOR(x.prepareFull == nullptr, x.prepareFull->viewNumber() == getCurrentView());

        prevViewInfo.push_back(x);
      } else {
        seqNumInfo.resetAndFree();
      }
    }

    auto complaints = viewsManager->getAllMsgsFromComplainedReplicas(true);

    if (ps_) {
      ViewChangeMsg *myVC = (getCurrentView() == 0 ? nullptr : viewsManager->getMyLatestViewChangeMsg());
      SeqNum stableLowerBoundWhenEnteredToView = viewsManager->stableLowerBoundWhenEnteredToView();
      const DescriptorOfLastExitFromView desc{getCurrentView(),
                                              lastStableSeqNum,
                                              lastExecutedSeqNum,
                                              prevViewInfo,
                                              myVC,
                                              stableLowerBoundWhenEnteredToView,
                                              std::move(complaints)};
      ps_->beginWriteTran();
      ps_->setDescriptorOfLastExitFromView(desc);
      ps_->clearSeqNumWindow();
      ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
    }

    pVC = viewsManager->prepareViewChangeMsgAndSetHigherView(
        nextView, wasInPrevViewNumber, lastStableSeqNum, lastExecutedSeqNum, &prevViewInfo);
    ConcordAssertNE(pVC, nullptr);
  }

  metric_view_.Get().Set(nextView);
  metric_current_primary_.Get().Set(getCurrentView() % config_.getnumReplicas());

  auto newView = getCurrentView();
  auto newPrimary = currentPrimary();
  LOG_INFO(VC_LOG,
           "Sending view change message: " << KVLOG(
               newView, wasInPrevViewNumber, newPrimary, lastExecutedSeqNum, lastStableSeqNum));

  sendToAllOtherReplicas(pVC);
}

void ReplicaImp::goToNextView() {
  // at this point we don't have f+1 ViewChangeMsg messages with view >= curView

  MoveToHigherView(getCurrentView() + 1);

  // at this point we don't have enough ViewChangeMsg messages (2f+2c+1) to enter the new view (because 2f+2c+1 > f+1)
}

bool ReplicaImp::tryToEnterView() {
  ConcordAssert(!currentViewIsActive());
  ConcordAssertEQ(activeExecutions_, 0);
  std::vector<PrePrepareMsg *> prePreparesForNewView;

  bool enteredView =
      viewsManager->tryToEnterView(getCurrentView(), lastStableSeqNum, lastExecutedSeqNum, &prePreparesForNewView);

  LOG_INFO(VC_LOG,
           "Called viewsManager->tryToEnterView: " << KVLOG(
               getCurrentView(), lastStableSeqNum, lastExecutedSeqNum, enteredView));
  if (enteredView)
    onNewView(prePreparesForNewView);
  else
    tryToSendStatusReport();

  return enteredView;
}

void ReplicaImp::onNewView(const std::vector<PrePrepareMsg *> &prePreparesForNewView) {
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
  SeqNum firstPPSeq = 0;
  SeqNum lastPPSeq = 0;

  time_in_active_view_.start();
  consensus_times_.clear();

  if (!prePreparesForNewView.empty()) {
    firstPPSeq = prePreparesForNewView.front()->seqNumber();
    lastPPSeq = prePreparesForNewView.back()->seqNumber();
  }

  LOG_INFO(VC_LOG,
           "New View: " << KVLOG(getCurrentView(),
                                 prePreparesForNewView.size(),
                                 firstPPSeq,
                                 lastPPSeq,
                                 lastStableSeqNum,
                                 lastExecutedSeqNum,
                                 viewsManager->stableLowerBoundWhenEnteredToView()));

  ConcordAssert(viewsManager->viewIsActive(getCurrentView()));
  ConcordAssertGE(lastStableSeqNum, viewsManager->stableLowerBoundWhenEnteredToView());
  ConcordAssertGE(lastExecutedSeqNum,
                  lastStableSeqNum);  // we moved to the new state, only after synchronizing the state

  timeOfLastViewEntrance = getMonotonicTime();  // TODO(GG): handle restart/pause

  NewViewMsg *newNewViewMsgToSend = nullptr;

  if (repsInfo->primaryOfView(getCurrentView()) == config_.getreplicaId()) {
    NewViewMsg *nv = viewsManager->getMyNewViewMsgForCurrentView();

    nv->finalizeMessage(*repsInfo);

    ConcordAssertEQ(nv->newView(), getCurrentView());

    newNewViewMsgToSend = nv;
  }

  if (prePreparesForNewView.empty()) {
    primaryLastUsedSeqNum = lastStableSeqNum;
    metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
    strictLowerBoundOfSeqNums = lastStableSeqNum;
    maxSeqNumTransferredFromPrevViews = lastStableSeqNum;
    LOG_INFO(VC_LOG,
             "No PrePrepare-s for the new view: " << KVLOG(
                 primaryLastUsedSeqNum, strictLowerBoundOfSeqNums, maxSeqNumTransferredFromPrevViews));
  } else {
    primaryLastUsedSeqNum = lastPPSeq;
    metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
    strictLowerBoundOfSeqNums = firstPPSeq - 1;
    maxSeqNumTransferredFromPrevViews = lastPPSeq;
    LOG_INFO(VC_LOG,
             "There are PrePrepare-s for the new view: " << KVLOG(firstPPSeq,
                                                                  lastPPSeq,
                                                                  primaryLastUsedSeqNum,
                                                                  strictLowerBoundOfSeqNums,
                                                                  maxSeqNumTransferredFromPrevViews));
  }

  if (ps_) {
    vector<ViewChangeMsg *> viewChangeMsgsForCurrentView = viewsManager->getViewChangeMsgsForCurrentView();
    NewViewMsg *newViewMsgForCurrentView = viewsManager->getNewViewMsgForCurrentView();

    bool myVCWasUsed = false;
    for (size_t i = 0; i < viewChangeMsgsForCurrentView.size() && !myVCWasUsed; i++) {
      ConcordAssertNE(viewChangeMsgsForCurrentView[i], nullptr);
      if (viewChangeMsgsForCurrentView[i]->idOfGeneratedReplica() == config_.getreplicaId()) myVCWasUsed = true;
    }

    ViewChangeMsg *myVC = nullptr;
    if (!myVCWasUsed) {
      myVC = viewsManager->getMyLatestViewChangeMsg();
    } else {
      // debug/test: check that my VC should be included
      ViewChangeMsg *tempMyVC = viewsManager->getMyLatestViewChangeMsg();
      ConcordAssertNE(tempMyVC, nullptr);
      Digest d;
      tempMyVC->getMsgDigest(d);
      ConcordAssert(newViewMsgForCurrentView->includesViewChangeFromReplica(config_.getreplicaId(), d));
    }

    DescriptorOfLastNewView viewDesc{getCurrentView(),
                                     newViewMsgForCurrentView,
                                     viewChangeMsgsForCurrentView,
                                     myVC,
                                     viewsManager->stableLowerBoundWhenEnteredToView(),
                                     maxSeqNumTransferredFromPrevViews};

    ps_->beginWriteTran();
    ps_->setDescriptorOfLastNewView(viewDesc);
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
    ps_->setStrictLowerBoundOfSeqNums(strictLowerBoundOfSeqNums);
  }

  const bool primaryIsMe = (config_.getreplicaId() == repsInfo->primaryOfView(getCurrentView()));

  for (size_t i = 0; i < prePreparesForNewView.size(); i++) {
    PrePrepareMsg *pp = prePreparesForNewView[i];
    ConcordAssertGE(pp->seqNumber(), firstPPSeq);
    ConcordAssertLE(pp->seqNumber(), lastPPSeq);
    ConcordAssertEQ(pp->firstPath(), CommitPath::SLOW);  // TODO(GG): don't we want to use the fast path?
    SeqNumInfo &seqNumInfo = mainLog->get(pp->seqNumber());

    if (ps_) {
      ps_->setPrePrepareMsgInSeqNumWindow(pp->seqNumber(), pp);
      ps_->setSlowStartedInSeqNumWindow(pp->seqNumber(), true);
    }

    if (primaryIsMe)
      seqNumInfo.addSelfMsg(pp);
    else
      seqNumInfo.addMsg(pp);

    seqNumInfo.startSlowPath();
  }

  if (ps_) ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());

  clientsManager->clearAllPendingRequests();

  // Once we move to higher view we would prefer tp avoid retransmitting clients request from previous view
  for (auto &[_, msg] : requestsOfNonPrimary) {
    (void)_;
    delete std::get<1>(msg);
  }
  requestsOfNonPrimary.clear();

  // clear requestsQueueOfPrimary
  while (!requestsQueueOfPrimary.empty()) {
    auto msg = requestsQueueOfPrimary.front();
    primaryCombinedReqSize -= msg->size();
    requestsQueueOfPrimary.pop();
    delete msg;
  }

  primary_queue_size_.Get().Set(requestsQueueOfPrimary.size());

  // send messages
  if (newNewViewMsgToSend != nullptr) {
    LOG_INFO(VC_LOG, "Sending NewView message to all replicas. " << KVLOG(getCurrentView()));
    sendToAllOtherReplicas(newNewViewMsgToSend);
  }

  for (size_t i = 0; i < prePreparesForNewView.size(); i++) {
    PrePrepareMsg *pp = prePreparesForNewView[i];
    SeqNumInfo &seqNumInfo = mainLog->get(pp->seqNumber());
    sendPreparePartial(seqNumInfo);
  }

  LOG_INFO(VC_LOG, "Start working in new view: " << KVLOG(getCurrentView()));

  controller->onNewView(getCurrentView(), primaryLastUsedSeqNum);
  metric_current_active_view_.Get().Set(getCurrentView());
  metric_sent_replica_asks_to_leave_view_msg_.Get().Set(0);

  onViewNumCallbacks_.invokeAll(isCurrentPrimary());
}

void ReplicaImp::sendCheckpointIfNeeded() {
  if (isCollectingState() || !currentViewIsActive()) return;

  const SeqNum lastCheckpointNumber = (lastExecutedSeqNum / checkpointWindowSize) * checkpointWindowSize;

  if (lastCheckpointNumber == 0) return;

  ConcordAssert(checkpointsLog->insideActiveWindow(lastCheckpointNumber));

  auto &checkInfo = checkpointsLog->get(lastCheckpointNumber);
  CheckpointMsg *checkpointMessage = checkInfo.selfCheckpointMsg();

  if (!checkpointMessage) {
    LOG_INFO(GL, "My Checkpoint message is missing. " << KVLOG(lastCheckpointNumber, lastExecutedSeqNum));
    return;
  }

  if (checkInfo.checkpointSentAllOrApproved()) return;

  // TODO(GG): 3 seconds, should be in configuration
  if ((getMonotonicTime() - checkInfo.selfExecutionTime()) >= 3s) {
    checkInfo.setCheckpointSentAllOrApproved();
    sendToAllOtherReplicas(checkpointMessage, true);
    return;
  }
  if (activeExecutions_ > 0)
    isSendCheckpointIfNeeded_ =
        true;  // if we defer the part of marking stable for fast path after that when we handle the deferred part we
               // want to run the all sendCheckpointIfNeeded method to verify if its even relevant now
  else {
    tryToMarkCheckpointStableForFastPath(lastCheckpointNumber, checkInfo, checkpointMessage);
  }
}

void ReplicaImp::tryToMarkCheckpointStableForFastPath(const SeqNum &lastCheckpointNumber,
                                                      CheckpointInfo<> &checkInfo,
                                                      CheckpointMsg *checkpointMessage) {
  ConcordAssertEQ(activeExecutions_, 0);
  const SeqNum refSeqNumberForCheckpoint = lastCheckpointNumber + MaxConcurrentFastPaths;
  if (lastExecutedSeqNum < refSeqNumberForCheckpoint) return;

  if (mainLog->insideActiveWindow(lastExecutedSeqNum))  // TODO(GG): condition is needed ?
  {
    SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum);

    if (seqNumInfo.hasFastPathFullCommitProof()) {
      checkInfo.tryToMarkCheckpointCertificateCompleted();

      ConcordAssert(checkInfo.isCheckpointCertificateComplete());

      onSeqNumIsStable(lastCheckpointNumber);

      checkInfo.setCheckpointSentAllOrApproved();

      return;
    }
  }

  checkInfo.setCheckpointSentAllOrApproved();
  sendToAllOtherReplicas(checkpointMessage, true);
}

void ReplicaImp::onTransferringCompleteImp(uint64_t newStateCheckpoint) {
  ConcordAssertEQ(activeExecutions_, 0);
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
  TimeRecorder scoped_timer(*histograms_.onTransferringCompleteImp);
  time_in_state_transfer_.end();
  LOG_INFO(GL, KVLOG(newStateCheckpoint));
  for (auto &req : requestsOfNonPrimary) {
    delete std::get<1>(req.second);
  }
  requestsOfNonPrimary.clear();
  if (ps_) {
    ps_->beginWriteTran();
  }
  SeqNum newCheckpointSeqNum = newStateCheckpoint * checkpointWindowSize;
  if (newCheckpointSeqNum <= lastExecutedSeqNum) {
    LOG_DEBUG(GL,
              "Executing onTransferringCompleteImp(newStateCheckpoint) where newStateCheckpoint <= lastExecutedSeqNum:"
                  << KVLOG(newStateCheckpoint, lastExecutedSeqNum));
    if (ps_) {
      ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
    }
    return;
  }
  lastExecutedSeqNum = newCheckpointSeqNum;
  if (ps_) {
    ps_->setLastExecutedSeqNum(lastExecutedSeqNum);
  }
  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum);
  }

  timeOfLastStateSynch = getMonotonicTime();  // TODO(GG): handle restart/pause

  clientsManager->loadInfoFromReservedPages();

  KeyExchangeManager::instance().loadPublicKeys();
  KeyExchangeManager::instance().loadClientPublicKeys();

  if (config_.timeServiceEnabled) {
    time_service_manager_->load();
  }

  if (newCheckpointSeqNum > lastStableSeqNum + kWorkWindowSize) {
    const SeqNum refPoint = newCheckpointSeqNum - kWorkWindowSize;
    const bool withRefCheckpoint = (checkpointsLog->insideActiveWindow(refPoint) &&
                                    (checkpointsLog->get(refPoint).selfCheckpointMsg() != nullptr));

    onSeqNumIsStable(refPoint, withRefCheckpoint, true);
  }

  ConcordAssert(checkpointsLog->insideActiveWindow(newCheckpointSeqNum));

  // create and send my checkpoint
  Digest stateDigest, reservedPagesDigest, rvbDataDigest;
  CheckpointMsg::State state;
  stateTransfer->getDigestOfCheckpoint(newStateCheckpoint,
                                       sizeof(Digest),
                                       state,
                                       stateDigest.content(),
                                       reservedPagesDigest.content(),
                                       rvbDataDigest.content());
  CheckpointMsg *checkpointMsg = new CheckpointMsg(
      config_.getreplicaId(), newCheckpointSeqNum, state, stateDigest, reservedPagesDigest, rvbDataDigest, false);
  checkpointMsg->sign();
  auto &checkpointInfo = checkpointsLog->get(newCheckpointSeqNum);
  checkpointInfo.addCheckpointMsg(checkpointMsg, config_.getreplicaId());
  checkpointInfo.setCheckpointSentAllOrApproved();

  if (newCheckpointSeqNum > primaryLastUsedSeqNum) primaryLastUsedSeqNum = newCheckpointSeqNum;

  if (checkpointInfo.isCheckpointSuperStable()) {
    onSeqNumIsSuperStable(newCheckpointSeqNum);
  }
  if (ps_) {
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
    ps_->setCheckpointMsgInCheckWindow(newCheckpointSeqNum, checkpointMsg);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }
  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);

  sendToAllOtherReplicas(checkpointMsg);

  if (!currentViewIsActive()) {
    LOG_INFO(GL, "tryToEnterView after State Transfer finished ...");
    tryToEnterView();
  }
}

void ReplicaImp::onSeqNumIsSuperStable(SeqNum superStableSeqNum) {
  ConcordAssertEQ(activeExecutions_,
                  0);  // We shouldn't have active executions when checking if sequence number is super stable
  if (lastSuperStableSeqNum >= superStableSeqNum) return;
  lastSuperStableSeqNum = superStableSeqNum;
  auto seq_num_to_stop_at = ControlStateManager::instance().getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value() && seq_num_to_stop_at.value() == superStableSeqNum) {
    LOG_INFO(GL, "Informing control state manager that consensus should be stopped: " << KVLOG(superStableSeqNum));

    metric_on_call_back_of_super_stable_cp_.Get().Set(1);
    IControlHandler::instance()->onSuperStableCheckpoint();
    ControlStateManager::instance().wedge();
  }
}

void ReplicaImp::onSeqNumIsStable(SeqNum newStableSeqNum, bool hasStateInformation, bool oldSeqNum) {
  ConcordAssertEQ(activeExecutions_,
                  0);  // We shouldn't have active executions when checking if sequence number is stable
  ConcordAssertOR(hasStateInformation, oldSeqNum);  // !hasStateInformation ==> oldSeqNum
  ConcordAssertEQ(newStableSeqNum % checkpointWindowSize, 0);

  if (newStableSeqNum <= lastStableSeqNum) return;
  checkpoint_times_.end(newStableSeqNum);
  TimeRecorder scoped_timer(*histograms_.onSeqNumIsStable);

  LOG_INFO(GL,
           "New stable sequence number" << KVLOG(lastStableSeqNum, newStableSeqNum, hasStateInformation, oldSeqNum));

  if (ps_) ps_->beginWriteTran();

  lastStableSeqNum = newStableSeqNum;
  metric_last_stable_seq_num_.Get().Set(lastStableSeqNum);

  if (ps_) ps_->setLastStableSeqNum(lastStableSeqNum);

  if (lastStableSeqNum > strictLowerBoundOfSeqNums) {
    strictLowerBoundOfSeqNums = lastStableSeqNum;
    if (ps_) ps_->setStrictLowerBoundOfSeqNums(strictLowerBoundOfSeqNums);
  }

  if (lastStableSeqNum > primaryLastUsedSeqNum) {
    primaryLastUsedSeqNum = lastStableSeqNum;
    metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
    if (ps_) ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
  }
  {
    TimeRecorder scoped_timer1(*histograms_.advanceActiveWindowMainLog);
    mainLog->advanceActiveWindow(lastStableSeqNum + 1);
  }
  // Basically, once a checkpoint become stable, we advance the checkpoints log window to it.
  // Alas, by doing so, we do not leave time for a checkpoint to try and become super stable.
  // For that we added another cell to the checkpoints log such that the "oldest" cell contains the checkpoint is
  // candidate for becoming super stable. So, once a checkpoint becomes stable we check if the previous checkpoint is
  // in the log, and if so we advance the log to that previous checkpoint.
  if (checkpointsLog->insideActiveWindow(newStableSeqNum - checkpointWindowSize)) {
    checkpointsLog->advanceActiveWindow(newStableSeqNum - checkpointWindowSize);
  } else {
    // If for some reason the previous checkpoint is not in the log, we advance the log to the current stable
    // checkpoint.
    checkpointsLog->advanceActiveWindow(lastStableSeqNum);
  }

  if (hasStateInformation) {
    if (lastStableSeqNum > lastExecutedSeqNum) {
      lastExecutedSeqNum = lastStableSeqNum;
      metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
      if (config_.getdebugStatisticsEnabled()) {
        DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum);
      }
      clientsManager->loadInfoFromReservedPages();
    }
    if (ps_) ps_->setLastExecutedSeqNum(lastExecutedSeqNum);
    auto &checkpointInfo = checkpointsLog->get(lastStableSeqNum);
    CheckpointMsg *checkpointMsg = checkpointInfo.selfCheckpointMsg();

    if (checkpointMsg == nullptr) {
      Digest stateDigest, reservedPagesDigest, rvbDataDigest;
      CheckpointMsg::State state;
      const uint64_t checkpointNum = lastStableSeqNum / checkpointWindowSize;
      stateTransfer->getDigestOfCheckpoint(checkpointNum,
                                           sizeof(Digest),
                                           state,
                                           stateDigest.content(),
                                           reservedPagesDigest.content(),
                                           rvbDataDigest.content());
      checkpointMsg = new CheckpointMsg(
          config_.getreplicaId(), lastStableSeqNum, state, stateDigest, reservedPagesDigest, rvbDataDigest, true);
      checkpointMsg->sign();
      checkpointInfo.addCheckpointMsg(checkpointMsg, config_.getreplicaId());
    } else {
      if (!checkpointMsg->isStableState()) {
        checkpointMsg->setStateAsStable();
        checkpointMsg->sign();
      }
    }

    if (!checkpointInfo.isCheckpointCertificateComplete()) checkpointInfo.tryToMarkCheckpointCertificateCompleted();
    ConcordAssert(checkpointInfo.isCheckpointCertificateComplete());

    // Call onSeqNumIsSuperStable in case that the self message was the last one to add
    if (checkpointInfo.isCheckpointSuperStable()) {
      onSeqNumIsSuperStable(lastStableSeqNum);
    }
    if (ps_) {
      ps_->setCheckpointMsgInCheckWindow(lastStableSeqNum, checkpointMsg);
      ps_->setCompletedMarkInCheckWindow(lastStableSeqNum, true);
    }
  }

  if (ps_) {
    auto &CheckpointInfo = checkpointsLog->get(lastStableSeqNum);
    std::vector<CheckpointMsg *> msgs;
    msgs.reserve(CheckpointInfo.getAllCheckpointMsgs().size());
    for (const auto &m : CheckpointInfo.getAllCheckpointMsgs()) {
      msgs.push_back(m.second);
    }
    DescriptorOfLastStableCheckpoint desc(config_.getnumReplicas(), msgs);
    ps_->setDescriptorOfLastStableCheckpoint(desc);
  }

  if (ps_) ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());

  if (((BatchingPolicy)config_.batchingPolicy == BATCH_SELF_ADJUSTED) && !oldSeqNum && currentViewIsActive() &&
      (currentPrimary() == config_.getreplicaId()) && !isCollectingState()) {
    tryToSendPrePrepareMsg(false);
  }

  if (!currentViewIsActive()) {
    LOG_INFO(GL, "tryToEnterView after Stable Sequence Number is Received ...");
    tryToEnterView();
  }

  auto seq_num_to_stop_at = ControlStateManager::instance().getCheckpointToStopAt();

  // Once a replica is has got the the wedge point, it mark itself as wedged. Then, once it restarts, it cleans
  // the wedge point.
  if (seq_num_to_stop_at.has_value() && seq_num_to_stop_at.value() == newStableSeqNum) {
    LOG_INFO(GL,
             "Informing control state manager that consensus should be stopped (with n-f/n replicas): " << KVLOG(
                 newStableSeqNum, metric_last_stable_seq_num_.Get().Get()));
    IControlHandler::instance()->onStableCheckpoint();
    ControlStateManager::instance().wedge();
  }
  onSeqNumIsStableCallbacks_.invokeAll(newStableSeqNum);
}
void ReplicaImp::sendRepilcaRestartReady(uint8_t reason, const std::string &extraData) {
  auto seq_num_to_stop_at = ControlStateManager::instance().getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value()) {
    unique_ptr<ReplicaRestartReadyMsg> readytToRestartMsg(
        ReplicaRestartReadyMsg::create(config_.getreplicaId(),
                                       seq_num_to_stop_at.value(),
                                       static_cast<ReplicaRestartReadyMsg::Reason>(reason),
                                       extraData));
    sendToAllOtherReplicas(readytToRestartMsg.get());
    auto &restart_ready_msgs = restart_ready_msgs_[reason];
    restart_ready_msgs[config_.getreplicaId()] = std::move(readytToRestartMsg);  // add self message to the list
    bool restart_bft_flag = bftEngine::ControlStateManager::instance().getRestartBftFlag();
    uint32_t targetNumOfMsgs =
        (restart_bft_flag ? (config_.getnumReplicas() - config_.getfVal()) : config_.getnumReplicas());
    if (restart_ready_msgs.size() == targetNumOfMsgs) {
      LOG_INFO(GL, "Target number = " << targetNumOfMsgs << " of restart ready msgs are recieved. Send restart proof");
      sendReplicasRestartReadyProof(reason);
    }
  }
}
void ReplicaImp::tryToSendReqMissingDataMsg(SeqNum seqNumber, bool slowPathOnly, uint16_t destReplicaId) {
  if ((!currentViewIsActive()) || (seqNumber <= strictLowerBoundOfSeqNums) ||
      (!mainLog->insideActiveWindow(seqNumber)) || (!mainLog->insideActiveWindow(seqNumber))) {
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  const Time curTime = getMonotonicTime();

  {
    Time t = seqNumInfo.getTimeOfFirstRelevantInfoFromPrimary();
    const Time lastInfoRequest = seqNumInfo.getTimeOfLastInfoRequest();

    if ((t < lastInfoRequest)) t = lastInfoRequest;

    if (t != MinTime && (t < curTime)) {
      auto diffMilli = duration_cast<milliseconds>(curTime - t);
      if (diffMilli.count() < dynamicUpperLimitOfRounds->upperLimit() / 4)  // TODO(GG): config
        return;
    }
  }

  seqNumInfo.setTimeOfLastInfoRequest(curTime);

  LOG_INFO(GL, "Try to request missing data. " << KVLOG(seqNumber, getCurrentView()));

  ReqMissingDataMsg reqData(config_.getreplicaId(), getCurrentView(), seqNumber);

  const bool routerForPartialProofs = repsInfo->isCollectorForPartialProofs(getCurrentView(), seqNumber);

  const bool routerForPartialPrepare = (currentPrimary() == config_.getreplicaId());

  const bool routerForPartialCommit = (currentPrimary() == config_.getreplicaId());

  const bool missingPrePrepare = (seqNumInfo.getPrePrepareMsg() == nullptr);
  const bool missingBigRequests = (!missingPrePrepare) && (!seqNumInfo.hasPrePrepareMsg());

  ReplicaId firstRepId = 0;
  ReplicaId lastRepId = config_.getnumReplicas() - 1;
  if (destReplicaId != ALL_OTHER_REPLICAS) {
    firstRepId = destReplicaId;
    lastRepId = destReplicaId;
  }

  for (ReplicaId destRep = firstRepId; destRep <= lastRepId; destRep++) {
    if (destRep == config_.getreplicaId()) continue;  // don't send to myself

    const bool destIsPrimary = (currentPrimary() == destRep);

    const bool missingPartialPrepare =
        (routerForPartialPrepare && (!seqNumInfo.preparedOrHasPreparePartialFromReplica(destRep)));

    const bool missingFullPrepare = !seqNumInfo.preparedOrHasPreparePartialFromReplica(destRep);

    const bool missingPartialCommit =
        (routerForPartialCommit && (!seqNumInfo.committedOrHasCommitPartialFromReplica(destRep)));

    const bool missingFullCommit = !seqNumInfo.committedOrHasCommitPartialFromReplica(destRep);

    const bool missingPartialProof = !slowPathOnly && routerForPartialProofs &&
                                     !seqNumInfo.hasFastPathFullCommitProof() &&
                                     !seqNumInfo.hasFastPathPartialCommitProofFromReplica(destRep);

    const bool missingFullProof = !slowPathOnly && !seqNumInfo.hasFastPathFullCommitProof();

    bool sendNeeded = missingPartialProof || missingPartialPrepare || missingFullPrepare || missingPartialCommit ||
                      missingFullCommit || missingFullProof;

    if (destIsPrimary && !sendNeeded) sendNeeded = missingBigRequests || missingPrePrepare;

    if (!sendNeeded) continue;

    reqData.resetFlags();

    if (destIsPrimary && missingPrePrepare) reqData.setPrePrepareIsMissing();

    if (missingPartialProof) reqData.setPartialProofIsMissing();
    if (missingPartialPrepare) reqData.setPartialPrepareIsMissing();
    if (missingFullPrepare) reqData.setFullPrepareIsMissing();
    if (missingPartialCommit) reqData.setPartialCommitIsMissing();
    if (missingFullCommit) reqData.setFullCommitIsMissing();
    if (missingFullProof) reqData.setFullCommitProofIsMissing();

    const bool slowPathStarted = seqNumInfo.slowPathStarted();

    if (slowPathStarted) reqData.setSlowPathHasStarted();

    auto destinationReplica = destRep;
    LOG_INFO(GL,
             "Send ReqMissingDataMsg. " << KVLOG(
                 destinationReplica, seqNumber, reqData.getFlags(), reqData.getFlagsAsBits()));

    send(&reqData, destRep);
    metric_sent_req_for_missing_data_++;
  }
}

template <>
void ReplicaImp::onMessage<ReqMissingDataMsg>(std::unique_ptr<ReqMissingDataMsg> message) {
  metric_received_req_missing_datas_++;
  auto *msg = message.release();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_INFO(CNSUS, "Received ReqMissingDataMsg. " << KVLOG(msgSender, msg->getFlags(), msg->getFlagsAsBits()));
  if ((currentViewIsActive()) && (mainLog->insideActiveWindow(msgSeqNum) || mainLog->isPressentInHistory(msgSeqNum))) {
    SeqNumInfo &seqNumInfo = mainLog->getFromActiveWindowOrHistory(msgSeqNum);

    PrePrepareMsg *pp = seqNumInfo.getSelfPrePrepareMsg();
    if (msg->getPrePrepareIsMissing()) {
      if (pp != nullptr) {
#ifdef ENABLE_ALL_METRICS
        sendAndIncrementMetric(pp, msgSender, metric_sent_preprepare_msg_due_to_reqMissingData_);
#else
        send(pp, msgSender);
#endif
      }
    }

    if (config_.getreplicaId() == currentPrimary()) {
      if (seqNumInfo.slowPathStarted() && !msg->getSlowPathHasStarted()) {
        StartSlowCommitMsg startSlowMsg(config_.getreplicaId(), getCurrentView(), msgSeqNum);
#ifdef ENABLE_ALL_METRICS
        sendAndIncrementMetric(&startSlowMsg, msgSender, metric_sent_startSlowPath_msg_due_to_reqMissingData_);
#else
        send(&startSlowMsg, msgSender);
#endif
      }
    }

    if (msg->getFullCommitIsMissing()) {
      CommitFullMsg *c = seqNumInfo.getValidCommitFullMsg();
      if (c != nullptr) {
        LOG_INFO(CNSUS, "Sending FullCommit message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
#ifdef ENABLE_ALL_METRICS
        sendAndIncrementMetric(c, msgSender, metric_sent_commitFull_msg_due_to_reqMissingData_);
#else
        send(c, msgSender);
#endif
      }
    }

    if (msg->getFullCommitProofIsMissing() && seqNumInfo.hasFastPathFullCommitProof()) {
      FullCommitProofMsg *fcp = seqNumInfo.getFastPathFullCommitProofMsg();
      LOG_INFO(CNSUS, "Sending FullCommitProof message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
#ifdef ENABLE_ALL_METRICS
      sendAndIncrementMetric(fcp, msgSender, metric_sent_fullCommitProof_msg_due_to_reqMissingData_);
#else
      send(fcp, msgSender);
#endif
    }

    if (msg->getPartialProofIsMissing() && seqNumInfo.isTimeCorrect()) {
      // TODO(GG): consider not to send if msgSender is not a collector
      PartialCommitProofMsg *pcf = seqNumInfo.getFastPathSelfPartialCommitProofMsg();

      if (pcf != nullptr) {
        LOG_INFO(CNSUS, "Sending PartialProof message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
#ifdef ENABLE_ALL_METRICS
        sendAndIncrementMetric(pcf, msgSender, metric_sent_partialCommitProof_msg_due_to_reqMissingData_);
#else
        send(pcf, msgSender);
#endif
      }
    }

    if (msg->getPartialPrepareIsMissing() && (currentPrimary() == msgSender) && seqNumInfo.isTimeCorrect()) {
      PreparePartialMsg *pr = seqNumInfo.getSelfPreparePartialMsg();

      if (pr != nullptr) {
        LOG_INFO(CNSUS, "Sending PartialPrepare message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
#ifdef ENABLE_ALL_METRICS
        sendAndIncrementMetric(pr, msgSender, metric_sent_preparePartial_msg_due_to_reqMissingData_);
#else
        send(pr, msgSender);
#endif
      }
    }

    if (msg->getFullPrepareIsMissing()) {
      PrepareFullMsg *pf = seqNumInfo.getValidPrepareFullMsg();

      if (pf != nullptr) {
        LOG_INFO(CNSUS, "Sending FullPrepare message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
#ifdef ENABLE_ALL_METRICS
        sendAndIncrementMetric(pf, msgSender, metric_sent_prepareFull_msg_due_to_reqMissingData_);
#else
        send(pf, msgSender);
#endif
      }
    }

    if (msg->getPartialCommitIsMissing() && (currentPrimary() == msgSender) && seqNumInfo.isTimeCorrect()) {
      CommitPartialMsg *c = seqNumInfo.getSelfCommitPartialMsg();
      if (c != nullptr) {
        LOG_INFO(CNSUS, "Sending PartialCommit message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
#ifdef ENABLE_ALL_METRICS
        sendAndIncrementMetric(c, msgSender, metric_sent_commitPartial_msg_due_to_reqMissingData_);
#else
        send(c, msgSender);
#endif
      }
    }
  } else {
    LOG_INFO(CNSUS,
             "Ignore the ReqMissingDataMsg message. " << KVLOG(msgSender,
                                                               currentViewIsActive(),
                                                               mainLog->insideActiveWindow(msgSeqNum),
                                                               mainLog->isPressentInHistory(msgSeqNum)));
  }

  delete msg;
}

void ReplicaImp::askToLeaveView(ReplicaAsksToLeaveViewMsg::Reason reasonToLeave) {
  std::unique_ptr<ReplicaAsksToLeaveViewMsg> askToLeaveView(
      ReplicaAsksToLeaveViewMsg::create(config_.getreplicaId(), getCurrentView(), reasonToLeave));
  sendToAllOtherReplicas(askToLeaveView.get());
  viewsManager->storeComplaint(std::move(askToLeaveView));
  metric_sent_replica_asks_to_leave_view_msg_++;

  if (activeExecutions_ > 0)
    shouldTryToGoToNextView_ = true;
  else
    tryToGoToNextView();
}

void ReplicaImp::onViewsChangeTimer(Timers::Handle timer)  // TODO(GG): review/update logic
{
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
  ConcordAssert(viewChangeProtocolEnabled);

  if (isCollectingState()) return;

  Time currTime = getMonotonicTime();

  //////////////////////////////////////////////////////////////////////////////
  //
  //////////////////////////////////////////////////////////////////////////////

  if (autoPrimaryRotationEnabled && currentViewIsActive()) {
    const uint64_t timeout = (isCurrentPrimary() ? (autoPrimaryRotationTimerMilli)
                                                 : (autoPrimaryRotationTimerMilli + viewChangeTimeoutMilli));

    const uint64_t diffMilli = duration_cast<milliseconds>(currTime - timeOfLastViewEntrance).count();

    if (diffMilli > timeout) {
      LOG_INFO(VC_LOG,
               "Initiate automatic view change in view=" << getCurrentView() << " (" << diffMilli
                                                         << " milli seconds after start working in the previous view)");
      if (activeExecutions_ > 0)
        shouldGoToNextView_ = true;
      else
        goToNextView();
      return;
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //
  //////////////////////////////////////////////////////////////////////////////

  uint64_t viewChangeTimeout = viewChangeTimerMilli;
  if (autoIncViewChangeTimer && ((lastViewThatTransferredSeqNumbersFullyExecuted + 1) < getCurrentView())) {
    uint64_t factor = (getCurrentView() - lastViewThatTransferredSeqNumbersFullyExecuted);
    viewChangeTimeout = viewChangeTimeout * factor;  // TODO(GG): review logic here
  }

  if (currentViewIsActive()) {
    if (isCurrentPrimary() || viewsManager->getComplaintFromReplica(config_.replicaId) != nullptr) return;

    std::string cidOfEarliestPendingRequest = std::string();
    const Time timeOfEarliestPendingRequest = clientsManager->infoOfEarliestPendingRequest(cidOfEarliestPendingRequest);

    const bool hasPendingRequest = (timeOfEarliestPendingRequest != MaxTime);

    if (!hasPendingRequest) return;

    const uint64_t diffMilli1 = duration_cast<milliseconds>(currTime - timeOfLastStateSynch).count();
    const uint64_t diffMilli2 = duration_cast<milliseconds>(currTime - timeOfLastViewEntrance).count();
    const uint64_t diffMilli3 = duration_cast<milliseconds>(currTime - timeOfEarliestPendingRequest).count();

    clientsManager->logAllPendingRequestsExceedingThreshold(viewChangeTimeout / 2, currTime);

    if ((diffMilli1 > viewChangeTimeout) && (diffMilli2 > viewChangeTimeout) && (diffMilli3 > viewChangeTimeout)) {
      LOG_INFO(
          VC_LOG,
          "Ask to leave view=" << getCurrentView() << " (" << diffMilli3
                               << " ms after the earliest pending client request)."
                               << KVLOG(cidOfEarliestPendingRequest, lastViewThatTransferredSeqNumbersFullyExecuted));

      askToLeaveView(ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout);
      return;
    }
  } else  // not currentViewIsActive()
  {
    if (lastAgreedView != getCurrentView()) return;
    if (repsInfo->primaryOfView(lastAgreedView) == config_.getreplicaId()) return;

    currTime = getMonotonicTime();
    const uint64_t timeSinceLastStateTransferMilli =
        duration_cast<milliseconds>(currTime - timeOfLastStateSynch).count();
    const uint64_t timeSinceLastAgreedViewMilli = duration_cast<milliseconds>(currTime - timeOfLastAgreedView).count();

    if ((timeSinceLastStateTransferMilli > viewChangeTimeout) && (timeSinceLastAgreedViewMilli > viewChangeTimeout)) {
      LOG_INFO(VC_LOG,
               "Unable to activate the last agreed view (despite receiving 2f+2c+1 view change msgs). "
               "State transfer hasn't kicked-in for a while either. Asking to leave the current view: "
                   << KVLOG(getCurrentView(),
                            timeSinceLastAgreedViewMilli,
                            timeSinceLastStateTransferMilli,
                            lastViewThatTransferredSeqNumbersFullyExecuted));

      askToLeaveView(ReplicaAsksToLeaveViewMsg::Reason::NewPrimaryGetInChargeTimeout);
      return;
    }
  }
}

void ReplicaImp::onStatusReportTimer(Timers::Handle timer) {
  if (isCollectingState() || bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;

  tryToSendStatusReport(true);

#ifdef DEBUG_MEMORY_MSG
  MessageBase::printLiveMessages();
#endif
}

void ReplicaImp::onSlowPathTimer(Timers::Handle timer) {
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;
  tryToStartSlowPaths();
  auto newPeriod = milliseconds(controller->slowPathsTimerMilli());
  timers_.reset(timer, newPeriod);
  metric_slow_path_timer_.Get().Set(controller->slowPathsTimerMilli());
}

void ReplicaImp::onInfoRequestTimer(Timers::Handle timer) {
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;
  tryToAskForMissingInfo();
  auto newPeriod = milliseconds(dynamicUpperLimitOfRounds->upperLimit() / 2);
  timers_.reset(timer, newPeriod);
  metric_info_request_timer_.Get().Set(dynamicUpperLimitOfRounds->upperLimit() / 2);
}

template <>
void ReplicaImp::onMessage<SimpleAckMsg>(std::unique_ptr<SimpleAckMsg> message) {
  metric_received_simple_acks_++;
  auto *msg = message.release();
  SCOPED_MDC_SEQ_NUM(std::to_string(msg->seqNumber()));
  uint16_t relatedMsgType = (uint16_t)msg->ackData();  // TODO(GG): does this make sense ?
  if (retransmissionsLogicEnabled) {
    LOG_DEBUG(CNSUS, "Received SimpleAckMsg" << KVLOG(msg->senderId(), msg->seqNumber(), relatedMsgType));
    retransmissionsManager->onAck(msg->senderId(), msg->seqNumber(), relatedMsgType);
  } else {
    LOG_WARN(CNSUS, "Received Ack, but retransmissions not enabled. " << KVLOG(msg->senderId(), relatedMsgType));
  }

  delete msg;
}

template <>
void ReplicaImp::onMessage<ReplicaRestartReadyMsg>(std::unique_ptr<ReplicaRestartReadyMsg> message) {
  auto *msg = message.release();
  auto &restart_msgs = restart_ready_msgs_[static_cast<uint8_t>(msg->getReason())];
  if (restart_msgs.find(msg->idOfGeneratedReplica()) == restart_msgs.end()) {
    restart_msgs[msg->idOfGeneratedReplica()] = std::make_unique<ReplicaRestartReadyMsg>(msg);
#ifdef ENABLE_ALL_METRICS
    metric_received_restart_ready_++;
#endif
  } else {
    LOG_INFO(GL,
             "Received multiple ReplicaRestartReadyMsg from sender_id "
                 << std::to_string(msg->idOfGeneratedReplica()) << " with seq_num" << std::to_string(msg->seqNum()));
    delete msg;
    return;
  }
  LOG_INFO(GL,
           "Received ReplicaRestartReadyMsg from sender_id " << std::to_string(msg->idOfGeneratedReplica())
                                                             << " with seq_num" << std::to_string(msg->seqNum()));
  bool restart_bft_flag = bftEngine::ControlStateManager::instance().getRestartBftFlag();
  uint32_t targetNumOfMsgs =
      (restart_bft_flag ? (config_.getnumReplicas() - config_.getfVal()) : config_.getnumReplicas());
  if (restart_msgs.size() == targetNumOfMsgs) {
    LOG_INFO(GL, "Target number = " << targetNumOfMsgs << " of restart ready msgs are recieved. Send restart proof");
    sendReplicasRestartReadyProof(static_cast<uint8_t>(msg->getReason()));
  }
  delete msg;
}

template <>
void ReplicaImp::onMessage<ReplicasRestartReadyProofMsg>(std::unique_ptr<ReplicasRestartReadyProofMsg> message) {
  auto *msg = message.release();
  LOG_INFO(GL,
           "Received  ReplicasRestartReadyProofMsg from sender_id "
               << std::to_string(msg->idOfGeneratedReplica()) << " with seq_num" << std::to_string(msg->seqNum()));
  metric_received_restart_proof_++;
  ControlStateManager::instance().onRestartProof(msg->seqNum(), static_cast<uint8_t>(msg->getRestartReason()));
  delete msg;
}

void ReplicaImp::sendReplicasRestartReadyProof(uint8_t reason) {
  auto seq_num_to_stop_at = ControlStateManager::instance().getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value()) {
    unique_ptr<ReplicasRestartReadyProofMsg> restartProofMsg(
        ReplicasRestartReadyProofMsg::create(config_.getreplicaId(),
                                             seq_num_to_stop_at.value(),
                                             static_cast<ReplicasRestartReadyProofMsg::RestartReason>(reason)));
    ConcordAssertGE(restart_ready_msgs_[reason].size(), static_cast<size_t>(config_.numReplicas - config_.fVal));
    for (auto &[_, v] : restart_ready_msgs_[reason]) {
      (void)_;  // unused variable hack
      restartProofMsg->addElement(v);
    }
    restartProofMsg->finalizeMessage();
    sendToAllOtherReplicas(restartProofMsg.get());
    metric_received_restart_proof_++;     // for self
    std::this_thread::sleep_for(1000ms);  // Sleep in order to let the os complete the send before shutting down
    bftEngine::ControlStateManager::instance().onRestartProof(restartProofMsg->seqNum(), reason);
  }
}

void ReplicaImp::onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum) {
  // TODO(GG): simple implementation - should be improved
  tryToSendStatusReport();
}

void ReplicaImp::onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum) {
  // TODO(GG): simple implementation - should be improved
  tryToSendStatusReport();
}

void ReplicaImp::onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum) {
  // TODO(GG): simple implementation - should be improved
  tryToSendStatusReport();
}

void ReplicaImp::onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum) {
  // TODO(GG): simple implementation - should be improved
  tryToSendStatusReport();
}

ReplicaImp::ReplicaImp(const LoadedReplicaData &ld,
                       shared_ptr<IRequestsHandler> requestsHandler,
                       IStateTransfer *stateTrans,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<PersistentStorage> persistentStorage,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers,
                       concordUtil::Timers &timers,
                       shared_ptr<concord::performance::PerformanceManager> pm,
                       shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm)
    : ReplicaImp(false,
                 ld.repConfig,
                 requestsHandler,
                 stateTrans,
                 ld.sigManager,
                 ld.repsInfo,
                 ld.viewsManager,
                 msgsCommunicator,
                 msgHandlers,
                 timers,
                 pm,
                 sm,
                 persistentStorage) {
  LOG_INFO(GL, "Initialising Replica with LoadedReplicaData");
  ConcordAssertNE(persistentStorage, nullptr);

  ps_ = persistentStorage;
  bftEngine::ControlStateManager::instance().setRemoveMetadataFunc([&](bool) { ps_->setEraseMetadataStorageFlag(); });
  bftEngine::ControlStateManager::instance().setRestartReadyFunc(
      [&](uint8_t reason, const std::string &extraData) { sendRepilcaRestartReady(reason, extraData); });
  bftEngine::EpochManager::instance().setNewEpochFlagHandler(
      std::bind(&PersistentStorage::setNewEpochFlag, ps_, placeholders::_1));
  lastAgreedView = ld.viewsManager->latestActiveView();

  if (ld.viewsManager->viewIsActive(lastAgreedView)) {
    viewsManager->setViewFromRecovery(lastAgreedView);
  } else {
    viewsManager->setViewFromRecovery(lastAgreedView + 1);
    ViewChangeMsg *t = ld.viewsManager->getMyLatestViewChangeMsg();
    ConcordAssert(t != nullptr);
    ConcordAssert(t->newView() == getCurrentView());
    viewsManager->insertStoredComplaintsIntoVCMsg(t);
    t->finalizeMessage();  // needed to initialize the VC message
  }

  metric_view_.Get().Set(getCurrentView());
  metric_last_agreed_view_.Get().Set(lastAgreedView);
  metric_current_primary_.Get().Set(getCurrentView() % config_.getnumReplicas());

  const bool inView = ld.viewsManager->viewIsActive(getCurrentView());

  primaryLastUsedSeqNum = ld.primaryLastUsedSeqNum;
  metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
  lastStableSeqNum = ld.lastStableSeqNum;
  metric_last_stable_seq_num_.Get().Set(lastStableSeqNum);
  lastExecutedSeqNum = ld.lastExecutedSeqNum;
  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
  strictLowerBoundOfSeqNums = ld.strictLowerBoundOfSeqNums;
  maxSeqNumTransferredFromPrevViews = ld.maxSeqNumTransferredFromPrevViews;
  lastViewThatTransferredSeqNumbersFullyExecuted = ld.lastViewThatTransferredSeqNumbersFullyExecuted;
  metric_total_committed_sn_ += lastExecutedSeqNum;
  mainLog->resetAll(lastStableSeqNum + 1);
  checkpointsLog->resetAll(lastStableSeqNum);

  bool viewIsActive = inView;
  LOG_INFO(GL,
           "Restarted ReplicaImp from persistent storage. " << KVLOG(getCurrentView(),
                                                                     lastAgreedView,
                                                                     viewIsActive,
                                                                     primaryLastUsedSeqNum,
                                                                     lastStableSeqNum,
                                                                     lastExecutedSeqNum,
                                                                     strictLowerBoundOfSeqNums,
                                                                     maxSeqNumTransferredFromPrevViews,
                                                                     lastViewThatTransferredSeqNumbersFullyExecuted));

  if (ld.lastStableSeqNum > 0) {
    auto &CheckpointInfo = checkpointsLog->get(ld.lastStableSeqNum);
    for (const auto &m : ld.lastStableCheckpointProof) {
      CheckpointInfo.addCheckpointMsg(m, m->idOfGeneratedReplica());
    }
  }

  if (inView) {
    const bool isPrimaryOfView = (repsInfo->primaryOfView(getCurrentView()) == config_.getreplicaId());

    SeqNum s = ld.lastStableSeqNum;

    for (size_t i = 0; i < kWorkWindowSize; i++) {
      s++;
      ConcordAssert(mainLog->insideActiveWindow(s));

      const SeqNumData &seqNumData = ld.seqNumWinArr[i];

      if (!seqNumData.isPrePrepareMsgSet()) continue;

      // such properties should be verified by the code the loads the persistent data
      ConcordAssertEQ(seqNumData.getPrePrepareMsg()->seqNumber(), s);

      SeqNumInfo &seqNumInfo = mainLog->get(s);

      // add prePrepareMsg

      if (isPrimaryOfView)
        seqNumInfo.addSelfMsg(seqNumData.getPrePrepareMsg(), true);
      else
        seqNumInfo.addMsg(seqNumData.getPrePrepareMsg(), true);

      ConcordAssert(seqNumData.getPrePrepareMsg()->equals(*seqNumInfo.getPrePrepareMsg()));

      const CommitPath pathInPrePrepare = seqNumData.getPrePrepareMsg()->firstPath();

      // TODO(GG): check this when we load the data from disk
      ConcordAssertOR(pathInPrePrepare != CommitPath::SLOW, seqNumData.getSlowStarted());

      if (pathInPrePrepare != CommitPath::SLOW) {
        // add PartialCommitProofMsg

        PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
        ConcordAssert(seqNumData.getPrePrepareMsg()->equals(*pp));
        Digest &ppDigest = pp->digestOfRequests();
        const SeqNum seqNum = pp->seqNumber();

        std::shared_ptr<IThresholdSigner> commitSigner;

        ConcordAssertOR((config_.getcVal() != 0), (pathInPrePrepare != CommitPath::FAST_WITH_THRESHOLD));

        if ((pathInPrePrepare == CommitPath::FAST_WITH_THRESHOLD) && (config_.getcVal() > 0))
          commitSigner = CryptoManager::instance().thresholdSignerForCommit(seqNum);
        else
          commitSigner = CryptoManager::instance().thresholdSignerForOptimisticCommit(seqNum);

        Digest tmpDigest;
        ppDigest.calcCombination(getCurrentView(), seqNum, tmpDigest);

        PartialCommitProofMsg *p = new PartialCommitProofMsg(
            config_.getreplicaId(), getCurrentView(), seqNum, pathInPrePrepare, tmpDigest, commitSigner);
        seqNumInfo.addFastPathSelfPartialCommitMsgAndDigest(p, tmpDigest);
      }

      if (seqNumData.getSlowStarted()) {
        startSlowPath(seqNumInfo);

        // add PreparePartialMsg
        PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
        PreparePartialMsg *p =
            PreparePartialMsg::create(getCurrentView(),
                                      pp->seqNumber(),
                                      config_.getreplicaId(),
                                      pp->digestOfRequests(),
                                      CryptoManager::instance().thresholdSignerForSlowPathCommit(pp->seqNumber()));
        ConcordAssert(seqNumInfo.addSelfMsg(p, true));
      }

      if (seqNumData.isPrepareFullMsgSet()) {
        try {
          ConcordAssert(seqNumInfo.addMsg(seqNumData.getPrepareFullMsg(), true));
        } catch (const std::exception &ex) {
          LOG_FATAL(GL, "Failed to add sn " << s << " to main log, reason: " << ex.what());
          throw;
        }

        Digest digest;
        seqNumData.getPrePrepareMsg()->digestOfRequests().digestOfDigest(digest);

        CommitPartialMsg *c = CommitPartialMsg::create(getCurrentView(),
                                                       s,
                                                       config_.getreplicaId(),
                                                       digest,
                                                       CryptoManager::instance().thresholdSignerForSlowPathCommit(s));

        ConcordAssert(seqNumInfo.addSelfCommitPartialMsgAndDigest(c, digest, true));
      }

      if (seqNumData.isCommitFullMsgSet()) {
        try {
          ConcordAssert(seqNumInfo.addMsg(seqNumData.getCommitFullMsg(), true));
        } catch (const std::exception &ex) {
          LOG_FATAL(GL, "Failed to add sn [" << s << "] to main log, reason: " << ex.what());
          throw;
        }

        ConcordAssert(seqNumData.getCommitFullMsg()->equals(*seqNumInfo.getValidCommitFullMsg()));
      }

      if (seqNumData.isFullCommitProofMsgSet()) {
        ConcordAssert(seqNumInfo.addFastPathFullCommitMsg(seqNumData.getFullCommitProofMsg(), true /*directAdd*/));
        ConcordAssert(seqNumData.getFullCommitProofMsg()->equals(*seqNumInfo.getFastPathFullCommitProofMsg()));
      }

      if (seqNumData.getForceCompleted()) seqNumInfo.forceComplete();
    }
  }

  ConcordAssert(ld.lastStableSeqNum % checkpointWindowSize == 0);

  for (SeqNum s = ld.lastStableSeqNum; s <= ld.lastStableSeqNum + kWorkWindowSize; s = s + checkpointWindowSize) {
    size_t i = (s - ld.lastStableSeqNum) / checkpointWindowSize;
    ConcordAssertLT(i, (sizeof(ld.checkWinArr) / sizeof(ld.checkWinArr[0])));
    const CheckData &checkData = ld.checkWinArr[i];

    ConcordAssert(checkpointsLog->insideActiveWindow(s));
    ConcordAssert(s == 0 ||                                                         // no checkpoints yet
                  s > ld.lastStableSeqNum ||                                        // not stable
                  checkData.isCheckpointMsgSet() ||                                 // if stable need to be set
                  ld.lastStableSeqNum == ld.lastExecutedSeqNum - kWorkWindowSize);  // after ST last executed may be on
    // the upper working window boundary

    if (!checkData.isCheckpointMsgSet()) continue;

    auto &checkInfo = checkpointsLog->get(s);

    ConcordAssertEQ(checkData.getCheckpointMsg()->seqNumber(), s);
    ConcordAssertEQ(checkData.getCheckpointMsg()->senderId(), config_.getreplicaId());
    ConcordAssertOR((s != ld.lastStableSeqNum), checkData.getCheckpointMsg()->isStableState());

    if (s != ld.lastStableSeqNum) {  // We have already added all msgs for our last stable Checkpoint
      checkInfo.addCheckpointMsg(checkData.getCheckpointMsg(), config_.getreplicaId());
      ConcordAssert(checkInfo.selfCheckpointMsg()->equals(*checkData.getCheckpointMsg()));
    } else {
      delete checkData.getCheckpointMsg();
    }

    if (checkData.getCompletedMark()) checkInfo.tryToMarkCheckpointCertificateCompleted();
  }

  if (ld.isExecuting) {
    ConcordAssert(viewsManager->viewIsActive(getCurrentView()));
    ConcordAssert(mainLog->insideActiveWindow(lastExecutedSeqNum + 1));
    const SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum + 1);
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
    ConcordAssertNE(pp, nullptr);
    ConcordAssertEQ(pp->seqNumber(), lastExecutedSeqNum + 1);
    ConcordAssertEQ(pp->viewNumber(), getCurrentView());
    ConcordAssertGT(pp->numberOfRequests(), 0);

    Bitmap b = ld.validRequestsThatAreBeingExecuted;
    size_t expectedValidRequests = 0;
    for (uint32_t i = 0; i < b.numOfBits(); i++) {
      if (b.get(i)) expectedValidRequests++;
    }
    ConcordAssertLE(expectedValidRequests, pp->numberOfRequests());

    recoveringFromExecutionOfRequests = true;
    mapOfRecoveredRequests = b;
    recoveredTime = ld.timeInTicks;
  }
}

ReplicaImp::ReplicaImp(const ReplicaConfig &config,
                       shared_ptr<IRequestsHandler> requestsHandler,
                       IStateTransfer *stateTrans,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<PersistentStorage> persistentStorage,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers,
                       concordUtil::Timers &timers,
                       shared_ptr<concord::performance::PerformanceManager> pm,
                       shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm)
    : ReplicaImp(true,
                 config,
                 requestsHandler,
                 stateTrans,
                 nullptr,
                 nullptr,
                 nullptr,
                 msgsCommunicator,
                 msgHandlers,
                 timers,
                 pm,
                 sm,
                 persistentStorage) {
  LOG_INFO(GL, "Initialising Replica with ReplicaConfig");
  if (persistentStorage != nullptr) {
    ps_ = persistentStorage;
    bftEngine::ControlStateManager::instance().setRemoveMetadataFunc([&](bool) { ps_->setEraseMetadataStorageFlag(); });
    bftEngine::ControlStateManager::instance().setRestartReadyFunc(
        [&](uint8_t reason, const std::string &extraData) { sendRepilcaRestartReady(reason, extraData); });
    bftEngine::EpochManager::instance().setNewEpochFlagHandler(
        std::bind(&PersistentStorage::setNewEpochFlag, ps_, placeholders::_1));
  }
}

ReplicaImp::ReplicaImp(bool firstTime,
                       const ReplicaConfig &config,
                       shared_ptr<IRequestsHandler> requestsHandler,
                       IStateTransfer *stateTrans,
                       SigManager *sigManager,
                       ReplicasInfo *replicasInfo,
                       ViewsManager *viewsMgr,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers,
                       concordUtil::Timers &timers,
                       shared_ptr<concord::performance::PerformanceManager> pm,
                       shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm,
                       shared_ptr<PersistentStorage> ps)
    : ReplicaForStateTransfer(config, requestsHandler, stateTrans, msgsCommunicator, msgHandlers, firstTime, timers),
      viewChangeProtocolEnabled{config.viewChangeProtocolEnabled},
      autoPrimaryRotationEnabled{config.autoPrimaryRotationEnabled},
      restarted_{!firstTime},
      internalThreadPool("ReplicaImp::internalThreadPool"),
      MAIN_THREAD_ID{std::this_thread::get_id()},
      postExecThread_{"ReplicaImp::postExecThread"},
      replyBuffer{static_cast<char *>(std::malloc(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)))},
      timeOfLastStateSynch{getMonotonicTime()},    // TODO(GG): TBD
      timeOfLastViewEntrance{getMonotonicTime()},  // TODO(GG): TBD
      timeOfLastAgreedView{getMonotonicTime()},    // TODO(GG): TBD
      pm_{pm},
      sm_{sm ? sm : std::make_shared<concord::secretsmanager::SecretsManagerPlain>()},
      metric_view_{metrics_.RegisterGauge("view", 0)},
      metric_last_stable_seq_num_{metrics_.RegisterGauge("lastStableSeqNum", lastStableSeqNum)},
      metric_last_executed_seq_num_{metrics_.RegisterGauge("lastExecutedSeqNum", lastExecutedSeqNum)},
      metric_last_agreed_view_{metrics_.RegisterGauge("lastAgreedView", lastAgreedView)},
      metric_current_active_view_{metrics_.RegisterGauge("currentActiveView", 0)},
      metric_viewchange_timer_{metrics_.RegisterGauge("viewChangeTimer", 0)},
      metric_retransmissions_timer_{metrics_.RegisterGauge("retransmissionTimer", 0)},
      metric_status_report_timer_{metrics_.RegisterGauge("statusReportTimer", 0)},
      metric_slow_path_timer_{metrics_.RegisterGauge("slowPathTimer", 0)},
      metric_info_request_timer_{metrics_.RegisterGauge("infoRequestTimer", 0)},
      metric_current_primary_{metrics_.RegisterGauge("currentPrimary", 0)},
      metric_concurrency_level_{metrics_.RegisterGauge("concurrencyLevel", config_.getconcurrencyLevel())},
      metric_primary_last_used_seq_num_{metrics_.RegisterGauge("primaryLastUsedSeqNum", primaryLastUsedSeqNum)},
      metric_on_call_back_of_super_stable_cp_{metrics_.RegisterGauge("OnCallBackOfSuperStableCP", 0)},
      metric_sent_replica_asks_to_leave_view_msg_{metrics_.RegisterGauge("sentReplicaAsksToLeaveViewMsg", 0)},
      metric_bft_batch_size_{metrics_.RegisterGauge("bft_batch_size", 0)},
      my_id{metrics_.RegisterGauge("my_id", config.replicaId)},
      primary_queue_size_{metrics_.RegisterGauge("primary_queue_size", 0)},
      consensus_avg_time_{metrics_.RegisterGauge("consensus_rolling_avg_time", 0)},
      accumulating_batch_avg_time_{metrics_.RegisterGauge("accumualating_batch_avg_time", 0)},
      deferredRORequestsMetric_{metrics_.RegisterGauge("deferrdRORequests", 0)},
      deferredMessagesMetric_{metrics_.RegisterGauge("deferredMessages", 0)},
      metric_first_commit_path_{metrics_.RegisterStatus(
          "firstCommitPath", CommitPathToStr(ControllerWithSimpleHistory_debugInitialFirstPath))},
      batch_closed_on_logic_off_{metrics_.RegisterCounter("total_number_batch_closed_on_logic_off")},
      batch_closed_on_logic_on_{metrics_.RegisterCounter("total_number_batch_closed_on_logic_on")},
      metric_indicator_of_non_determinism_{metrics_.RegisterCounter("indicator_of_non_determinism")},
      metric_total_committed_sn_{metrics_.RegisterCounter("total_committed_seqNum")},
      metric_total_slowPath_{metrics_.RegisterCounter("totalSlowPaths")},
      metric_total_slowPath_requests_{metrics_.RegisterCounter("totalSlowPathRequests")},
      metric_received_start_slow_commits_{metrics_.RegisterCounter("receivedStartSlowCommitMsgs")},
      metric_total_fastPath_{metrics_.RegisterCounter("totalFastPaths")},
      metric_committed_slow_{metrics_.RegisterCounter("CommittedSlow")},
      metric_committed_fast_threshold_{metrics_.RegisterCounter("CommittedFastThreshold")},
      metric_committed_fast_{metrics_.RegisterCounter("CommittedFast")},
      metric_total_fastPath_requests_{metrics_.RegisterCounter("totalFastPathRequests")},
      metric_received_internal_msgs_{metrics_.RegisterCounter("receivedInternalMsgs")},
      metric_received_client_requests_{metrics_.RegisterCounter("receivedClientRequestMsgs")},
      metric_received_pre_prepares_{metrics_.RegisterCounter("receivedPrePrepareMsgs")},
      metric_received_partial_commit_proofs_{metrics_.RegisterCounter("receivedPartialCommitProofMsgs")},
      metric_received_full_commit_proofs_{metrics_.RegisterCounter("receivedFullCommitProofMsgs")},
      metric_received_prepare_partials_{metrics_.RegisterCounter("receivedPreparePartialMsgs")},
      metric_received_commit_partials_{metrics_.RegisterCounter("receivedCommitPartialMsgs")},
      metric_received_prepare_fulls_{metrics_.RegisterCounter("receivedPrepareFullMsgs")},
      metric_received_commit_fulls_{metrics_.RegisterCounter("receivedCommitFullMsgs")},
      metric_received_checkpoints_{metrics_.RegisterCounter("receivedCheckpointMsgs")},
      metric_received_replica_statuses_{metrics_.RegisterCounter("receivedReplicaStatusMsgs")},
      metric_received_view_changes_{metrics_.RegisterCounter("receivedViewChangeMsgs")},
      metric_received_new_views_{metrics_.RegisterCounter("receivedNewViewMsgs")},
      metric_received_req_missing_datas_{metrics_.RegisterCounter("receivedReqMissingDataMsgs")},
      metric_received_simple_acks_{metrics_.RegisterCounter("receivedSimpleAckMsgs")},
      metric_sent_status_msgs_not_due_timer_{metrics_.RegisterCounter("sentStatusMsgsNotDueTime")},
      metric_sent_req_for_missing_data_{metrics_.RegisterCounter("sentReqForMissingData")},
      metric_sent_checkpoint_msg_due_to_status_{metrics_.RegisterCounter("sentCheckpointMsgDueToStatus")},
      metric_sent_viewchange_msg_due_to_status_{metrics_.RegisterCounter("sentViewChangeMsgDueToTimer")},
      metric_sent_newview_msg_due_to_status_{metrics_.RegisterCounter("sentNewviewMsgDueToCounter")},
      metric_sent_preprepare_msg_due_to_status_{metrics_.RegisterCounter("sentPreprepareMsgDueToStatus")},
      metric_sent_replica_asks_to_leave_view_msg_due_to_status_{
          metrics_.RegisterCounter("sentReplicaAsksToLeaveViewMsgDueToStatus")},
#ifdef ENABLE_ALL_METRICS
      metric_sent_preprepare_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentPreprepareMsgDueToReqMissingData")},
      metric_sent_startSlowPath_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentStartSlowPathMsgDueToReqMissingData")},
      metric_sent_partialCommitProof_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentPartialCommitProofMsgDueToReqMissingData")},
      metric_sent_preparePartial_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentPrepreparePartialMsgDueToReqMissingData")},
      metric_sent_prepareFull_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentPrepareFullMsgDueToReqMissingData")},
      metric_sent_commitPartial_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentCommitPartialMsgDueToRewMissingData")},
      metric_sent_commitFull_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentCommitFullMsgDueToReqMissingData")},
      metric_sent_fullCommitProof_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentFullCommitProofMsgDueToReqMissingData")},
#endif
      metric_total_finished_consensuses_{metrics_.RegisterCounter("totalOrderedRequests")},
      metric_total_preexec_requests_executed_{metrics_.RegisterCounter("totalPreExecRequestsExecuted")},
#ifdef ENABLE_ALL_METRICS
      metric_received_restart_ready_{metrics_.RegisterCounter("receivedRestartReadyMsg", 0)},
#endif
      metric_received_restart_proof_{metrics_.RegisterCounter("receivedRestartProofMsg", 0)},
      metric_consensus_duration_{metrics_, "consensusDuration", 1000, 100, true},
      metric_post_exe_duration_{metrics_, "postExeDuration", 1000, 100, true},
      metric_core_exe_func_duration_{metrics_, "postExeCoreFuncDuration", 1000, 100, true},
      metric_consensus_end_to_core_exe_duration_{metrics_, "consensusEndToExeStartDuration", 1000, 100, true},
      metric_post_exe_thread_idle_time_{metrics_, "PostExeThreadIdleDuration", 1000, 100, true},
      metric_post_exe_thread_active_time_{metrics_, "PostExeThreadActiveDuration", 1000, 100, true},
      metric_primary_batching_duration_{metrics_, "primaryBatchingDuration", 10000, 1000, true},
      consensus_times_(histograms_.consensus),
      checkpoint_times_(histograms_.checkpointFromCreationToStable),
      time_in_active_view_(histograms_.timeInActiveView),
      time_in_state_transfer_(histograms_.timeInStateTransfer),
      reqBatchingLogic_(*this, config_, metrics_, timers),
      replStatusHandlers_(*this) {
  ConcordAssertLT(config_.getreplicaId(), config_.getnumReplicas());
  // TODO(GG): more asserts on params !!!!!!!!!!!

  ConcordAssert(firstTime || ((replicasInfo != nullptr) && (viewsMgr != nullptr) && (sigManager != nullptr)));

  LOG_INFO(GL, "Initialising Replica" << KVLOG(firstTime));

  onViewNumCallbacks_.add([&](bool) {
    if (config_.keyExchangeOnStart && !KeyExchangeManager::instance().exchanged()) {
      LOG_INFO(GL, "key exchange has not been finished yet. Give it another try");
      KeyExchangeManager::instance().sendInitialKey(this);
    }
  });
  stateTransfer->addOnFetchingStateChangeCallback([&](uint64_t) {
    // With (n-f) initial key exchange support, if we have a lagged replica
    // which syncs its state through ST, we need to make sure that it completes
    // initial key exchange after completing ST
    if (!isCollectingState()) {
      if (ReplicaConfig::instance().getkeyExchangeOnStart() && !KeyExchangeManager::instance().exchanged()) {
        KeyExchangeManager::instance().sendInitialKey(this, lastExecutedSeqNum);
        LOG_INFO(GL, "Send initial key exchange after completing state transfer " << KVLOG(lastExecutedSeqNum));
      }
    } else {
      LOG_WARN(GL, "State Transfer is still collecting! Initial key exchange cannot yet be initiated!");
    }
  });
  registerMsgHandlers();
  replStatusHandlers_.registerStatusHandlers();
  maxQueueSize_ = config_.postExecutionQueuesSize;
  // Register metrics component with the default aggregator.
  metrics_.Register();

  if (firstTime) {
    repsInfo = new ReplicasInfo(config_, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);
    sigManager_.reset(SigManager::init(config_.replicaId,
                                       config_.replicaPrivateKey,
                                       config_.publicKeysOfReplicas,
                                       concord::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                       ReplicaConfig::instance().getPublicKeysOfClients(),
                                       concord::crypto::KeyFormat::PemFormat,
                                       {{repsInfo->getIdOfOperator(),
                                         ReplicaConfig::instance().getOperatorPublicKey(),
                                         concord::crypto::KeyFormat::PemFormat}},
                                       *repsInfo));
    viewsManager = new ViewsManager(repsInfo);
  } else {
    repsInfo = replicasInfo;
    sigManager_.reset(sigManager);
    viewsManager = viewsMgr;
  }
  bft::communication::StateControl::instance().setGetPeerPubKeyMethod(
      [&](uint32_t id) { return sigManager_->getPublicKeyOfVerifier(id); });

  clientsManager = std::make_shared<ClientsManager>(ps,
                                                    repsInfo->idsOfClientProxies(),
                                                    repsInfo->idsOfExternalClients(),
                                                    repsInfo->idsOfIClientServices(),
                                                    repsInfo->idsOfInternalClients(),
                                                    metrics_);
  internalBFTClient_.reset(
      new InternalBFTClient(*(repsInfo->idsOfInternalClients()).cbegin() + config_.getreplicaId(), msgsCommunicator_));

  // autoPrimaryRotationEnabled implies viewChangeProtocolEnabled
  // Note: "p=>q" is equivalent to "not p or q"
  ConcordAssertOR(!autoPrimaryRotationEnabled, viewChangeProtocolEnabled);

  viewChangeTimerMilli = (viewChangeTimeoutMilli > 0) ? viewChangeTimeoutMilli : config.viewChangeTimerMillisec;
  ConcordAssertGT(viewChangeTimerMilli, 0);

  if (autoPrimaryRotationEnabled) {
    autoPrimaryRotationTimerMilli =
        (autoPrimaryRotationTimerMilli > 0) ? autoPrimaryRotationTimerMilli : config.autoPrimaryRotationTimerMillisec;
    ConcordAssertGT(autoPrimaryRotationTimerMilli, 0);
  }

  // TODO(GG): use config ...
  dynamicUpperLimitOfRounds = new DynamicUpperLimitWithSimpleFilter<int64_t>(400, 2, 2500, 70, 32, 1000, 2, 2);

  mainLog.reset(new WindowOfSeqNumInfo(1, (InternalReplicaApi *)this));

  checkpointsLog = new SequenceWithActiveWindow<kWorkWindowSize + 2 * checkpointWindowSize,
                                                checkpointWindowSize,
                                                SeqNum,
                                                CheckpointInfo<>,
                                                CheckpointInfo<>>(0, (InternalReplicaApi *)this);

  // create controller . TODO(GG): do we want to pass the controller as a parameter ?
  controller = new ControllerWithSimpleHistory(
      config_.getcVal(), config_.getfVal(), config_.getreplicaId(), getCurrentView(), primaryLastUsedSeqNum);

  if (retransmissionsLogicEnabled) {
    retransmissionsManager =
        std::make_unique<RetransmissionsManager>(&internalThreadPool, &getIncomingMsgsStorage(), kWorkWindowSize, 0);
  }

  ticks_gen_ = concord::cron::TicksGenerator::create(internalBFTClient_,
                                                     *clientsManager,
                                                     msgsCommunicator_->getIncomingMsgsStorage(),
                                                     config.ticksGeneratorPollPeriod);

  if (currentViewIsActive()) {
    time_in_active_view_.start();
  }

  KeyExchangeManager::InitData id{
      internalBFTClient_, &CryptoManager::instance(), &CryptoManager::instance(), sm_, clientsManager.get(), &timers_};

  KeyExchangeManager::instance(&id);
  DbCheckpointManager::instance(internalBFTClient_.get());

  if (getReplicaConfig().dbCheckpointFeatureEnabled == true) {
    onSeqNumIsStableCallbacks_.add([this](SeqNum seqNum) {
      auto currTime = std::chrono::system_clock::now().time_since_epoch();
      auto timeSinceLastSnapshot = (std::chrono::duration_cast<std::chrono::seconds>(currTime) -
                                    DbCheckpointManager::instance().getLastCheckpointCreationTime())
                                       .count();
      auto seqNumsExecutedSinceLastDbCheckPt = seqNum - DbCheckpointManager::instance().getLastDbCheckpointSeqNum();
      if (getReplicaConfig().dbCheckpointFeatureEnabled && seqNum && isCurrentPrimary() &&
          (seqNumsExecutedSinceLastDbCheckPt >= getReplicaConfig().dbCheckPointWindowSize) &&
          (timeSinceLastSnapshot >= getReplicaConfig().dbSnapshotIntervalSeconds.count())) {
        DbCheckpointManager::instance().sendInternalCreateDbCheckpointMsg(seqNum, false);
        LOG_INFO(GL, "send msg to create Db Checkpoint:" << KVLOG(seqNum));
      }
      DbCheckpointManager::instance().onStableSeqNum(seqNum);
    });
    DbCheckpointManager::instance().setGetLastStableSeqNumCb([this]() -> SeqNum { return lastStableSeqNum; });
  }
  LOG_INFO(GL, "ReplicaConfig parameters: " << config);
  auto numThreads = 8;
  if (!firstTime) {
    numThreads = config_.getsizeOfInternalThreadPool();
  }
  LOG_INFO(GL, "Starting internal replica thread pool. " << KVLOG(numThreads));
  internalThreadPool.start(numThreads);
  postExecThread_.start(1);  // This thread pool should always be with 1 thread to maintain execution sequential;
}

ReplicaImp::~ReplicaImp() {
  // TODO(GG): rewrite this method !!!!!!!! (notice that the order may be important here ).
  // TODO(GG): don't delete objects that are passed as params (TBD)
  if (!internalThreadPool.isStopped()) {
    LOG_FATAL(GL, "internalThreadPool should be stopped in ReplicaImp's stop function and not in destructor");
    ConcordAssert(false);
  }

  if (!postExecThread_.isStopped()) {
    LOG_FATAL(GL, "postExecThread_ should be stopped in ReplicaImp's stop function and not in destructor");
    ConcordAssert(false);
  }

  delete viewsManager;
  delete controller;
  delete dynamicUpperLimitOfRounds;
  delete checkpointsLog;
  delete repsInfo;
  free(replyBuffer);

  tableOfStableCheckpoints.clear();

  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::freeDebugStatisticsData();
  }
}

void ReplicaImp::stop() {
  LOG_DEBUG(GL, "ReplicaImp::stop started");

  // stop components dependent on ReplicaImp
  stopCallbacks_.invokeAll();

  internalThreadPool.stop();
  postExecThread_.stop();

  if (retransmissionsLogicEnabled) {
    timers_.cancel(retranTimer_);
  }
  timers_.cancel(slowPathTimer_);
  timers_.cancel(infoReqTimer_);
  timers_.cancel(statusReportTimer_);
  timers_.cancel(clientRequestsRetransmissionTimer_);
  if (viewChangeProtocolEnabled) {
    timers_.cancel(viewChangeTimer_);
  }
  ReplicaForStateTransfer::stop();
  LOG_DEBUG(GL, "ReplicaImp::stop done");
}

void ReplicaImp::addTimers() {
  int statusReportTimerMilli =
      (sendStatusPeriodMilli > 0) ? sendStatusPeriodMilli : config_.getstatusReportTimerMillisec();
  ConcordAssertGT(statusReportTimerMilli, 0);
  metric_status_report_timer_.Get().Set(statusReportTimerMilli);
  statusReportTimer_ = timers_.add(milliseconds(statusReportTimerMilli),
                                   Timers::Timer::RECURRING,
                                   [this](Timers::Handle h) { onStatusReportTimer(h); });
  clientRequestsRetransmissionTimer_ = timers_.add(
      milliseconds(config_.clientRequestRetransmissionTimerMilli), Timers::Timer::RECURRING, [this](Timers::Handle h) {
        if (isCurrentPrimary() || isCollectingState() || ControlHandler::instance()->onPruningProcess()) return;
        auto currentTime = getMonotonicTime();
        for (const auto &[sn, msg] : requestsOfNonPrimary) {
          auto timeout = duration_cast<milliseconds>(currentTime - std::get<0>(msg)).count();
          if (timeout > (3 * config_.clientRequestRetransmissionTimerMilli)) {
            LOG_INFO(GL, "retransmitting client request in non primary due to timeout" << KVLOG(sn, timeout));
            requestsOfNonPrimary[sn].first = getMonotonicTime();
            send(std::get<1>(msg), currentPrimary());
          }
        }
      });
  if (viewChangeProtocolEnabled) {
    int t = viewChangeTimerMilli;
    if (autoPrimaryRotationEnabled && t > autoPrimaryRotationTimerMilli) t = autoPrimaryRotationTimerMilli;
    metric_viewchange_timer_.Get().Set(t / 2);
    // TODO(GG): What should be the time period here?
    // TODO(GG): Consider to split to 2 different timers
    viewChangeTimer_ =
        timers_.add(milliseconds(t / 2), Timers::Timer::RECURRING, [this](Timers::Handle h) { onViewsChangeTimer(h); });
  }
  if (retransmissionsLogicEnabled) {
    metric_retransmissions_timer_.Get().Set(retransmissionsTimerMilli);
    retranTimer_ = timers_.add(milliseconds(retransmissionsTimerMilli),
                               Timers::Timer::RECURRING,
                               [this](Timers::Handle h) { onRetransmissionsTimer(h); });
  }
  const int slowPathsTimerPeriod = controller->timeToStartSlowPathMilli();
  metric_slow_path_timer_.Get().Set(slowPathsTimerPeriod);
  slowPathTimer_ = timers_.add(
      milliseconds(slowPathsTimerPeriod), Timers::Timer::RECURRING, [this](Timers::Handle h) { onSlowPathTimer(h); });

  metric_info_request_timer_.Get().Set(dynamicUpperLimitOfRounds->upperLimit() / 2);
  infoReqTimer_ = timers_.add(milliseconds(dynamicUpperLimitOfRounds->upperLimit() / 2),
                              Timers::Timer::RECURRING,
                              [this](Timers::Handle h) { onInfoRequestTimer(h); });
}

void ReplicaImp::start() {
  LOG_INFO(GL, "Running ReplicaImp");
  sigManager_->SetAggregator(aggregator_);
  CheckpointMsg::setAggregator(aggregator_);
  KeyExchangeManager::instance().setAggregator(aggregator_);
  ReplicaForStateTransfer::start();

  if (config_.timeServiceEnabled) {
    time_service_manager_.emplace(aggregator_);
    LOG_INFO(GL, "Time Service enabled");
  }

  if (!firstTime_ || config_.getdebugPersistentStorageEnabled()) clientsManager->loadInfoFromReservedPages();
  addTimers();
  recoverRequests();
  // The following line will start the processing thread.
  // It must happen after the replica recovers requests in the main thread.
  msgsCommunicator_->startMsgsProcessing(config_.getreplicaId());

  if (ReplicaConfig::instance().getkeyExchangeOnStart() && !KeyExchangeManager::instance().exchanged()) {
    KeyExchangeManager::instance().sendInitialKey(this);
  } else {
    // If key exchange is disabled, first publish the replica's main (rsa/eddsa) key to clients
    if (ReplicaConfig::instance().publishReplicasMasterKeyOnStartup) KeyExchangeManager::instance().sendMainPublicKey();
  }
  KeyExchangeManager::instance().sendInitialClientsKeys(SigManager::instance()->getClientsPublicKeys());
}

void ReplicaImp::recoverRequests() {
  if (recoveringFromExecutionOfRequests) {
    LOG_INFO(GL, "Recovering execution of requests");
    if (config_.timeServiceEnabled) {
      time_service_manager_->recoverTime(recoveredTime);
    }
    SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum + 1);
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
    ConcordAssertNE(pp, nullptr);
    auto span = concordUtils::startSpan("bft_recover_requests_on_start");
    SCOPED_MDC_SEQ_NUM(std::to_string(pp->seqNumber()));
    const uint16_t numOfRequests = pp->numberOfRequests();
    executeRequestsInPrePrepareMsg(span, pp, true);
    metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
    // TODO: check if these metric updates can be removed since the replica is started over
    updateExecutedPathMetrics(seqNumInfo.slowPathStarted(), numOfRequests);
    recoveringFromExecutionOfRequests = false;
    mapOfRecoveredRequests = Bitmap();
  }
}

void ReplicaImp::executeReadOnlyRequest(concordUtils::SpanWrapper &parent_span, ClientRequestMsg *request) {
  ConcordAssert(request->isReadOnly());
  ConcordAssert(!isCollectingState());

  auto span = concordUtils::startChildSpan("bft_execute_read_only_request", parent_span);
  ClientReplyMsg reply(currentPrimary(), request->requestSeqNum(), config_.getreplicaId());
  uint16_t clientId = request->clientProxyId();
  int status = 0;
  bftEngine::IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  accumulatedRequests.push_back(bftEngine::IRequestsHandler::ExecutionRequest{clientId,
                                                                              static_cast<uint64_t>(lastExecutedSeqNum),
                                                                              request->getCid(),
                                                                              request->flags(),
                                                                              request->requestLength(),
                                                                              request->requestBuf(),
                                                                              "",
                                                                              reply.maxReplyLength(),
                                                                              reply.replyBuf(),
                                                                              request->requestSeqNum(),
                                                                              request->requestIndexInBatch(),
                                                                              request->result()});
  {
    TimeRecorder scoped_timer(*histograms_.executeReadOnlyRequest);
    bftRequestsHandler_->execute(accumulatedRequests, std::nullopt, request->getCid(), span);
  }
  IRequestsHandler::ExecutionRequest &single_request = accumulatedRequests.back();
  status = single_request.outExecutionStatus;
  const uint32_t actualReplyLength = single_request.outActualReplySize;
  const uint32_t actualReplicaSpecificInfoLength = single_request.outReplicaSpecificInfoSize;
  LOG_DEBUG(GL,
            "Executed read only request. " << KVLOG(clientId,
                                                    lastExecutedSeqNum,
                                                    request->requestLength(),
                                                    reply.maxReplyLength(),
                                                    actualReplyLength,
                                                    actualReplicaSpecificInfoLength,
                                                    status));
  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)
  if (!status) {
    if (actualReplyLength > 0) {
      reply.setReplyLength(actualReplyLength);
      reply.setReplicaSpecificInfoLength(actualReplicaSpecificInfoLength);
      send(&reply, clientId);
      return;
    } else {
      LOG_WARN(GL, "Received zero size response. " << KVLOG(clientId));
      strcpy(single_request.outReply, "Executed data is empty");
      single_request.outActualReplySize = strlen(single_request.outReply);
      status = static_cast<uint32_t>(bftEngine::OperationResult::EXEC_DATA_EMPTY);
    }

  } else {
    LOG_WARN(GL, "Received error while executing RO request. " << KVLOG(clientId, status));
  }
  ClientReplyMsg replyMsg(
      0, request->requestSeqNum(), single_request.outReply, single_request.outActualReplySize, status);
  send(&replyMsg, clientId);
  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onRequestCompleted(true);
  }
}

void ReplicaImp::setConflictDetectionBlockId(const ClientRequestMsg &clientReqMsg,
                                             IRequestsHandler::ExecutionRequest &execReq) {
  ConcordAssertGT(clientReqMsg.requestLength(), sizeof(uint64_t));
  auto requestSize = clientReqMsg.requestLength() - sizeof(uint64_t);
  auto opt_block_id = *(reinterpret_cast<uint64_t *>(clientReqMsg.requestBuf() + requestSize));
  LOG_DEBUG(GL, "Conflict detection optimization block id is " << opt_block_id);
  execReq.blockId = opt_block_id;
  execReq.requestSize = requestSize;
}

// TODO(GG): understand and handle the update in
// https://github.com/vmware/concord-bft/commit/2d812aa53d3821e365809774afb30468134c026d
// TODO(GG): When the replica starts, this method should be called before we start receiving messages ( because we may
// have some committed seq numbers)
// TODO(GG): move the "logic related to requestMissingInfo" to the caller of this method (e.g., by returning a value
// to the caller)
// Pushes execution requests to another thread
void ReplicaImp::tryToStartOrFinishExecution(bool requestMissingInfo) {
  ConcordAssert(!isCollectingState());
  ConcordAssert(currentViewIsActive());
  ConcordAssertGE(lastExecutedSeqNum, lastStableSeqNum);

  if (activeExecutions_ > 0) {
    ConcordAssert(startedExecution);
    return;  // because this method will be called again as soon as the execution completes
  }

  if (lastExecutedSeqNum >= lastStableSeqNum + kWorkWindowSize) {
    if (startedExecution) {
      startedExecution = false;
      onExecutionFinish();
    }

    return;
  }

  const SeqNum nextExecutedSeqNum = lastExecutedSeqNum + 1;
  const SeqNumInfo &seqNumInfo = mainLog->get(nextExecutedSeqNum);
  PrePrepareMsg *prePrepareMsg = seqNumInfo.getPrePrepareMsg();

  const bool ready = (prePrepareMsg != nullptr) && (seqNumInfo.isCommitted__gg());

  if (!ready) {
    if (startedExecution) {
      startedExecution = false;
      onExecutionFinish();
    } else {
      if (requestMissingInfo) {
        LOG_INFO(GL,
                 "Asking for missing information: " << KVLOG(nextExecutedSeqNum, getCurrentView(), lastStableSeqNum));
        tryToSendReqMissingDataMsg(nextExecutedSeqNum);
      }
    }

    return;
  }

  if (!startedExecution) startedExecution = true;

  const bool allowParallel = config_.enablePostExecutionSeparation;

  startPrePrepareMsgExecution(prePrepareMsg, allowParallel, false);
}

// TODO(GG): this method is also used for recovery
// TODO(GG): notice that we use Internal messages (and we may use them during recovery)
// TODO(GG): handle histograms_.executeRequestsInPrePrepareMsg
void ReplicaImp::startPrePrepareMsgExecution(PrePrepareMsg *ppMsg,
                                             bool allowParallelExecution,
                                             bool recoverFromErrorInRequestsExecution) {
  // when we recover, we don't execute requests in parallel
  ConcordAssert(!recoverFromErrorInRequestsExecution ||
                !allowParallelExecution);  // recoverFromErrorInRequestsExecution --> !allowParallelExecution
  ConcordAssert(!isCollectingState());
  ConcordAssert(currentViewIsActive());
  ConcordAssertNE(ppMsg, nullptr);
  ConcordAssertEQ(ppMsg->seqNumber(), lastExecutedSeqNum + 1);
  ConcordAssertEQ(ppMsg->viewNumber(), getCurrentView());

  const uint16_t numOfRequests = ppMsg->numberOfRequests();

  // recoverFromErrorInRequestsExecution ==> (numOfRequests > 0)
  ConcordAssertOR(!recoverFromErrorInRequestsExecution, (numOfRequests > 0));

  if (numOfRequests > 0) {
    // TODO(GG): should be verified in the validation of the PrePrepare . Consdier to remove this assert
    ConcordAssert(!config_.timeServiceEnabled || ppMsg->getTime() > 0);

    histograms_.numRequestsInPrePrepareMsg->record(numOfRequests);
    Bitmap requestSet(numOfRequests);
    size_t reqIdx = 0;
    uint16_t numOfSpecialReqs = 0;
    RequestsIterator reqIter(ppMsg);
    char *requestBody = nullptr;

    bool shouldRunRequestsInParallel =
        false;  // true IFF we have requests that will be executed in parallel to the main replica thread

    //////////////////////////////////////////////////////////////////////
    // Phase 1:
    // a. Find the requests that should be executed
    // b. Send reply for each request that has already been executed
    // c. skip over invalid requests
    // d. count number of "special requests"
    //////////////////////////////////////////////////////////////////////

    if (!recoverFromErrorInRequestsExecution) {
      markSpecialRequests(reqIter,
                          requestBody,
                          numOfSpecialReqs,
                          reqIdx,
                          requestSet,
                          allowParallelExecution,
                          shouldRunRequestsInParallel);
    } else {  // if we recover
      requestSet = mapOfRecoveredRequests;
      // count numOfSpecialReqs
      while (reqIter.getAndGoToNext(requestBody)) {
        ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);
        if ((req.flags() & MsgFlag::KEY_EXCHANGE_FLAG) || (req.flags() & MsgFlag::RECONFIG_FLAG)) {
          numOfSpecialReqs++;
          reqIdx++;
          continue;
        }
        reqIdx++;
      }
      reqIter.restart();
    }
    executeAllPrePreparedRequests(allowParallelExecution,
                                  shouldRunRequestsInParallel,
                                  numOfSpecialReqs,
                                  ppMsg,
                                  requestSet,
                                  recoverFromErrorInRequestsExecution);

  } else  // if we don't have requests in ppMsg
  {
    // send internal message that will call to finishExecutePrePrepareMsg
    ConcordAssert(activeExecutions_ == 0);
    activeExecutions_ = 1;
    if (isCurrentPrimary()) {
      metric_post_exe_thread_active_time_.addStartTimeStamp(0);
      metric_post_exe_thread_idle_time_.finishMeasurement(0);
    }

    InternalMessage im = FinishPrePrepareExecutionInternalMsg{ppMsg, nullptr};  // TODO(GG): check....
    getIncomingMsgsStorage().pushInternalMsg(std::move(im));
  }
}

void ReplicaImp::markSpecialRequests(RequestsIterator &reqIter,
                                     char *requestBody,
                                     uint16_t &numOfSpecialReqs,
                                     size_t &reqIdx,
                                     Bitmap &requestSet,
                                     bool allowParallelExecution,
                                     bool &shouldRunRequestsInParallel) {
  while (reqIter.getAndGoToNext(requestBody)) {
    ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);
    //        SCOPED_MDC_CID(req.getCid());
    NodeIdType clientId = req.clientProxyId();
    if ((req.flags() & MsgFlag::KEY_EXCHANGE_FLAG) || (req.flags() & MsgFlag::RECONFIG_FLAG) ||
        (req.flags() & MsgFlag::CLIENTS_PUB_KEYS_FLAG)) {
      numOfSpecialReqs++;
      reqIdx++;
      continue;
    }

    if (req.requestLength() == 0) {
      if (clientId == currentPrimary())
        ++numValidNoOps;  // TODO(GG): do we want empty requests from non-primary replicas?
      reqIdx++;
      continue;
    }

    const bool validClient = isValidClient(clientId);
    if (!validClient) {
      ++numInvalidClients;
      //          LOG_WARN(GL, "The client is not valid" << KVLOG(clientId));
      reqIdx++;
      continue;
    }

    if (isReplyAlreadySentToClient(clientId, req.requestSeqNum())) {
      auto replyMsg = clientsManager->allocateReplyFromSavedOne(clientId, req.requestSeqNum(), currentPrimary());
      if (replyMsg) {
        send(replyMsg.get(), clientId);
      }
      reqIdx++;
      continue;
    }
    if (allowParallelExecution && !shouldRunRequestsInParallel) shouldRunRequestsInParallel = true;
    requestSet.set(reqIdx);
    reqIdx++;
  }
  reqIter.restart();

  if (ps_) {
    auto ticks = config_.timeServiceEnabled ? time_service_manager_->getTime().count() : 0;
    DescriptorOfLastExecution execDesc{lastExecutedSeqNum + 1, requestSet, ticks};
    ps_->beginWriteTran();
    ps_->setDescriptorOfLastExecution(execDesc);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }
}

void ReplicaImp::executeAllPrePreparedRequests(bool allowParallelExecution,
                                               bool shouldRunRequestsInParallel,
                                               uint16_t numOfSpecialReqs,
                                               PrePrepareMsg *ppMsg,
                                               Bitmap &requestSet,
                                               bool recoverFromErrorInRequestsExecution) {
  // !allowParallelExecution --> !shouldRunRequestsInParallel
  ConcordAssert(allowParallelExecution || !shouldRunRequestsInParallel);

  //////////////////////////////////////////////////////////////////////
  // Phase 2: execute requests + send replies
  // In this phase the application state may be changed. We also change data in the state transfer module.
  // TODO(GG): Explain what happens in recovery mode (what are the requirements from  the application, and from the
  // state transfer module.
  //////////////////////////////////////////////////////////////////////

  Timestamp time;
  if (config_.timeServiceEnabled) {
    ConcordAssert(ppMsg->getTime() > 0);  // TODO(GG): should be verified when receiving the PrePrepare message

    time.time_since_epoch = ConsensusTime(ppMsg->getTime());
    time.time_since_epoch = time_service_manager_->compareAndUpdate(time.time_since_epoch);

    LOG_DEBUG(GL, "Timestamp to be provided to the execution: " << time.time_since_epoch.count() << "ms");
  }

  if (numOfSpecialReqs > 0) executeSpecialRequests(ppMsg, numOfSpecialReqs, recoverFromErrorInRequestsExecution, time);

  ConcordAssert(activeExecutions_ == 0);
  activeExecutions_ = 1;
  if (isCurrentPrimary()) {
    metric_post_exe_thread_active_time_.addStartTimeStamp(0);
    metric_post_exe_thread_idle_time_.finishMeasurement(0);
  }
  if (shouldRunRequestsInParallel) {
    PostExecJob *j = new PostExecJob(ppMsg, requestSet, time, *this);
    postExecThread_.add(j);
  } else {
    executeRequests(ppMsg,
                    requestSet,
                    time);  // this method will send internal message that will call to finishExecutePrePrepareMsg
  }
}
// TODO(GG): explain "special requests" (TBD - mention that thier effect on the res pages should be idempotent)
void ReplicaImp::executeSpecialRequests(PrePrepareMsg *ppMsg,
                                        uint16_t numOfSpecialReqs,
                                        bool recoverFromErrorInRequestsExecution,
                                        Timestamp &outTimestamp) {
  IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  size_t reqIdx = 0;
  RequestsIterator reqIter(ppMsg);
  char *requestBody = nullptr;
  while (reqIter.getAndGoToNext(requestBody) && numOfSpecialReqs > 0) {
    ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);

    if ((req.flags() & MsgFlag::KEY_EXCHANGE_FLAG) || (req.flags() & MsgFlag::RECONFIG_FLAG) ||
        (req.flags() & MsgFlag::CLIENTS_PUB_KEYS_FLAG))  // TODO(GG): check how exactly the following will work
                                                         // when we try to recover
    {
      NodeIdType clientId = req.clientProxyId();

      accumulatedRequests.push_back(IRequestsHandler::ExecutionRequest{
          clientId,
          static_cast<uint64_t>(lastExecutedSeqNum + 1),
          ppMsg->getCid(),
          req.flags(),
          req.requestLength(),
          req.requestBuf(),
          std::string(req.requestSignature(), req.requestSignatureLength()),
          static_cast<uint32_t>(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)),
          static_cast<char *>(std::malloc(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader))),
          req.requestSeqNum(),
          req.requestIndexInBatch(),
          req.result()});

      numOfSpecialReqs--;
    }
    reqIdx++;
  }

  // TODO(GG): the following code is cumbersome. We can call to execute directly in the above loop
  IRequestsHandler::ExecutionRequestsQueue singleRequest;
  for (IRequestsHandler::ExecutionRequest &req : accumulatedRequests) {
    ConcordAssert(singleRequest.empty());
    singleRequest.push_back(req);
    {
      const concordUtils::SpanContext &span_context{""};
      auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
      span.setTag("rid", config_.getreplicaId());
      span.setTag("cid", req.cid);
      span.setTag("seq_num", req.requestSequenceNum);
      bftRequestsHandler_->execute(singleRequest, outTimestamp, ppMsg->getCid(), span);
      if (config_.timeServiceEnabled) outTimestamp.request_position++;
    }
    req = singleRequest.at(0);
    singleRequest.clear();
  }

  sendResponses(ppMsg, accumulatedRequests);
}

// TODO(LG) - make this method static
void ReplicaImp::executeRequests(PrePrepareMsg *ppMsg, Bitmap &requestSet, Timestamp time) {
  //   TimeRecorder scoped_timer(*histograms_.executeRequestsAndSendResponses);
  //  SCOPED_MDC("pp_msg_cid", ppMsg->getCid());
  auto pAccumulatedRequests =
      make_unique<IRequestsHandler::ExecutionRequestsQueue>();  // new IRequestsHandler::ExecutionRequestsQueue;
  size_t reqIdx = 0;
  RequestsIterator reqIter(ppMsg);
  char *requestBody = nullptr;
  while (reqIter.getAndGoToNext(requestBody)) {
    size_t tmp = reqIdx;
    reqIdx++;
    ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);

    if (!requestSet.get(tmp) || req.requestLength() == 0) {
      InternalMessage im = RemovePendingForExecutionRequest{req.clientProxyId(), req.requestSeqNum()};
      getIncomingMsgsStorage().pushInternalMsg(std::move(im));
      continue;
    }

    //    SCOPED_MDC_CID(req.getCid());
    NodeIdType clientId = req.clientProxyId();

    auto replyBuffer =
        static_cast<char *>(std::malloc(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)));
    uint32_t replySize = 0;
    if ((req.flags() & HAS_PRE_PROCESSED_FLAG) && (req.result() != static_cast<uint32_t>(OperationResult::UNKNOWN))) {
      replySize = req.requestLength();
      memcpy(replyBuffer, req.requestBuf(), req.requestLength());
    }

    pAccumulatedRequests->push_back(IRequestsHandler::ExecutionRequest{
        clientId,
        static_cast<uint64_t>(lastExecutedSeqNum + 1),
        req.getCid(),
        req.flags(),
        req.requestLength(),
        req.requestBuf(),
        std::string(req.requestSignature(), req.requestSignatureLength()),
        static_cast<uint32_t>(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)),
        replyBuffer,
        req.requestSeqNum(),
        req.requestIndexInBatch(),
        req.result(),
        replySize});

    if (req.flags() & HAS_PRE_PROCESSED_FLAG) {
      setConflictDetectionBlockId(req, pAccumulatedRequests->back());
    }
  }
  if (ReplicaConfig::instance().blockAccumulation) {
    LOG_DEBUG(GL,
              "Executing all the requests of preprepare message with cid: " << ppMsg->getCid() << " with accumulation");
    {
      //      TimeRecorder scoped_timer1(*histograms_.executeWriteRequest);
      const concordUtils::SpanContext &span_context{""};
      auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
      span.setTag("rid", config_.getreplicaId());
      span.setTag("cid", ppMsg->getCid());
      span.setTag("seq_num", ppMsg->seqNumber());
      if (isCurrentPrimary()) {
        metric_consensus_end_to_core_exe_duration_.finishMeasurement(ppMsg->seqNumber());
        metric_core_exe_func_duration_.addStartTimeStamp(ppMsg->seqNumber());
      }
      bftRequestsHandler_->execute(*pAccumulatedRequests, time, ppMsg->getCid(), span);
      if (isCurrentPrimary()) {
        metric_core_exe_func_duration_.finishMeasurement(ppMsg->seqNumber());
      }
    }
  } else {
    LOG_INFO(
        GL,
        "Executing all the requests of preprepare message with cid: " << ppMsg->getCid() << " without accumulation");
    IRequestsHandler::ExecutionRequestsQueue singleRequest;
    for (auto &req : *pAccumulatedRequests) {
      singleRequest.push_back(req);
      {
        //        TimeRecorder scoped_timer1(*histograms_.executeWriteRequest);
        const concordUtils::SpanContext &span_context{""};
        auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
        span.setTag("rid", config_.getreplicaId());
        span.setTag("cid", ppMsg->getCid());
        span.setTag("seq_num", ppMsg->seqNumber());
        bftRequestsHandler_->execute(singleRequest, time, ppMsg->getCid(), span);
        if (config_.timeServiceEnabled) {
          time.request_position++;
        }
      }
      req = singleRequest.at(0);
      singleRequest.clear();
    }
  }

  // send internal message that will call to finishExecutePrePrepareMsg(ppMsg);
  InternalMessage im =
      FinishPrePrepareExecutionInternalMsg{ppMsg, pAccumulatedRequests.release()};  // TODO(GG): check....
  getIncomingMsgsStorage().pushInternalMsg(std::move(im));
}

void ReplicaImp::finishExecutePrePrepareMsg(PrePrepareMsg *ppMsg,
                                            IRequestsHandler::ExecutionRequestsQueue *pAccumulatedRequests) {
  activeExecutions_ = 0;
  if (isCurrentPrimary()) {
    metric_post_exe_thread_idle_time_.addStartTimeStamp(0);
    metric_post_exe_thread_active_time_.finishMeasurement(0);
  }

  if (pAccumulatedRequests != nullptr) {
    sendResponses(ppMsg, *pAccumulatedRequests);
    delete pAccumulatedRequests;
  }
  LOG_INFO(CNSUS, "Finished execution of request seqNum:" << ppMsg->seqNumber());
  uint64_t checkpointNum{};
  if ((lastExecutedSeqNum + 1) % checkpointWindowSize == 0) {
    // Save the epoch to the reserved pages
    auto epochMgr = bftEngine::EpochManager::instance();
    auto epochNum = epochMgr.getSelfEpochNumber();
    epochMgr.setSelfEpochNumber(epochNum);
    epochMgr.setGlobalEpochNumber(epochNum);
    checkpointNum = (lastExecutedSeqNum + 1) / checkpointWindowSize;
    stateTransfer->createCheckpointOfCurrentState(
        checkpointNum);  // TODO(GG): should make sure that this operation is idempotent, even if it was partially
                         // executed (because of the recovery)
    checkpoint_times_.start(lastExecutedSeqNum);
  }

  finalizeExecution();

  if (ppMsg->numberOfRequests() > 0) bftRequestsHandler_->onFinishExecutingReadWriteRequests();

  if (ps_) ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());

  sendCheckpointIfNeeded();

  bool firstCommitPathChanged = controller->onNewSeqNumberExecution(lastExecutedSeqNum);

  if (firstCommitPathChanged) {
    metric_first_commit_path_.Get().Set(CommitPathToStr(controller->getCurrentFirstPath()));
  }

  updateLimitsAndMetrics(ppMsg);

  if (isCurrentPrimary()) {
    metric_post_exe_duration_.finishMeasurement(ppMsg->seqNumber());
  }

  tryToStartOrFinishExecution(false);
}

void ReplicaImp::finalizeExecution() {
  //////////////////////////////////////////////////////////////////////
  // Phase 3: finalize the execution of lastExecutedSeqNum+1
  // TODO(GG): Explain what happens in recovery mode
  //////////////////////////////////////////////////////////////////////

  LOG_DEBUG(CNSUS, "Finalized execution. " << KVLOG(lastExecutedSeqNum + 1, getCurrentView(), lastStableSeqNum));

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setLastExecutedSeqNum(lastExecutedSeqNum + 1);
  }

  lastExecutedSeqNum = lastExecutedSeqNum + 1;

  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum);
  }

  if (lastViewThatTransferredSeqNumbersFullyExecuted < getCurrentView() &&
      (lastExecutedSeqNum >= maxSeqNumTransferredFromPrevViews)) {
    // we store the old value of the seqNum column so we can return to it after logging the view number
    auto mdcSeqNum = MDC_GET(MDC_SEQ_NUM_KEY);
    MDC_PUT(MDC_SEQ_NUM_KEY, std::to_string(getCurrentView()));

    LOG_INFO(VC_LOG,
             "Rebuilding of previous View's Working Window complete. "
                 << KVLOG(getCurrentView(),
                          lastViewThatTransferredSeqNumbersFullyExecuted,
                          lastExecutedSeqNum,
                          maxSeqNumTransferredFromPrevViews));
    lastViewThatTransferredSeqNumbersFullyExecuted = getCurrentView();
    MDC_PUT(MDC_SEQ_NUM_KEY, mdcSeqNum);
    if (ps_) {
      ps_->setLastViewThatTransferredSeqNumbersFullyExecuted(lastViewThatTransferredSeqNumbersFullyExecuted);
    }
  }

  tryToMarkSeqNumAsStable();
}

void ReplicaImp::updateExecutedPathMetrics(const bool isSlow, uint16_t numOfRequests) {
  metric_total_finished_consensuses_++;
  auto &pathCounter = isSlow ? metric_total_slowPath_ : metric_total_fastPath_;
  auto &pathRequestCounter = isSlow ? metric_total_slowPath_requests_ : metric_total_fastPath_requests_;
  pathCounter++;
  pathRequestCounter += numOfRequests;
}

void ReplicaImp::updateLimitsAndMetrics(PrePrepareMsg *ppMsg) {
  // TODO(GG): clean the following logic
  if (mainLog->insideActiveWindow(lastExecutedSeqNum)) {  // update dynamicUpperLimitOfRounds
    SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum);
    const Time firstInfo = seqNumInfo.getTimeOfFirstRelevantInfoFromPrimary();
    const Time currTime = getMonotonicTime();
    if ((firstInfo < currTime)) {
      const int64_t durationMilli = duration_cast<milliseconds>(currTime - firstInfo).count();
      dynamicUpperLimitOfRounds->add(durationMilli);
    }
    auto numOfRequests = ppMsg->numberOfRequests();
    if (numOfRequests > 0) {
      consensus_time_.add(seqNumInfo.getCommitDurationMs());
      consensus_avg_time_.Get().Set((uint64_t)consensus_time_.avg());
      if (consensus_time_.numOfElements() == 1000) consensus_time_.reset();  // We reset the average every 1000 samples
      metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
    }
    updateExecutedPathMetrics(seqNumInfo.slowPathStarted(), numOfRequests);
  }

  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onRequestCompleted(false);
  }
}

void ReplicaImp::startCollectingState(std::string_view reason) {
  LOG_INFO(GL, "Start Collecting State" << KVLOG(reason));
  time_in_state_transfer_.start();
  clientsManager->clearAllPendingRequests();  // to avoid entering a new view on old request timeout
  stateTransfer->startCollectingState();
}

void ReplicaImp::handleDeferredRequests() {
  if (isStartCollectingState_) {
    if (!isCollectingState()) {
      startCollectingState("Handle Deferred Requests");
    } else {
      LOG_ERROR(GL, "Collecting state should be active while we are in onExecutionFinish");
    }
    isStartCollectingState_ = false;
  }
  if (!isCollectingState()) {
    if (isSendCheckpointIfNeeded_) {
      sendCheckpointIfNeeded();
      isSendCheckpointIfNeeded_ = false;
    }
    if (shouldGoToNextView_) {
      goToNextView();
      shouldGoToNextView_ = false;
      shouldTryToGoToNextView_ = false;
    }
    if (shouldTryToGoToNextView_ && !shouldGoToNextView_) {
      tryToGoToNextView();
      shouldTryToGoToNextView_ = false;
    }
    while (!deferredMessages_.empty()) {
      auto *msg = deferredMessages_.front();
      deferredMessages_.pop_front();
      deferredMessagesMetric_--;
      auto msgHandlerCallback = msgHandlers_->getCallback(msg->type());
      if (msgHandlerCallback) {
        msgHandlerCallback(std::unique_ptr<MessageBase>(msg));
      } else {
        delete msg;
      }
    }
    // Currently we are avoiding duplicates on deferred RO requests queue
    while (!deferredRORequests_.empty()) {
      auto *msg = deferredRORequests_.front();
      deferredRORequests_.pop_front();
      deferredRORequestsMetric_--;
      auto msgHandlerCallback = msgHandlers_->getCallback(msg->type());
      if (msgHandlerCallback) {
        msgHandlerCallback(std::unique_ptr<MessageBase>(msg));
      } else {
        delete msg;
      }
    }
  }
}

void ReplicaImp::onExecutionFinish() {
  handleDeferredRequests();
  auto seqNumToStopAt = ControlStateManager::instance().getCheckpointToStopAt();
  if (seqNumToStopAt.value_or(0) == lastExecutedSeqNum) ControlStateManager::instance().wedge();
  if (seqNumToStopAt.has_value() && seqNumToStopAt.value() > lastExecutedSeqNum && isCurrentPrimary()) {
    // If after execution, we discover that we need to wedge at some future point, push a noop command to the incoming
    // messages queue.
    LOG_INFO(GL, "sending noop command to bring the system into wedge checkpoint");
    concord::messages::ReconfigurationRequest req;
    req.sender = config_.replicaId;
    req.command = concord::messages::WedgeCommand{config_.replicaId, true};
    // Mark this request as an internal one
    std::vector<uint8_t> data_vec;
    concord::messages::serialize(data_vec, req);
    req.signature.resize(SigManager::instance()->getMySigLength());
    SigManager::instance()->sign(data_vec.data(), data_vec.size(), req.signature.data());
    data_vec.clear();
    concord::messages::serialize(data_vec, req);
    std::string strMsg(data_vec.begin(), data_vec.end());
    auto requestSeqNum =
        std::chrono::duration_cast<std::chrono::microseconds>(getMonotonicTime().time_since_epoch()).count();
    auto crm = new ClientRequestMsg(internalBFTClient_->getClientId(),
                                    RECONFIG_FLAG,
                                    requestSeqNum,
                                    strMsg.size(),
                                    strMsg.c_str(),
                                    60000,
                                    "wedge-noop-command-" + std::to_string(lastExecutedSeqNum + 1));
    // Now, try to send a new PrePrepare message immediately, without waiting to a new batch
    onMessage(std::unique_ptr<ClientRequestMsg>(crm));
    tryToSendPrePrepareMsg(false);
  }

  if (ControlStateManager::instance().getCheckpointToStopAt().has_value() &&
      lastExecutedSeqNum == ControlStateManager::instance().getCheckpointToStopAt()) {
    // We are about to stop execution. To avoid VC we now clear all pending requests
    clientsManager->clearAllPendingRequests();
  }

  // Sending noop commands to get the system to a stable checkpoint,
  // allowing the create dbCheckpoint operator command to create a dbCheckpoint/snapshot.
  if (getReplicaConfig().dbCheckpointFeatureEnabled && isCurrentPrimary()) {
    const auto &seq_num_to_create_dbcheckpoint = DbCheckpointManager::instance().getNextStableSeqNumToCreateSnapshot();
    if (seq_num_to_create_dbcheckpoint.has_value()) {
      if (seq_num_to_create_dbcheckpoint.value() > (lastExecutedSeqNum + activeExecutions_)) {
        DbCheckpointManager::instance().sendInternalCreateDbCheckpointMsg(lastExecutedSeqNum + activeExecutions_ + 1,
                                                                          true);  // noop=true
      } else {
        onSeqNumIsStableCallbacks_.add([seq_num_to_create_dbcheckpoint](SeqNum seqNum) {
          if (seqNum == seq_num_to_create_dbcheckpoint) {
            DbCheckpointManager::instance().sendInternalCreateDbCheckpointMsg(seqNum, false);  // noop=false
            LOG_INFO(GL,
                     "sendInternalCreateDbCheckpointMsg with noop: false, "
                         << KVLOG(seq_num_to_create_dbcheckpoint.value()));
          }
        });
        DbCheckpointManager::instance().setNextStableSeqNumToCreateSnapshot(std::nullopt);
      }
      LOG_INFO(GL,
               "sendInternalCreateDbCheckpointMsg: " << KVLOG(
                   lastExecutedSeqNum, activeExecutions_, seq_num_to_create_dbcheckpoint.value()));
    }
  }

  if (isCurrentPrimary() && requestsQueueOfPrimary.size() > 0) tryToSendPrePrepareMsg(true);
}

void ReplicaImp::executeRequestsInPrePrepareMsg(concordUtils::SpanWrapper &parent_span,
                                                PrePrepareMsg *ppMsg,
                                                bool recoverFromErrorInRequestsExecution) {
  TimeRecorder scoped_timer(*histograms_.executeRequestsInPrePrepareMsg);
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) {
    return;
  }
  auto span = concordUtils::startChildSpan("bft_execute_requests_in_preprepare", parent_span);
  if (!isCollectingState()) ConcordAssert(currentViewIsActive());

  ConcordAssertNE(ppMsg, nullptr);
  ConcordAssertEQ(ppMsg->viewNumber(), getCurrentView());
  ConcordAssertEQ(ppMsg->seqNumber(), lastExecutedSeqNum + 1);
  const uint16_t numOfRequests = ppMsg->numberOfRequests();

  // recoverFromErrorInRequestsExecution ==> (numOfRequests > 0)
  ConcordAssertOR(!recoverFromErrorInRequestsExecution, (numOfRequests > 0));

  if (numOfRequests > 0) {
    if (config_.timeServiceEnabled) {
      // First request should be a time request, if time-service is enabled
      ConcordAssert(ppMsg->getTime() > 0);
    }

    histograms_.numRequestsInPrePrepareMsg->record(numOfRequests);
    Bitmap requestSet(numOfRequests);
    size_t reqIdx = 0;
    RequestsIterator reqIter(ppMsg);
    char *requestBody = nullptr;

    //////////////////////////////////////////////////////////////////////
    // Phase 1:
    // a. Find the requests that should be executed
    // b. Send reply for each request that has already been executed
    //////////////////////////////////////////////////////////////////////
    if (!recoverFromErrorInRequestsExecution) {
      while (reqIter.getAndGoToNext(requestBody)) {
        ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader *>(requestBody));
        SCOPED_MDC_CID(req.getCid());
        NodeIdType clientId = req.clientProxyId();
        const bool validNoop = ((clientId == currentPrimary()) && (req.requestLength() == 0));
        if (validNoop) {
          ++numValidNoOps;
          reqIdx++;
          continue;
        }
        const bool validClient =
            isValidClient(clientId) ||
            ((req.flags() & RECONFIG_FLAG || req.flags() & INTERNAL_FLAG) && isIdOfReplica(clientId));
        if (!validClient) {
          ++numInvalidClients;
          LOG_WARN(CNSUS, "The client is not valid" << KVLOG(clientId));
          reqIdx++;
          continue;
        }
        if (isReplyAlreadySentToClient(clientId, req.requestSeqNum())) {
          auto replyMsg = clientsManager->allocateReplyFromSavedOne(clientId, req.requestSeqNum(), currentPrimary());
          if (replyMsg) {
            send(replyMsg.get(), clientId);
          }
          reqIdx++;
          continue;
        }
        requestSet.set(reqIdx++);
      }
      reqIter.restart();

      if (ps_) {
        auto ticks = config_.timeServiceEnabled ? time_service_manager_->getTime().count() : 0;
        DescriptorOfLastExecution execDesc{lastExecutedSeqNum + 1, requestSet, ticks};
        ps_->beginWriteTran();
        ps_->setDescriptorOfLastExecution(execDesc);
        ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
      }
    } else {
      requestSet = mapOfRecoveredRequests;
    }

    //////////////////////////////////////////////////////////////////////
    // Phase 2: execute requests + send replies
    // In this phase the application state may be changed. We also change data in the state transfer module.
    // TODO(GG): Explain what happens in recovery mode (what are the requirements from  the application, and from the
    // state transfer module.
    //////////////////////////////////////////////////////////////////////

    auto dur = controller->durationSincePrePrepare(lastExecutedSeqNum + 1);
    if (dur > 0) {
      // Primary
      LOG_INFO(CNSUS, "Consensus reached, sleep_duration_ms [" << dur << "ms]");

    } else {
      LOG_INFO(CNSUS, "Consensus reached");
    }
    executeRequestsAndSendResponses(ppMsg, requestSet, span);
  }
  uint64_t checkpointNum{};
  if ((lastExecutedSeqNum + 1) % checkpointWindowSize == 0) {
    // Save the epoch to the reserved pages
    auto epochMgr = bftEngine::EpochManager::instance();
    auto epochNum = epochMgr.getSelfEpochNumber();
    epochMgr.setSelfEpochNumber(epochNum);
    epochMgr.setGlobalEpochNumber(epochNum);
    checkpointNum = (lastExecutedSeqNum + 1) / checkpointWindowSize;
    stateTransfer->createCheckpointOfCurrentState(checkpointNum);
    checkpoint_times_.start(lastExecutedSeqNum);
  }

  //////////////////////////////////////////////////////////////////////
  // Phase 3: finalize the execution of lastExecutedSeqNum+1
  // TODO(GG): Explain what happens in recovery mode
  //////////////////////////////////////////////////////////////////////

  LOG_DEBUG(CNSUS, "Finalized execution. " << KVLOG(lastExecutedSeqNum + 1, getCurrentView(), lastStableSeqNum));

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setLastExecutedSeqNum(lastExecutedSeqNum + 1);
  }

  lastExecutedSeqNum = lastExecutedSeqNum + 1;

  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum);
  }
  if (lastViewThatTransferredSeqNumbersFullyExecuted < getCurrentView() &&
      (lastExecutedSeqNum >= maxSeqNumTransferredFromPrevViews)) {
    // we store the old value of the seqNum column so we can return to it after logging the view number
    auto mdcSeqNum = MDC_GET(MDC_SEQ_NUM_KEY);
    MDC_PUT(MDC_SEQ_NUM_KEY, std::to_string(getCurrentView()));

    LOG_INFO(VC_LOG,
             "Rebuilding of previous View's Working Window complete. "
                 << KVLOG(getCurrentView(),
                          lastViewThatTransferredSeqNumbersFullyExecuted,
                          lastExecutedSeqNum,
                          maxSeqNumTransferredFromPrevViews));
    lastViewThatTransferredSeqNumbersFullyExecuted = getCurrentView();
    MDC_PUT(MDC_SEQ_NUM_KEY, mdcSeqNum);
    if (ps_) {
      ps_->setLastViewThatTransferredSeqNumbersFullyExecuted(lastViewThatTransferredSeqNumbersFullyExecuted);
    }
  }

  tryToMarkSeqNumAsStable();

  if (numOfRequests > 0) bftRequestsHandler_->onFinishExecutingReadWriteRequests();

  if (ps_) ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());

  sendCheckpointIfNeeded();

  bool firstCommitPathChanged = controller->onNewSeqNumberExecution(lastExecutedSeqNum);

  if (firstCommitPathChanged) {
    metric_first_commit_path_.Get().Set(CommitPathToStr(controller->getCurrentFirstPath()));
  }
  // TODO(GG): clean the following logic
  if (mainLog->insideActiveWindow(lastExecutedSeqNum)) {  // update dynamicUpperLimitOfRounds
    const SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum);
    const Time firstInfo = seqNumInfo.getTimeOfFirstRelevantInfoFromPrimary();
    const Time currTime = getMonotonicTime();
    if ((firstInfo < currTime)) {
      const int64_t durationMilli = duration_cast<milliseconds>(currTime - firstInfo).count();
      dynamicUpperLimitOfRounds->add(durationMilli);
    }
  }

  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onRequestCompleted(false);
  }
}

void ReplicaImp::tryToMarkSeqNumAsStable() {
  if (lastExecutedSeqNum % checkpointWindowSize != 0) return;

  Digest stateDigest, reservedPagesDigest, rvbDataDigest;
  CheckpointMsg::State state;
  const uint64_t checkpointNum = lastExecutedSeqNum / checkpointWindowSize;
  stateTransfer->getDigestOfCheckpoint(checkpointNum,
                                       sizeof(Digest),
                                       state,
                                       stateDigest.content(),
                                       reservedPagesDigest.content(),
                                       rvbDataDigest.content());
  CheckpointMsg *checkMsg = new CheckpointMsg(
      config_.getreplicaId(), lastExecutedSeqNum, state, stateDigest, reservedPagesDigest, rvbDataDigest, false);
  checkMsg->sign();
  auto &checkInfo = checkpointsLog->get(lastExecutedSeqNum);
  checkInfo.addCheckpointMsg(checkMsg, config_.getreplicaId());

  if (ps_) ps_->setCheckpointMsgInCheckWindow(lastExecutedSeqNum, checkMsg);

  if (checkInfo.isCheckpointCertificateComplete()) {
    onSeqNumIsStable(lastExecutedSeqNum);
  }
  checkInfo.setSelfExecutionTime(getMonotonicTime());
  if (checkInfo.isCheckpointSuperStable()) {
    onSeqNumIsSuperStable(lastStableSeqNum);
  }

  CryptoManager::instance().onCheckpoint(checkpointNum);
}

void ReplicaImp::executeRequestsAndSendResponses(PrePrepareMsg *ppMsg,
                                                 Bitmap &requestSet,
                                                 concordUtils::SpanWrapper &span) {
  TimeRecorder scoped_timer(*histograms_.executeRequestsAndSendResponses);
  SCOPED_MDC("pp_msg_cid", ppMsg->getCid());
  IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  size_t reqIdx = 0;
  RequestsIterator reqIter(ppMsg);
  char *requestBody = nullptr;
  auto timestamp = config_.timeServiceEnabled ? std::make_optional<Timestamp>() : std::nullopt;
  if (config_.timeServiceEnabled) {
    ConcordAssert(ppMsg->getTime() > 0);
    timestamp->time_since_epoch = ConsensusTime(ppMsg->getTime());
    timestamp->time_since_epoch = time_service_manager_->compareAndUpdate(timestamp->time_since_epoch);
    LOG_DEBUG(GL, "Timestamp to be provided to the execution: " << timestamp->time_since_epoch.count() << "ms");
  }
  while (reqIter.getAndGoToNext(requestBody)) {
    size_t tmp = reqIdx;
    reqIdx++;
    ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader *>(requestBody));
    if (!requestSet.get(tmp) || req.requestLength() == 0) {
      clientsManager->removePendingForExecutionRequest(req.clientProxyId(), req.requestSeqNum());
      continue;
    }
    SCOPED_MDC_CID(req.getCid());
    NodeIdType clientId = req.clientProxyId();

    auto replyBuffer =
        static_cast<char *>(std::malloc(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)));
    uint32_t replySize = 0;
    if ((req.flags() & HAS_PRE_PROCESSED_FLAG) && (req.result() != static_cast<uint32_t>(OperationResult::UNKNOWN))) {
      replySize = req.requestLength();
      memcpy(replyBuffer, req.requestBuf(), req.requestLength());
    }

    accumulatedRequests.push_back(IRequestsHandler::ExecutionRequest{
        clientId,
        static_cast<uint64_t>(lastExecutedSeqNum + 1),
        ppMsg->getCid(),
        req.flags(),
        req.requestLength(),
        req.requestBuf(),
        std::string(req.requestSignature(), req.requestSignatureLength()),
        static_cast<uint32_t>(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)),
        replyBuffer,
        req.requestSeqNum(),
        req.requestIndexInBatch(),
        req.result(),
        replySize});
    // Decode the pre-execution block-id for the conflict detection optimization,
    // and pass it to the post-execution.
    if (req.flags() & HAS_PRE_PROCESSED_FLAG) {
      setConflictDetectionBlockId(req, accumulatedRequests.back());
    }
  }
  if (ReplicaConfig::instance().blockAccumulation) {
    LOG_INFO(GL,
             "Executing all the requests of preprepare message with cid: " << ppMsg->getCid() << " with accumulation");
    {
      TimeRecorder scoped_timer1(*histograms_.executeWriteRequest);
      bftRequestsHandler_->execute(accumulatedRequests, timestamp, ppMsg->getCid(), span);
    }
  } else {
    LOG_INFO(
        GL,
        "Executing all the requests of preprepare message with cid: " << ppMsg->getCid() << " without accumulation");
    IRequestsHandler::ExecutionRequestsQueue singleRequest;
    for (auto &req : accumulatedRequests) {
      singleRequest.push_back(req);
      {
        TimeRecorder scoped_timer1(*histograms_.executeWriteRequest);
        bftRequestsHandler_->execute(singleRequest, timestamp, ppMsg->getCid(), span);
        if (config_.timeServiceEnabled) {
          timestamp->request_position++;
        }
      }
      req = singleRequest.at(0);
      singleRequest.clear();
    }
  }
  sendResponses(ppMsg, accumulatedRequests);
}

void ReplicaImp::sendResponses(PrePrepareMsg *ppMsg, IRequestsHandler::ExecutionRequestsQueue &accumulatedRequests) {
  TimeRecorder scoped_timer(*histograms_.prepareAndSendResponses);
  for (auto &req : accumulatedRequests) {
    auto executionResult = req.outExecutionStatus;
    std::unique_ptr<ClientReplyMsg> replyMsg;

    // Internal clients don't expect to be answered
    if (repsInfo->isIdOfInternalClient(req.clientId)) {
      clientsManager->removePendingForExecutionRequest(req.clientId, req.requestSequenceNum);
      free(req.outReply);
      continue;
    }
    if (executionResult != 0) {
      LOG_WARN(
          GL,
          "Request execution failed: " << KVLOG(
              req.clientId, req.requestSequenceNum, ppMsg->getCid(), req.outExecutionStatus, req.outActualReplySize));
    } else {
      if (req.flags & HAS_PRE_PROCESSED_FLAG) metric_total_preexec_requests_executed_++;
      if (req.outActualReplySize != 0) {
        replyMsg = clientsManager->allocateNewReplyMsgAndWriteToStorage(req.clientId,
                                                                        req.requestSequenceNum,
                                                                        currentPrimary(),
                                                                        req.outReply,
                                                                        req.outActualReplySize,
                                                                        req.reqIndexInClientBatch,
                                                                        req.outReplicaSpecificInfoSize,
                                                                        executionResult);
        if (replyMsg) {
          send(replyMsg.get(), req.clientId);
          free(req.outReply);
        } else {
          LOG_DEBUG(GL,
                    "Failed to allocate and save new reply to storage"
                        << KVLOG(req.clientId, req.requestSequenceNum, req.reqIndexInClientBatch));
        }
        req.outReply = nullptr;
        clientsManager->removePendingForExecutionRequest(req.clientId, req.requestSequenceNum);
        continue;
      } else {
        LOG_WARN(CNSUS, "Received zero size response." << KVLOG(req.clientId, req.requestSequenceNum, ppMsg->getCid()));
        strcpy(req.outReply, "Executed data is empty");
        req.outActualReplySize = strlen(req.outReply);
        executionResult = static_cast<uint32_t>(bftEngine::OperationResult::EXEC_DATA_EMPTY);
      }
    }

    replyMsg = clientsManager->allocateNewReplyMsgAndWriteToStorage(req.clientId,
                                                                    req.requestSequenceNum,
                                                                    currentPrimary(),
                                                                    req.outReply,
                                                                    req.outActualReplySize,
                                                                    req.reqIndexInClientBatch,
                                                                    0,
                                                                    executionResult);
    if (replyMsg) {
      send(replyMsg.get(), req.clientId);
      free(req.outReply);
    } else {
      LOG_DEBUG(GL,
                "Failed to allocate and save new reply to storage"
                    << KVLOG(req.clientId, req.requestSequenceNum, req.reqIndexInClientBatch));
    }
    req.outReply = nullptr;
    clientsManager->removePendingForExecutionRequest(req.clientId, req.requestSequenceNum);
  }
}

void ReplicaImp::tryToRemovePendingRequestsForSeqNum(SeqNum seqNum) {
  if (lastExecutedSeqNum >= seqNum) return;
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNum));
  SeqNumInfo &seqNumInfo = mainLog->get(seqNum);
  PrePrepareMsg *prePrepareMsg = seqNumInfo.getPrePrepareMsg();
  if (prePrepareMsg == nullptr) return;
  LOG_INFO(GL, "clear pending requests" << KVLOG(seqNum));

  RequestsIterator reqIter(prePrepareMsg);
  char *requestBody = nullptr;
  while (reqIter.getAndGoToNext(requestBody)) {
    ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);
    if (!clientsManager->isValidClient(req.clientProxyId())) continue;
    auto clientId = req.clientProxyId();
    LOG_DEBUG(GL, "removing pending requests for client" << KVLOG(clientId));
    clientsManager->markRequestAsCommitted(clientId, req.requestSeqNum());
  }
}

void ReplicaImp::executeNextCommittedRequests(concordUtils::SpanWrapper &parent_span, bool requestMissingInfo) {
  if (!isCollectingState()) ConcordAssert(currentViewIsActive());
  ConcordAssertGE(lastExecutedSeqNum, lastStableSeqNum);
  auto span = concordUtils::startChildSpan("bft_execute_next_committed_requests", parent_span);

  while (lastExecutedSeqNum < lastStableSeqNum + kWorkWindowSize) {
    SeqNum nextExecutedSeqNum = lastExecutedSeqNum + 1;
    SCOPED_MDC_SEQ_NUM(std::to_string(nextExecutedSeqNum));
    if (ControlStateManager::instance().isWedged()) {
      LOG_INFO(CNSUS, "system is wedged, no new prePrepare requests will be executed until its unwedged");
      break;
    }
    SeqNumInfo &seqNumInfo = mainLog->get(nextExecutedSeqNum);

    PrePrepareMsg *prePrepareMsg = seqNumInfo.getPrePrepareMsg();

    const bool ready = (prePrepareMsg != nullptr) && (seqNumInfo.isCommitted__gg());

    if (requestMissingInfo && !ready) {
      LOG_INFO(GL, "Asking for missing information: " << KVLOG(nextExecutedSeqNum, getCurrentView(), lastStableSeqNum));
      tryToSendReqMissingDataMsg(nextExecutedSeqNum);
    }

    if (!ready) break;

    ConcordAssertEQ(prePrepareMsg->seqNumber(), nextExecutedSeqNum);
    ConcordAssertEQ(prePrepareMsg->viewNumber(), getCurrentView());  // TODO(GG): TBD
    const uint16_t numOfRequests = prePrepareMsg->numberOfRequests();

    executeRequestsInPrePrepareMsg(span, prePrepareMsg);
    consensus_time_.add(seqNumInfo.getCommitDurationMs());
    consensus_avg_time_.Get().Set((uint64_t)consensus_time_.avg());
    if (consensus_time_.numOfElements() == 1000) consensus_time_.reset();  // We reset the average every 1000 samples
    metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
    updateExecutedPathMetrics(seqNumInfo.slowPathStarted(), numOfRequests);

    auto seqNumToStopAt = ControlStateManager::instance().getCheckpointToStopAt();
    if (seqNumToStopAt.value_or(0) == lastExecutedSeqNum) ControlStateManager::instance().wedge();
    if (seqNumToStopAt.has_value() && seqNumToStopAt.value() > lastExecutedSeqNum && isCurrentPrimary()) {
      // If after execution, we discover that we need to wedge at some futuer point, push a noop command to the incoming
      // messages queue.
      LOG_INFO(GL, "sending noop command to bring the system into wedge checkpoint");
      concord::messages::ReconfigurationRequest req;
      req.sender = config_.replicaId;
      req.command = concord::messages::WedgeCommand{config_.replicaId, true};
      // Mark this request as an internal one
      std::vector<uint8_t> data_vec;
      concord::messages::serialize(data_vec, req);
      std::string sig(SigManager::instance()->getMySigLength(), '\0');
      SigManager::instance()->sign(reinterpret_cast<char *>(data_vec.data()), data_vec.size(), sig.data());
      req.signature = std::vector<uint8_t>(sig.begin(), sig.end());
      data_vec.clear();
      concord::messages::serialize(data_vec, req);
      std::string strMsg(data_vec.begin(), data_vec.end());
      auto requestSeqNum =
          std::chrono::duration_cast<std::chrono::microseconds>(getMonotonicTime().time_since_epoch()).count();
      auto crm = new ClientRequestMsg(internalBFTClient_->getClientId(),
                                      RECONFIG_FLAG,
                                      requestSeqNum,
                                      strMsg.size(),
                                      strMsg.c_str(),
                                      60000,
                                      "wedge-noop-command-" + std::to_string(lastExecutedSeqNum + 1));
      // Now, try to send a new PrePrepare message immediately, without waiting to a new batch
      onMessage(std::unique_ptr<ClientRequestMsg>(crm));
      tryToSendPrePrepareMsg(false);
    }
  }
  auto seqNumToStopAt = ControlStateManager::instance().getCheckpointToStopAt();
  if (seqNumToStopAt.has_value() && seqNumToStopAt.value() > lastExecutedSeqNum && isCurrentPrimary()) {
    // If after execution, we discover that we need to wedge at some future point, push a noop command to the incoming
    // messages queue.
    LOG_INFO(GL, "sending noop command to bring the system into wedge checkpoint");
    concord::messages::ReconfigurationRequest req;
    req.sender = config_.replicaId;
    req.command = concord::messages::WedgeCommand{config_.replicaId, true};
    // Mark this request as an internal one
    std::vector<uint8_t> data_vec;
    concord::messages::serialize(data_vec, req);
    std::string sig(SigManager::instance()->getMySigLength(), '\0');
    SigManager::instance()->sign(reinterpret_cast<char *>(data_vec.data()), data_vec.size(), sig.data());
    req.signature = std::vector<uint8_t>(sig.begin(), sig.end());
    data_vec.clear();
    concord::messages::serialize(data_vec, req);
    std::string strMsg(data_vec.begin(), data_vec.end());
    auto current_time =
        std::chrono::duration_cast<std::chrono::microseconds>(getMonotonicTime().time_since_epoch()).count();
    auto crm = new ClientRequestMsg(internalBFTClient_->getClientId(),
                                    RECONFIG_FLAG,
                                    current_time,
                                    strMsg.size(),
                                    strMsg.c_str(),
                                    60000,
                                    "wedge-noop-command-" + std::to_string(lastExecutedSeqNum));
    // Now, try to send a new PrePrepare message immediately, without waiting to a new batch
    onMessage(std::unique_ptr<ClientRequestMsg>(crm));
    tryToSendPrePrepareMsg(false);
  }

  if (ControlStateManager::instance().getCheckpointToStopAt().has_value() &&
      lastExecutedSeqNum == ControlStateManager::instance().getCheckpointToStopAt()) {
    // We are about to stop execution. To avoid VC we now clear all pending requests
    clientsManager->clearAllPendingRequests();
  }
  if (isCurrentPrimary() && requestsQueueOfPrimary.size() > 0) tryToSendPrePrepareMsg(true);
}

void ReplicaImp::tryToGoToNextView() {
  if (viewsManager->hasQuorumToLeaveView()) {
    goToNextView();
  } else {
    LOG_INFO(VC_LOG, "Insufficient quorum for moving to next view " << KVLOG(getCurrentView()));
  }
}

IncomingMsgsStorage &ReplicaImp::getIncomingMsgsStorage() { return *msgsCommunicator_->getIncomingMsgsStorage(); }

void ReplicaImp::startSlowPath(SeqNumInfo &seqNumInfo) {
  seqNumInfo.startSlowPath();
  metric_total_slowPath_++;
}
void ReplicaImp::updateCommitMetrics(const CommitPath &commitPath) {
  metric_total_committed_sn_++;
  switch (commitPath) {
    case CommitPath::FAST_WITH_THRESHOLD:
      metric_committed_fast_threshold_++;
      break;
    case CommitPath::OPTIMISTIC_FAST:
      metric_committed_fast_++;
      break;
    case CommitPath::SLOW:
      metric_committed_slow_++;
      break;
    default:
      LOG_ERROR(CNSUS, "Invalid commit path value: " << static_cast<int8_t>(commitPath));
  }
}

// TODO(GG): the timer for state transfer !!!!

// TODO(GG): !!!! view changes and retransmissionsLogic --- check ....

}  // namespace bftEngine::impl
