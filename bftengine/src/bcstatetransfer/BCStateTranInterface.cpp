// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "assertUtils.hpp"
#include "BCStateTran.hpp"

using namespace std::placeholders;
using namespace std::chrono;

// uncomment to add debug prints
// #define BCSTATETRAN_INTERFACE_DO_DEBUG
#undef DEBUG_PRINT
#ifdef BCSTATETRAN_INTERFACE_DO_DEBUG
#define MARK_START_TIME auto start = std::chrono::steady_clock::now()
#define CALC_DURATION auto jobDuration = calcDuration(start)
#define DEBUG_PRINT(x, y) LOG_INFO(x, y)
#else
#define DEBUG_PRINT(x, y)
#define MARK_START_TIME
#define CALC_DURATION
#endif

namespace bftEngine::bcst::impl {

[[maybe_unused]] static inline uint64_t calcDuration(time_point<steady_clock> startTime) {
  return duration_cast<std::chrono::microseconds>(steady_clock::now() - startTime).count();
}

// This function turns clang-format off, do not remove the related comments
// clang-format off
void BCStateTran::bindInterfaceHandlers() {
  handleStateTransferMessageHandler_ = (config_.runInSeparateThread) ?
    // Async (non-blocking) call
    static_cast<decltype(handleStateTransferMessageHandler_)>([this]
      (char *msg, uint32_t msgLen, uint16_t senderId, LocalTimePoint incomingEventsQPushTime) {
        incomingEventsQ_->push(std::bind(&BCStateTran::handleStateTransferMessageImpl, this, msg, msgLen, senderId,
          incomingEventsQPushTime), false); }) :
    std::bind(&BCStateTran::handleStateTransferMessageImpl, this, _1, _2, _3, _4);

  handleIncomingConsensusMessageHandler_ = (config_.runInSeparateThread) ?
    // Async (non-blocking) call
    static_cast<decltype(handleIncomingConsensusMessageHandler_)> ([this](ConsensusMsg msg) {
        incomingEventsQ_->push(std::bind(&BCStateTran::handleIncomingConsensusMessageImpl, this, msg), false); }) :
    std::bind(&BCStateTran::handleIncomingConsensusMessageImpl, this, _1);

  onTimerHandler_ = (config_.runInSeparateThread) ?
    // Async (non-blocking) call
    static_cast<decltype(onTimerHandler_)>([this]() {
      incomingEventsQ_->push(std::bind(&BCStateTran::onTimerImpl, this), false);
      }) : std::bind(&BCStateTran::onTimerImpl, this);

  startCollectingStateHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(startCollectingStateHandler_)>([this]() {
      DEBUG_PRINT(logger_, "Before startCollectingStateImpl");
      MARK_START_TIME;
      incomingEventsQ_->push(std::bind(&BCStateTran::startCollectingStateImpl, this));
      CALC_DURATION;
      DEBUG_PRINT(logger_, "After startCollectingStateImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::startCollectingStateImpl, this);

  initHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(initHandler_)>([this](uint64_t maxNumOfRequiredStoredCheckpoints,
              uint32_t numberOfRequiredReservedPages, uint32_t sizeOfReservedPage) {
                DEBUG_PRINT(logger_, "Before initImpl");
                MARK_START_TIME;
                incomingEventsQ_->push(std::bind(&BCStateTran::initImpl, this, maxNumOfRequiredStoredCheckpoints,
                  numberOfRequiredReservedPages, sizeOfReservedPage));
                CALC_DURATION;
                DEBUG_PRINT(logger_, "After initImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::initImpl, this, _1, _2, _3);

  startRunningHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(startRunningHandler_)>(
      [this](IReplicaForStateTransfer* r) {
        DEBUG_PRINT(logger_, "Before startRunningImpl");
        MARK_START_TIME;
        incomingEventsQ_->push(std::bind(&BCStateTran::startRunningImpl, this, r));
        CALC_DURATION;
        DEBUG_PRINT(logger_, "After startRunningImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::startRunningImpl, this, _1);

  stopRunningHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(stopRunningHandler_)>([this]() {
      DEBUG_PRINT(logger_, "Before stopRunningImpl");
      MARK_START_TIME;
      incomingEventsQ_->push(std::bind(&BCStateTran::stopRunningImpl, this));
      CALC_DURATION;
      DEBUG_PRINT(logger_, "After stopRunningImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::stopRunningImpl, this);

  createCheckpointOfCurrentStateHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(createCheckpointOfCurrentStateHandler_)>([this](uint64_t checkpointNumber) {
      DEBUG_PRINT(logger_, "Before createCheckpointOfCurrentStateImpl");
      MARK_START_TIME;
      incomingEventsQ_->push(std::bind(&BCStateTran::createCheckpointOfCurrentStateImpl, this, checkpointNumber));
      CALC_DURATION;
      DEBUG_PRINT(logger_, "After createCheckpointOfCurrentStateImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::createCheckpointOfCurrentStateImpl, this, _1);

  getDigestOfCheckpointHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(getDigestOfCheckpointHandler_)>([this](uint64_t checkpointNumber, uint16_t sizeOfDigestBuffer,
        uint64_t& outBlockId, char* outStateDigest, char* outResPagesDigest, char* outRVBDataDigest) {
      DEBUG_PRINT(logger_, "Before getDigestOfCheckpointImpl");
      MARK_START_TIME;
      incomingEventsQ_->push(std::bind(
        &BCStateTran::getDigestOfCheckpointImpl, this, checkpointNumber,sizeOfDigestBuffer,
        std::ref(outBlockId), outStateDigest, outResPagesDigest, outRVBDataDigest));
        CALC_DURATION;
      DEBUG_PRINT(logger_, "After getDigestOfCheckpointImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::getDigestOfCheckpointImpl, this, _1, _2, _3 , _4, _5, _6);

  addOnFetchingStateChangeCallbackHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(addOnFetchingStateChangeCallbackHandler_)>([this](const std::function<void(uint64_t)> &cb) {
      DEBUG_PRINT(logger_, "Before addOnFetchingStateChangeCallbackImpl");
      MARK_START_TIME;
      incomingEventsQ_->push(std::bind(&BCStateTran::addOnFetchingStateChangeCallbackImpl, this, cb));
      CALC_DURATION;
      DEBUG_PRINT(logger_, "After addOnFetchingStateChangeCallbackImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::addOnFetchingStateChangeCallbackImpl, this, _1);

  getStatusHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(getStatusHandler_)>([this](std::string& outStatus) {
      DEBUG_PRINT(logger_, "Before getStatusImpl");
      MARK_START_TIME;
      incomingEventsQ_->push(std::bind(&BCStateTran::getStatusImpl, this, std::ref(outStatus)));
      CALC_DURATION;
      DEBUG_PRINT(logger_, "After getStatusImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::getStatusImpl, this, _1);

  addOnTransferringCompleteCallbackHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(addOnTransferringCompleteCallbackHandler_)>([this](
      const std::function<void(uint64_t)>& cb, StateTransferCallBacksPriorities priority) {
        DEBUG_PRINT(logger_, "Before addOnTransferringCompleteCallbackImpl");
        MARK_START_TIME;
        incomingEventsQ_->push(std::bind(
          &BCStateTran::addOnTransferringCompleteCallbackImpl, this, cb, priority));
          CALC_DURATION;
        DEBUG_PRINT(logger_, "After addOnTransferringCompleteCallbackImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::addOnTransferringCompleteCallbackImpl, this, _1, _2);

  setEraseMetadataFlagHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(setEraseMetadataFlagHandler_)>([this]() {
      DEBUG_PRINT(logger_, "Before setEraseMetadataFlagImpl");
      MARK_START_TIME;
      incomingEventsQ_->push(std::bind(&BCStateTran::setEraseMetadataFlagImpl, this));
      CALC_DURATION;
      DEBUG_PRINT(logger_, "After setEraseMetadataFlagImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::setEraseMetadataFlagImpl, this);

  setReconfigurationEngineHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(setReconfigurationEngineHandler_)>(
      [this](std::shared_ptr<ClientReconfigurationEngine> cre) {
        DEBUG_PRINT(logger_, "Before setReconfigurationEngineImpl");
        MARK_START_TIME;
        incomingEventsQ_->push(std::bind(&BCStateTran::setReconfigurationEngineImpl, this, cre));
        CALC_DURATION;
        DEBUG_PRINT(logger_, "After setReconfigurationEngineImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::setReconfigurationEngineImpl, this, _1);

  reportLastAgreedPrunableBlockIdHandler_ = (config_.runInSeparateThread) ?
    static_cast<decltype(reportLastAgreedPrunableBlockIdHandler_)>(
      [this](uint64_t lastAgreedPrunableBlockId) {
        DEBUG_PRINT(logger_, "Before reportLastAgreedPrunableBlockIdImpl");
        MARK_START_TIME;
        incomingEventsQ_->push(
          std::bind(&BCStateTran::reportLastAgreedPrunableBlockIdImpl, this, lastAgreedPrunableBlockId));
        CALC_DURATION;
        DEBUG_PRINT(logger_, "After reportLastAgreedPrunableBlockIdImpl" << KVLOG(jobDuration)); }) :
    std::bind(&BCStateTran::reportLastAgreedPrunableBlockIdImpl, this, _1);
}
// clang-format on

std::string BCStateTran::getStatus() {
  std::string status;
  getStatusHandler_(std::ref(status));
  return status;
}

void BCStateTran::stopRunning() {
  stopRunningHandler_();
  if (incomingEventsQ_) {
    incomingEventsQ_->stop();
  }
}
}  // namespace bftEngine::bcst::impl