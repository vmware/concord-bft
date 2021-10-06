// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <cmath>
#include <stdio.h>
#include "UtilCalc.hpp"

namespace concordUtils {

UtilCalc::UtilCalc()
    : metricsComponent_{concordMetrics::Component("util_calculator", std::make_shared<concordMetrics::Aggregator>())},
      average_util_gauge_{metricsComponent_.RegisterGauge("average_util_gauge", 0)} {
  activeMilliSeconds_ = 0;
  lastSecond_ = 0;
  startMilli_ = 0;
  aggMilliSeconds_ = 0;
  secondCount_ = 0;
  metricsComponent_.Register();
}

void UtilCalc::Start(bftEngine::impl::MsgCode::Type type) {
  uint64_t nowMilli = getMonotonicTimeMilli();
  uint64_t nowSecond = nowMilli / 1000;
  if (nowSecond > lastSecond_ && activeMilliSeconds_ > 0) {
    Add(activeMilliSeconds_);
    activeMilliSeconds_ = 0;
  }
  current_type_ = type;
  lastSecond_ = nowSecond;
  startMilli_ = nowMilli;
}

void UtilCalc::End() {
  uint64_t nowMilli = getMonotonicTimeMilli();
  uint64_t nowSecond = nowMilli / 1000;
  if (nowSecond < lastSecond_) {
    LOG_WARN(logger_, "The clock is not monotonic, can't measure");
  } else if (nowSecond == lastSecond_) {
    if (nowMilli - startMilli_ > 0) activeMilliSeconds_ += (nowMilli - startMilli_);
  } else if (nowSecond > lastSecond_) {
    activeMilliSeconds_ += (1000 - (startMilli_ % 1000));
    Add(activeMilliSeconds_);
    if (nowSecond - lastSecond_ > 10) {
      LOG_WARN(logger_, "Measured a gap of=10 seconds");
      lastSecond_ = nowSecond - 10;
    }
    for (uint64_t i = lastSecond_ + 1; i <= nowSecond - 1; i++) Add(1000);
    activeMilliSeconds_ = (nowMilli % 1000);
    lastSecond_ = nowSecond;
  }
}

void UtilCalc::Add(uint64_t ms) {
  histograms_.mainThread->record(ms);
  UpdateHistogram(ms);
  aggMilliSeconds_ += ms;
  secondCount_++;
  average_util_.add(ms);
  average_util_gauge_.Get().Set((uint64_t)average_util_.avg());
  if (average_util_.numOfElements() == 1000) average_util_.reset();  // reset the average every 1000 samples
}

void UtilCalc::UpdateHistogram(uint64_t ms) {
  switch (current_type_) {
    case bftEngine::impl::MsgCode::ClientRequest:
      histograms_.ClientRequestMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::PrePrepare:
      histograms_.PrePrepareMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::PreparePartial:
      histograms_.PreparePartialMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::PrepareFull:
      histograms_.PrepareFullMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::CommitPartial:
      histograms_.CommitPartialMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::CommitFull:
      histograms_.CommitFullMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::StartSlowCommit:
      histograms_.StartSlowCommitMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::PartialCommitProof:
      histograms_.PartialCommitProofMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::FullCommitProof:
      histograms_.FullCommitProofMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::SimpleAck:
      histograms_.SimpleAckMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::ViewChange:
      histograms_.ViewChangeMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::NewView:
      histograms_.NewViewMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::Checkpoint:
      histograms_.CheckpointMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::AskForCheckpoint:
      histograms_.AskForCheckpointMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::ReplicaStatus:
      histograms_.ReplicaStatusMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::ReqMissingData:
      histograms_.ReqMissingDataMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::ReplicaAsksToLeaveView:
      histograms_.ReplicaAsksToLeaveViewMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::ReplicaRestartReady:
      histograms_.ReplicaRestartReadyMsg->record(ms);
      break;
    case bftEngine::impl::MsgCode::ReplicasRestartReadyProof:
      histograms_.ReplicasRestartReadyProofMsg->record(ms);
      break;
    default:
      LOG_DEBUG(logger_, "Message without histogram");
  }
}

std::string UtilCalc::ToString() const {
  std::string r = "";
  r += "Measured " + std::to_string(secondCount_) + " seconds\n";
  uint64_t aggMsPercent = aggMilliSeconds_ / (secondCount_ * 1000);
  r += "The main thread was occupied " + std::to_string(aggMsPercent * 100) + " of the time";
  return r;
}

uint64_t UtilCalc::getMonotonicTimeMilli() {
  steady_clock::time_point curTimePoint = steady_clock::now();
  return duration_cast<milliseconds>(curTimePoint.time_since_epoch()).count();
}

}  // namespace concordUtils
