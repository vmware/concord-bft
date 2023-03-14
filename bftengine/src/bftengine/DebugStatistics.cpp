// Concord
//
// Copyright (c) 2018, 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "DebugStatistics.hpp"

#include <iomanip>
#include <inttypes.h>
#include <chrono>

#include "log/logger.hpp"
#include "messages/MsgCode.hpp"

using namespace std;
using namespace std::chrono;

namespace bftEngine {
namespace impl {

void DebugStatistics::onCycleCheck() {
  DebugStatDesc& d = getDebugStatDesc();

  if (!d.initialized)  // if this is the first time
  {
    d.initialized = true;
    d.lastCycleTime = getMonotonicTime();
    clearCounters(d);
    return;
  }

  Time currTime = getMonotonicTime();

  auto durationSec = duration_cast<seconds>(currTime - d.lastCycleTime);

  if (durationSec < seconds(DEBUG_STAT_PERIOD_SECONDS)) return;

  long double readThroughput = 0;
  long double writeThroughput = 0;

  if (d.completedReadOnlyRequests > 0) readThroughput = (double(d.completedReadOnlyRequests) / durationSec.count());

  if (d.completedReadWriteRequests > 0) writeThroughput = (double(d.completedReadWriteRequests) / durationSec.count());

  double avgBatchSize = 0;
  double avgPendingRequests = 0;

  if (d.prePrepareMessages > 0) {
    avgBatchSize = (double)d.batchRequests / d.prePrepareMessages;
    avgPendingRequests = (double)d.pendingRequests / d.prePrepareMessages;
  }

  // We use INFO logging instead of DEBUG logging since there is already a
  // separate switch to turn DebugStatistics on and off. It's likely we want
  // this data to appear when we do not want full debug logging.
  LOG_INFO(GL,
           " STAT:" << endl
                    << "    ReadOnlyThroughput = " << fixed << setprecision(2) << readThroughput << endl
                    << "    WriteThroughput = " << fixed << setprecision(2) << writeThroughput << endl
                    << "    ReceivedMessages = " << d.receivedMessages << endl
                    << "    SentMessages = " << d.sendMessages << endl
                    << "    NumberOfReceivedSTMessages = " << d.numberOfReceivedSTMessages << endl
                    << "    NumberOfReceivedStatusMessages = " << d.numberOfReceivedStatusMessages << endl
                    << "    NumberOfReceivedCommitMessages = " << d.numberOfReceivedCommitMessages << endl
                    << "    LastExecutedSeqNumber = " << d.lastExecutedSequenceNumber << endl
                    << "    PrePrepareMessages = " << d.prePrepareMessages << endl
                    << "    AvgBatchSize = " << fixed << setprecision(2) << avgBatchSize << endl
                    << "    AvgPendingRequest = " << fixed << setprecision(2) << avgPendingRequests << endl);

  d.lastCycleTime = currTime;
  clearCounters(d);
}

void DebugStatistics::onReceivedExMessage(uint16_t type) {
  DebugStatDesc& d = getDebugStatDesc();

  d.receivedMessages++;

  switch (type) {
    case MsgCode::ReplicaStatus:
      d.numberOfReceivedStatusMessages++;
      break;
    case MsgCode::StateTransfer:  // TODO(GG): TBD?
      d.numberOfReceivedSTMessages++;
      break;
    case MsgCode::CommitPartial:
    case MsgCode::CommitFull:
      d.numberOfReceivedCommitMessages++;
      break;
    default:
      break;
  }
}

void DebugStatistics::onSendExMessage(uint16_t type) {
  DebugStatDesc& d = getDebugStatDesc();

  d.sendMessages++;
}

void DebugStatistics::onRequestCompleted(bool isReadOnly) {
  DebugStatDesc& d = getDebugStatDesc();

  if (isReadOnly)
    d.completedReadOnlyRequests++;
  else
    d.completedReadWriteRequests++;
}

void DebugStatistics::onSendPrePrepareMessage(size_t batchRequests, size_t pendingRequests) {
  DebugStatDesc& d = getDebugStatDesc();

  d.prePrepareMessages++;
  d.batchRequests += batchRequests;
  d.pendingRequests += pendingRequests;
}

void DebugStatistics::onLastExecutedSequenceNumberChanged(int64_t newNumber) {
  DebugStatDesc& d = getDebugStatDesc();
  d.lastExecutedSequenceNumber = newNumber;
}

void DebugStatistics::clearCounters(DebugStatDesc& d) {
  d.receivedMessages = 0;
  d.sendMessages = 0;
  d.completedReadOnlyRequests = 0;
  d.completedReadWriteRequests = 0;
  d.numberOfReceivedSTMessages = 0;
  d.numberOfReceivedStatusMessages = 0;
  d.numberOfReceivedCommitMessages = 0;
  d.lastExecutedSequenceNumber = 0;
  d.prePrepareMessages = 0;
  d.batchRequests = 0;
  d.pendingRequests = 0;
}

DebugStatistics::DebugStatDesc DebugStatistics::globalDebugStatDesc;

void DebugStatistics::initDebugStatisticsData() {}

void DebugStatistics::freeDebugStatisticsData() {}

}  // namespace impl
}  // namespace bftEngine
