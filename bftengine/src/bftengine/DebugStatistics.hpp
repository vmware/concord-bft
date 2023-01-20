// Concord
//
// Copyright (c) 2018, 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstddef>
#include <stdint.h>
#include "TimeUtils.hpp"
#include <atomic>

namespace bftEngine {
namespace impl {

const unsigned int DEBUG_STAT_PERIOD_SECONDS = 7;  // 2;

class DebugStatistics {
 public:
  static void onCycleCheck();

  static void onReceivedExMessage(uint16_t type);

  static void onSendExMessage(uint16_t type);

  static void onRequestCompleted(bool isReadOnly);

  static void onSendPrePrepareMessage(size_t batchRequests, size_t pendingRequests);

  static void initDebugStatisticsData();

  static void freeDebugStatisticsData();

  static void onLastExecutedSequenceNumberChanged(int64_t newNumber);

 private:
  struct DebugStatDesc {
    bool initialized;
    Time lastCycleTime;

    size_t receivedMessages;
    std::atomic<size_t> sendMessages;
    size_t completedReadOnlyRequests;
    size_t completedReadWriteRequests;
    size_t numberOfReceivedSTMessages;
    size_t numberOfReceivedStatusMessages;
    size_t numberOfReceivedCommitMessages;
    int64_t lastExecutedSequenceNumber;

    size_t prePrepareMessages;
    size_t batchRequests;
    size_t pendingRequests;

    DebugStatDesc() : initialized(false) {}
  };

  static DebugStatDesc globalDebugStatDesc;

  static DebugStatDesc& getDebugStatDesc() { return globalDebugStatDesc; }

  static void clearCounters(DebugStatDesc& d);
};

}  // namespace impl
}  // namespace bftEngine
