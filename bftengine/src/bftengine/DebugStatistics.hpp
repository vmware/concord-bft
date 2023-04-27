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
    bool initialized = false;
    Time lastCycleTime;

    size_t receivedMessages = 0;
    std::atomic<size_t> sendMessages = 0;
    size_t completedReadOnlyRequests = 0;
    size_t completedReadWriteRequests = 0;
    size_t numberOfReceivedSTMessages = 0;
    size_t numberOfReceivedStatusMessages = 0;
    size_t numberOfReceivedCommitMessages = 0;
    int64_t lastExecutedSequenceNumber = 0;

    size_t prePrepareMessages = 0;
    size_t batchRequests = 0;
    size_t pendingRequests = 0;

    DebugStatDesc() : initialized(false) {}
  };

  static DebugStatDesc globalDebugStatDesc;

  static DebugStatDesc& getDebugStatDesc() { return globalDebugStatDesc; }

  static void clearCounters(DebugStatDesc& d);
};

}  // namespace impl
}  // namespace bftEngine
