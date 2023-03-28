// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "diagnostics.h"

namespace preprocessor {
class PreProcessorRecorder {
 public:
  PreProcessorRecorder() {
    auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    registrar.perf.registerComponent("pre-execution",
                                     {onClientPreProcessRequestMsg,
                                      onClientBatchPreProcessRequestMsg,
                                      onPreProcessRequestMsg,
                                      onPreProcessBatchRequestMsg,
                                      onPreProcessReplyMsg,
                                      onPreProcessBatchReplyMsg,
                                      launchReqPreProcessing,
                                      handlePreProcessedReqByNonPrimary,
                                      handlePreProcessedReqPrimaryRetry,
                                      handlePreProcessedReqByPrimary,
                                      sendPreProcessRequestToAllReplicas,
                                      sendPreProcessBatchRequestToAllReplicas,
                                      finalizePreProcessing,
                                      signPreProcessReplyHash,
                                      verifyPreProcessReplySig,
                                      totalPreExecutionDuration,
                                      launchAsyncPreProcessJob,
                                      onRequestsStatusCheckTimer});
  }

  // 5 Minutes, 300 seconds
  static constexpr int64_t MAX_VALUE_MICROSECONDS = 300000000;

  using Recorder = concord::diagnostics::Recorder;
  using Unit = concord::diagnostics::Unit;

  DEFINE_SHARED_RECORDER(onClientPreProcessRequestMsg, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(onClientBatchPreProcessRequestMsg, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(onPreProcessRequestMsg, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(onPreProcessBatchRequestMsg, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(onPreProcessBatchReplyMsg, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(onPreProcessReplyMsg, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(launchReqPreProcessing, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(handlePreProcessedReqByNonPrimary, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(handlePreProcessedReqPrimaryRetry, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(handlePreProcessedReqByPrimary, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(sendPreProcessRequestToAllReplicas, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(sendPreProcessBatchRequestToAllReplicas, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(verifyPreProcessReplySig, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(signPreProcessReplyHash, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(finalizePreProcessing, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(totalPreExecutionDuration, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(launchAsyncPreProcessJob, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(onRequestsStatusCheckTimer, MAX_VALUE_MICROSECONDS, Unit::MICROSECONDS);
};

}  // namespace preprocessor
