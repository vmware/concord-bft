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
                                     {onMessage,
                                      launchReqPreProcessing,
                                      handlePreProcessedReqByNonPrimary,
                                      handlePreProcessedReqByPrimary,
                                      sendPreProcessRequestToAllReplicas,
                                      finalizePreProcessing,
                                      validateMessage,
                                      calculateHash,
                                      signHash,
                                      convertAndCompareHashes,
                                      totalPreExecutionDuration});
  }

  // 5 Minutes, 300 seconds
  static constexpr int64_t MAX_VALUE_MICROSECONDS = 300000000;

  using Recorder = concord::diagnostics::Recorder;
  using Unit = concord::diagnostics::Unit;

  DEFINE_SHARED_RECORDER(onMessage, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(launchReqPreProcessing, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(handlePreProcessedReqByNonPrimary, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(handlePreProcessedReqByPrimary, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(sendPreProcessRequestToAllReplicas, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(finalizePreProcessing, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(validateMessage, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(calculateHash, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(signHash, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(convertAndCompareHashes, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(totalPreExecutionDuration, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
};

}  // namespace preprocessor
