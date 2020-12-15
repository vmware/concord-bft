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
    try {
      registrar.perf.registerComponent("pre-execution",
                                       {{"onMessage", onMessage},
                                        {"launchReqPreProcessing", launchReqPreProcessing},
                                        {"handlePreProcessedReqByNonPrimary", handlePreProcessedReqByNonPrimary},
                                        {"handlePreProcessedReqByPrimary", handlePreProcessedReqByPrimary},
                                        {"sendPreProcessRequestToAllReplicas", sendPreProcessRequestToAllReplicas},
                                        {"finalizePreProcessing", finalizePreProcessing},
                                        {"validateMessage", validateMessage},
                                        {"calculateHash", calculateHash},
                                        {"signHash", signHash},
                                        {"convertAndCompareHashes", convertAndCompareHashes},
                                        {"totalPreExecutionDuration", preExecutionTotal}});
    } catch (std::invalid_argument &e) {
      // if component already exists lets keep record on the same histograms
    }
  }

  // 5 Minutes, 300 seconds
  static constexpr int64_t MAX_VALUE_MICROSECONDS = 300000000;
  typedef std::shared_ptr<concord::diagnostics::Recorder> RecorderSharedPtr;
  RecorderSharedPtr onMessage = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr launchReqPreProcessing = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr handlePreProcessedReqByNonPrimary = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr handlePreProcessedReqByPrimary = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr sendPreProcessRequestToAllReplicas = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr finalizePreProcessing = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr validateMessage = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr calculateHash = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr signHash = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr convertAndCompareHashes = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  RecorderSharedPtr preExecutionTotal = std::make_shared<concord::diagnostics::Recorder>(
      1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
};

}  // namespace preprocessor
