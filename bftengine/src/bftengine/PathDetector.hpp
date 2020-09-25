
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

#include "bftengine/IPathDetector.hpp"
#include "SequenceWithActiveWindow.hpp"
#include "SysConsts.hpp"
#include "SeqNumInfo.hpp"
#include <memory>

typedef SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumInfo, SeqNumInfo> WindowOfSeqNumInfo;

class PathDetector : public IPathDetector {
  std::shared_ptr<WindowOfSeqNumInfo> windowOfSeqNums_;

 public:
  PathDetector(std::shared_ptr<WindowOfSeqNumInfo> win) : windowOfSeqNums_(win) {}
  virtual bool isSlowPath(const uint64_t& sn) { return windowOfSeqNums_->get(sn).isCommittedInSlowPath(); }
};
