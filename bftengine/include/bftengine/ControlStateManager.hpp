// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once
#include <optional>
#include "IStateTransfer.hpp"
#include "ReservedPages.hpp"
#include "Serializable.h"
#include "SysConsts.hpp"

namespace bftEngine {

class ControlStatePage : public concord::serialize::SerializableFactory<ControlStatePage> {
 public:
  int64_t seqNumToStopAt_ = 0;
  int64_t eraseMetadataAtSeqNum_ = 0;
  ControlStatePage() {
    static_assert(sizeof(ControlStatePage) < 4096, "The page exceeds the maximal size of reserved page");
  }

 private:
  const std::string getVersion() const override { return "1"; }

  void serializeDataMembers(std::ostream& outStream) const override {
    serialize(outStream, seqNumToStopAt_);
    serialize(outStream, eraseMetadataAtSeqNum_);
  }
  void deserializeDataMembers(std::istream& inStream) override {
    deserialize(inStream, seqNumToStopAt_);
    deserialize(inStream, eraseMetadataAtSeqNum_);
  }
};

static constexpr uint32_t ControlHandlerStateManagerReservedPagesIndex = 1;
static constexpr uint32_t ControlHandlerStateManagerNumOfReservedPages = 1;
class ControlStateManager : public ResPagesClient<ControlStateManager,
                                                  ControlHandlerStateManagerReservedPagesIndex,
                                                  ControlHandlerStateManagerNumOfReservedPages> {
 public:
  void setStopAtNextCheckpoint(int64_t currentSeqNum);
  std::optional<int64_t> getCheckpointToStopAt();

  void setEraseMetadataFlag(int64_t currentSeqNum);
  std::optional<int64_t> getEraseMetadataFlag();

  ControlStateManager(IStateTransfer* state_transfer, uint32_t sizeOfReservedPages);
  ControlStateManager& operator=(const ControlStateManager&) = delete;
  ControlStateManager(const ControlStateManager&) = delete;
  ~ControlStateManager() {}

  void disable() { enabled_ = false; }
  void enable() { enabled_ = true; }

 private:
  IStateTransfer* state_transfer_;
  const uint32_t sizeOfReservedPage_;
  std::string scratchPage_;
  bool enabled_ = true;
  ControlStatePage page_;
};
}  // namespace bftEngine
