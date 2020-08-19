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

static constexpr uint32_t ControlHandlerStateManagerReservedPagesIndex = 1;
static constexpr uint32_t ControlHandlerStateManagerNumOfReservedPages = 1;
class ControlStateManager : public ResPagesClient<ControlStateManager,
                                                  ControlHandlerStateManagerReservedPagesIndex,
                                                  ControlHandlerStateManagerNumOfReservedPages> {
 public:
  void setStopAtNextCheckpoint(int64_t currentSeqNum);
  std::optional<int64_t> getCheckpointToStopAt();
  void clearCheckpointToStopAt();
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

  // In the control handler manager reserved pages space, each control data should has its own page.
  // This struct define the index of each page in this space.
  struct reservedPageIndexer {
    uint32_t update_reserved_page_ = 0;
  };

  reservedPageIndexer reserved_pages_indexer_;

  uint32_t getUpdateReservedPageIndex() { return resPageOffset() + reserved_pages_indexer_.update_reserved_page_; }
};

namespace controlStateMessages {

// control state messages
class StopAtNextCheckpointMessage : public concord::serialize::SerializableFactory<StopAtNextCheckpointMessage> {
  const std::string getVersion() const override { return "1"; }
  void serializeDataMembers(std::ostream& outStream) const override {
    serialize_impl(outStream, seqNumToStopAt_, int{});
  }
  void deserializeDataMembers(std::istream& inStream) override { deserialize_impl(inStream, seqNumToStopAt_, int{}); }

 public:
  int64_t seqNumToStopAt_ = 0;
  StopAtNextCheckpointMessage(int64_t checkpointToStopAt) : seqNumToStopAt_(checkpointToStopAt){};
  StopAtNextCheckpointMessage() = default;
};
}  // namespace controlStateMessages
}  // namespace bftEngine
