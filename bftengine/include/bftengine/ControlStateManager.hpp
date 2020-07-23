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
  void setStopAtNextCheckpoint(uint64_t currentSeqNum);
  std::optional<uint64_t> getCheckpointToStopAt();

  ControlStateManager(IStateTransfer* state_transfer, uint32_t sizeOfReservedPages);
  ControlStateManager& operator=(const ControlStateManager&) = delete;
  ControlStateManager(const ControlStateManager&) = delete;
  ~ControlStateManager() {}

 private:
  IStateTransfer* state_transfer_;
  const uint32_t sizeOfReservedPage_;
  std::string scratchPage_;

  // reserved page indexer
  struct reservedPageIndexer {
    uint32_t update_reserved_page_ = 0;
  };

  reservedPageIndexer reserved_pages_indexer_;
};

namespace controlStateMessages {

// control state messages
class StopAtNextCheckpointMessage : public concord::serialize::SerializableFactory<StopAtNextCheckpointMessage> {
  const std::string getVersion() const override { return "1"; }
  void serializeDataMembers(std::ostream& outStream) const override { serialize_impl(outStream, seqNumToStopAt_, int{}); }
  void deserializeDataMembers(std::istream& inStream) override { deserialize_impl(inStream, seqNumToStopAt_, int{}); }

 public:
  uint64_t seqNumToStopAt_ = 0;
  StopAtNextCheckpointMessage(uint64_t checkpointToStopAt) : seqNumToStopAt_(checkpointToStopAt) {};
  StopAtNextCheckpointMessage() = default;
};
}  // namespace controlStateMessages
}  // namespace bftEngine
