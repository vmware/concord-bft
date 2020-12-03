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
  int64_t seq_num_to_stop_at_ = 0;
  int64_t erase_metadata_at_seq_num_ = 0;
  ControlStatePage() {
    static_assert(sizeof(ControlStatePage) < 4096, "The page exceeds the maximal size of reserved page");
  }

 private:
  const std::string getVersion() const override { return "1"; }

  void serializeDataMembers(std::ostream& outStream) const override {
    serialize(outStream, seq_num_to_stop_at_);
    serialize(outStream, erase_metadata_at_seq_num_);
  }
  void deserializeDataMembers(std::istream& inStream) override {
    deserialize(inStream, seq_num_to_stop_at_);
    deserialize(inStream, erase_metadata_at_seq_num_);
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

  void clearControlStateManagerData();

 private:
  IStateTransfer* state_transfer_;
  const uint32_t sizeOfReservedPage_;
  std::string scratchPage_;
  bool enabled_ = true;
  ControlStatePage page_;
};
}  // namespace bftEngine
