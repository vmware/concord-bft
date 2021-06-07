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
#include "ReservedPagesClient.hpp"
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

static constexpr uint32_t ControlHandlerStateManagerNumOfReservedPages = 1;

class ControlStateManager : public ResPagesClient<ControlStateManager, ControlHandlerStateManagerNumOfReservedPages> {
 public:
  static ControlStateManager& instance() {
    static ControlStateManager instance_;
    return instance_;
  }
  ~ControlStateManager() = default;
  void setStopAtNextCheckpoint(int64_t currentSeqNum);
  std::optional<int64_t> getCheckpointToStopAt();

  void setEraseMetadataFlag(int64_t currentSeqNum);
  std::optional<int64_t> getEraseMetadataFlag();

  void markRemoveMetadata() { remove_metadata_(); }
  void clearCheckpointToStopAt();
  void setPruningProcess(bool onPruningProcess) { onPruningProcess_ = onPruningProcess; }
  bool getPruningProcessStatus() const { return onPruningProcess_; }

  void disable() { enabled_ = false; }
  void enable() { enabled_ = true; }

  void setRemoveMetadataFunc(std::function<void()> fn) { remove_metadata_ = fn; }

 private:
  ControlStateManager() { scratchPage_.resize(sizeOfReservedPage()); }
  ControlStateManager& operator=(const ControlStateManager&) = delete;
  ControlStateManager(const ControlStateManager&) = delete;

  std::string scratchPage_;
  bool enabled_ = true;
  ControlStatePage page_;
  std::atomic_bool onPruningProcess_ = false;
  std::function<void()> remove_metadata_;
};
}  // namespace bftEngine
