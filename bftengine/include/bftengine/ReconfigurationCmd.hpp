// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
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
#include "ReservedPagesClient.hpp"
#include "Serializable.h"
#include "concord.cmf.hpp"

namespace bftEngine {
class ReconfigurationCmd : public bftEngine::ResPagesClient<ReconfigurationCmd, 1> {
  struct ReconfigurationCmdData : public concord::serialize::SerializableFactory<ReconfigurationCmdData> {
    std::vector<uint8_t> data_;
    uint64_t seqNum_{0};
    uint64_t epochNum_{0};
    ReconfigurationCmdData() = default;
    ReconfigurationCmdData(std::vector<uint8_t>& d) { data_ = std::move(d); }
    void serializeDataMembers(std::ostream& outStream) const override {
      serialize(outStream, epochNum_);
      serialize(outStream, seqNum_);
      serialize(outStream, data_);
    }
    void deserializeDataMembers(std::istream& inStream) override {
      deserialize(inStream, epochNum_);
      deserialize(inStream, seqNum_);
      deserialize(inStream, data_);
    }
  };
  ReconfigurationCmd() = default;

 public:
  static ReconfigurationCmd& instance() {
    static ReconfigurationCmd instance_;
    return instance_;
  }
  void saveReconfigurationCmdToResPages(const concord::messages::AddRemoveWithWedgeCommand& rreq,
                                        const uint64_t& sequence_num,
                                        const uint64_t& epoch_number) {}
  bool getReconfigurationCmdFromResPages(concord::messages::AddRemoveWithWedgeCommand& rreq,
                                         uint64_t& sequence_num,
                                         uint64_t& epoch_number) {
    return false;
  }
};
}  // namespace bftEngine