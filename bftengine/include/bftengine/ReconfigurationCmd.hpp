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
#include <optional>
#include "client/reconfiguration/cre_interfaces.hpp"
#include "Metrics.hpp"

namespace bftEngine {
class ReconfigurationCmd : public bftEngine::ResPagesClient<ReconfigurationCmd, 1> {
  ReconfigurationCmd()
      : logger_(logging::getLogger("concord.bftengine.reconfigurationCmd")),
        metrics_{
            concordMetrics::Component("reconfiguration_cmd_blockid", std::make_shared<concordMetrics::Aggregator>())},
        reconfiguration_cmd_blockid_gauge_(metrics_.RegisterGauge("reconfiguration_cmd_blockid", 0)) {
    page_.resize(sizeOfReservedPage());
    metrics_.Register();
  }

 public:
  struct ReconfigurationCmdData : public concord::serialize::SerializableFactory<ReconfigurationCmdData> {
    struct cmdBlock : public concord::serialize::SerializableFactory<cmdBlock> {
      uint64_t blockId_{0};
      uint64_t epochNum_{0};
      uint64_t wedgePoint_{0};
      std::vector<uint8_t> data_;
      cmdBlock(const uint64_t& blockId = 0, const uint64_t& epochNum = 0, const uint64_t& wedgePoint = 0)
          : blockId_(blockId), epochNum_(epochNum), wedgePoint_(wedgePoint) {}
      void serializeDataMembers(std::ostream& outStream) const override {
        serialize(outStream, blockId_);
        serialize(outStream, epochNum_);
        serialize(outStream, wedgePoint_);
        serialize(outStream, data_);
      }
      void deserializeDataMembers(std::istream& inStream) override {
        deserialize(inStream, blockId_);
        deserialize(inStream, epochNum_);
        deserialize(inStream, wedgePoint_);
        deserialize(inStream, data_);
      }
    };
    std::map<std::string, cmdBlock> reconfigurationCommands_;
    ReconfigurationCmdData() = default;
    void serializeDataMembers(std::ostream& outStream) const override {
      serialize(outStream, reconfigurationCommands_);
    }
    void deserializeDataMembers(std::istream& inStream) override { deserialize(inStream, reconfigurationCommands_); }
  };

  static ReconfigurationCmd& instance() {
    static ReconfigurationCmd instance_;
    return instance_;
  }
  void saveReconfigurationCmdToResPages(const concord::messages::ReconfigurationRequest& rreq,
                                        const std::string& key,
                                        const uint64_t& block_id,
                                        const uint64_t& wedge_point,
                                        const uint64_t& epoch_number) {
    reconfiguration_cmd_blockid_gauge_.Get().Set(block_id);
    metrics_.UpdateAggregator();
    ReconfigurationCmdData cmdData;
    if (loadReservedPage(0, sizeOfReservedPage(), page_.data())) {
      std::istringstream inStream;
      inStream.str(page_);
      concord::serialize::Serializable::deserialize(inStream, cmdData);
    }
    ReconfigurationCmdData::cmdBlock cmdblock = {block_id, epoch_number, wedge_point};
    concord::messages::serialize(cmdblock.data_, rreq);
    cmdData.reconfigurationCommands_[key] = std::move(cmdblock);
    std::ostringstream outStream;
    concord::serialize::Serializable::serialize(outStream, cmdData);
    auto data = outStream.str();
    saveReservedPage(0, data.size(), data.data());
  }

  bool getStateFromResPages(std::vector<concord::client::reconfiguration::State>& states) {
    if (!loadReservedPage(0, sizeOfReservedPage(), page_.data())) return false;
    ReconfigurationCmdData cmdData;
    std::istringstream inStream;
    inStream.str(page_);
    concord::serialize::Serializable::deserialize(inStream, cmdData);
    uint64_t latestKnownBlockId = 0;
    for (auto& [k, v] : cmdData.reconfigurationCommands_) {
      (void)k;
      std::ostringstream outStream;
      latestKnownBlockId = std::max(latestKnownBlockId, v.blockId_);
      concord::serialize::Serializable::serialize(outStream, v);
      auto data = outStream.str();
      concord::client::reconfiguration::State s = {v.blockId_, {data.begin(), data.end()}};
      states.push_back(std::move(s));
    }
    reconfiguration_cmd_blockid_gauge_.Get().Set(latestKnownBlockId);
    metrics_.UpdateAggregator();
    return true;
  }
  std::optional<uint64_t> getReconfigurationCommandEpochNumber() {
    if (!loadReservedPage(0, sizeOfReservedPage(), page_.data())) return std::nullopt;
    ReconfigurationCmdData cmdData;
    std::istringstream inStream;
    inStream.str(page_);
    concord::serialize::Serializable::deserialize(inStream, cmdData);
    uint64_t latestCmdEpoch = 0;
    for (auto& [k, v] : cmdData.reconfigurationCommands_) {
      (void)k;
      std::ostringstream outStream;
      latestCmdEpoch = std::max(latestCmdEpoch, v.epochNum_);
    }
    return latestCmdEpoch;
  }

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) { metrics_.SetAggregator(aggregator); }

 private:
  std::string page_;
  logging::Logger logger_;
  concordMetrics::Component metrics_;
  concordMetrics::GaugeHandle reconfiguration_cmd_blockid_gauge_;
};  // namespace bftEngine
}  // namespace bftEngine
