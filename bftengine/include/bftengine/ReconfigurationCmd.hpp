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

namespace bftEngine {
class ReconfigurationCmd : public bftEngine::ResPagesClient<ReconfigurationCmd, 1> {
  struct ReconfigurationCmdData : public concord::serialize::SerializableFactory<ReconfigurationCmdData> {
    uint64_t epochNum_{0};
    uint64_t wedgePoint_{0};
    std::vector<uint8_t> data_;
    ReconfigurationCmdData() = default;
    void serializeDataMembers(std::ostream& outStream) const override {
      serialize(outStream, epochNum_);
      serialize(outStream, wedgePoint_);
      serialize(outStream, data_);
    }
    void deserializeDataMembers(std::istream& inStream) override {
      deserialize(inStream, epochNum_);
      deserialize(inStream, wedgePoint_);
      deserialize(inStream, data_);
    }
  };
  ReconfigurationCmd()
      : metrics_{concordMetrics::Component("reconifguration_wedge_point",
                                           std::make_shared<concordMetrics::Aggregator>())},
        reconfiguration_wedge_point_gauge_(metrics_.RegisterGauge("reconifguration_wedge_point", 0)) {
    page.resize(sizeOfReservedPage());
    metrics_.Register();
  }

 public:
  static ReconfigurationCmd& instance() {
    static ReconfigurationCmd instance_;
    return instance_;
  }
  void saveReconfigurationCmdToResPages(const concord::messages::ReconfigurationRequest& rreq,
                                        const uint64_t& wedge_point,
                                        const uint64_t& epoch_number) {
    ReconfigurationCmdData cmdData;
    concord::messages::serialize(cmdData.data_, rreq);
    cmdData.wedgePoint_ = wedge_point;
    cmdData.epochNum_ = epoch_number;
    reconfiguration_wedge_point_gauge_.Get().Set(wedge_point);
    metrics_.UpdateAggregator();
    std::ostringstream outStream;
    concord::serialize::Serializable::serialize(outStream, cmdData);
    auto data = outStream.str();
    saveReservedPage(0, data.size(), data.data());
  }
  bool getReconfigurationCmdFromResPages(concord::messages::ReconfigurationRequest& rreq,
                                         uint64_t& wedge_point,
                                         uint64_t& epoch_number) {
    if (!loadReservedPage(0, sizeOfReservedPage(), page.data())) return false;
    ReconfigurationCmdData cmdData;
    std::istringstream inStream;
    inStream.str(page);
    concord::serialize::Serializable::deserialize(inStream, cmdData);
    concord::messages::deserialize(cmdData.data_, rreq);
    wedge_point = cmdData.wedgePoint_;
    epoch_number = cmdData.epochNum_;
    reconfiguration_wedge_point_gauge_.Get().Set(wedge_point);
    metrics_.UpdateAggregator();
    return true;
  }

  std::optional<uint64_t> getReconfigurationCmdWedgePoint() {
    concord::messages::ReconfigurationRequest cmd;
    uint64_t wedgePoint{0};
    uint64_t epochNumber{0};
    if (getReconfigurationCmdFromResPages(cmd, wedgePoint, epochNumber)) {
      return wedgePoint;
    }
    return {};
  }

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) { metrics_.SetAggregator(aggregator); }

 private:
  std::string page;
  concordMetrics::Component metrics_;
  concordMetrics::GaugeHandle reconfiguration_wedge_point_gauge_;
};
}  // namespace bftEngine
