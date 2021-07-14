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
#include "ReservedPagesClient.hpp"
#include "Serializable.h"
#include "Metrics.hpp"
#include <functional>

namespace bftEngine {
class EpochManager : public bftEngine::ResPagesClient<EpochManager, 1> {
  struct EpochData : public concord::serialize::SerializableFactory<EpochData> {
    uint64_t epochNumber_;
    EpochData() = default;
    EpochData(uint64_t epochNumber) : epochNumber_{epochNumber} {}

    void serializeDataMembers(std::ostream& outStream) const override { serialize(outStream, epochNumber_); }
    void deserializeDataMembers(std::istream& inStream) override { deserialize(inStream, epochNumber_); }
  };

  EpochManager()
      : metrics_{concordMetrics::Component("epoch_manager", std::make_shared<concordMetrics::Aggregator>())},
        epoch_number_gauge_(metrics_.RegisterGauge("epoch_number", 0)) {
    page.resize(sizeOfReservedPage());
    metrics_.Register();
  }

 public:
  static EpochManager& instance() {
    static EpochManager instance_;
    return instance_;
  }
  uint64_t getSelfEpochNumber() { return epochNumber_; }
  uint64_t getGlobalEpochNumber() {
    if (!loadReservedPage(0, sizeOfReservedPage(), page.data())) return 0;
    EpochData edata;
    std::istringstream inStream;
    inStream.str(page);
    concord::serialize::Serializable::deserialize(inStream, edata);
    return edata.epochNumber_;
  }
  void setSelfEpochNumber(uint64_t newEpoch) {
    epochNumber_ = newEpoch;
    epoch_number_gauge_.Get().Set(newEpoch);
    metrics_.UpdateAggregator();
  }
  void setGlobalEpochNumber(uint64_t newEpoch) {
    EpochData edata{newEpoch};
    std::ostringstream outStream;
    concord::serialize::Serializable::serialize(outStream, edata);
    auto data = outStream.str();
    saveReservedPage(0, data.size(), data.data());
  }

  void startNewEpoch() { startNewEpoch_ = true; }
  void markEpochAsStarted() { startNewEpoch_ = false; }
  bool isNewEpoch() { return startNewEpoch_; }
  void setNewEpochFlagHandler(const std::function<void(bool)>& cb) { setNewEpochFlagCallBack_ = cb; }
  void setNewEpochFlag(bool flag) {
    if (setNewEpochFlagCallBack_) setNewEpochFlagCallBack_(flag);
  }
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) { metrics_.SetAggregator(aggregator); }

 private:
  uint64_t epochNumber_{0};
  bool startNewEpoch_{false};
  std::string page;
  std::function<void(bool)> setNewEpochFlagCallBack_;
  concordMetrics::Component metrics_;
  concordMetrics::GaugeHandle epoch_number_gauge_;
};
}  // namespace bftEngine