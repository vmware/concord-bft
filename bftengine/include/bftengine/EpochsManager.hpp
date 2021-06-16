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
#include "unordered_map"
#include "IStateTransfer.hpp"
#include "Metrics.hpp"

namespace bftEngine::impl {
class IInternalBFTClient;
class RSASigner;
}  // namespace bftEngine::impl

namespace bftEngine {
class EpochManager : public ResPagesClient<EpochManager, 1> {
 public:
  struct InitData {
    std::shared_ptr<impl::IInternalBFTClient> cl;
    std::shared_ptr<impl::RSASigner> signer;
    IStateTransfer& state_transfer;
    uint32_t replica_id;
    uint32_t n;
    uint32_t f;
    bool is_ro;
  };

  struct EpochsData : public concord::serialize::SerializableFactory<EpochsData> {
    std::unordered_map<uint32_t, uint64_t> epochs_;
    uint32_t n_;
    EpochsData() = default;
    EpochsData(uint32_t n) : EpochsData() {
      static_assert(sizeof(EpochsData) < 4096, "The page exceeds the maximal size of reserved page");
      for (uint32_t i = 0; i < n; i++) {
        epochs_.emplace(i, 0);
      }
      n_ = n;
    }

    const std::string getVersion() const override { return "1"; }

    void serializeDataMembers(std::ostream& outStream) const override;
    void deserializeDataMembers(std::istream& inStream) override;
  };

 public:
  static EpochManager& instance(InitData* id = nullptr) {
    static EpochManager instance_(id);
    return instance_;
  }
  EpochManager(InitData* id);
  ~EpochManager() = default;
  void updateEpochForReplica(uint32_t replica_id, uint64_t epoch_id);
  uint64_t getEpochForReplica(uint32_t replica_id);
  const EpochsData& getEpochData();
  void sendUpdateEpochMsg(uint64_t epoch);
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    aggregator_ = aggregator;
    metrics_.SetAggregator(aggregator);
    metrics_.UpdateAggregator();
  }
  uint64_t getSelfEpoch() { return epochs_data_.epochs_[replica_id_]; }

 private:
  EpochManager& operator=(const EpochManager&) = delete;
  EpochManager(const EpochManager&) = delete;

  std::shared_ptr<impl::IInternalBFTClient> bft_client_;
  std::shared_ptr<impl::RSASigner> signer_;
  uint32_t replica_id_;
  EpochsData epochs_data_;
  std::string scratchPage_;
  bool is_ro_;

  // Metrics
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
  // We are not expecting to have many epochs changes, thus we will update the aggregator on every change.
  concordMetrics::Component metrics_;
  concordMetrics::GaugeHandle epoch_number;
  concordMetrics::CounterHandle num_of_sent_epoch_messages_;
};
}  // namespace bftEngine
