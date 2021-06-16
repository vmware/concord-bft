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
#include "IStateTransfer.hpp"
#include "ReservedPagesClient.hpp"
#include "Serializable.h"
#include "unordered_map"

namespace bftEngine::impl {
class IInternalBFTClient;
class ISigner;
}  // namespace bftEngine::impl

namespace bftEngine {
class EpochManager : public ResPagesClient<EpochManager, 1> {
 public:
  struct InitData {
    std::shared_ptr<impl::IInternalBFTClient> cl;
    std::shared_ptr<impl::ISigner> signer;
    bool first_time;
    uint32_t n;
    uint32_t f;
  };

  struct EpochsData : public concord::serialize::SerializableFactory<EpochsData> {
    std::unordered_map<uint32_t, uint64_t> epochs_;
    uint32_t n_;
    EpochsData(uint32_t n) {
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
  EpochManager(InitData* id) : bft_client_{id->cl}, signer_{id->signer}, epochs_data_{id->n} {
    scratchPage_.resize(sizeOfReservedPage());
    if (loadReservedPage(0, sizeOfReservedPage(), scratchPage_.data())) {
      std::istringstream inStream;
      inStream.str(scratchPage_);
      concord::serialize::Serializable::deserialize(inStream, epochs_data_);
    }
    if (id->first_time) {
      // Send a message with the current replica epoch
    }
  }
  ~EpochManager() = default;
  void updateEpochForReplica(uint32_t replica_id, uint64_t epoch_id);
  uint64_t getEpochForReplica(uint32_t replica_id);
  const EpochsData& getEpochData();

 private:
  EpochManager& operator=(const EpochManager&) = delete;
  EpochManager(const EpochManager&) = delete;

  std::shared_ptr<impl::IInternalBFTClient> bft_client_;
  std::shared_ptr<impl::ISigner> signer_;
  EpochsData epochs_data_;
  std::string scratchPage_;
};
}  // namespace bftEngine
