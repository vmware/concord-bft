// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "IStateTransfer.hpp"

namespace bftEngine {
namespace impl {
class NullStateTransfer : public IStateTransfer {
 public:
  virtual void init(uint64_t maxNumOfRequiredStoredCheckpoints,
                    uint32_t numberOfRequiredReservedPages,
                    uint32_t sizeOfReservedPage) override;
  virtual void startRunning(IReplicaForStateTransfer* r) override;
  virtual void stopRunning() override;
  virtual bool isRunning() const override;

  virtual void createCheckpointOfCurrentState(uint64_t checkpointNumber) override;
  virtual void getDigestOfCheckpoint(uint64_t checkpointNumber,
                                     uint16_t sizeOfDigestBuffer,
                                     uint64_t& outBlockId,
                                     char* outStateDigest,
                                     char* outResPagesDigest,
                                     char* outRBVDataDigest) override;
  virtual void startCollectingState() override;
  virtual bool isCollectingState() const override;

  virtual uint32_t numberOfReservedPages() const override;
  virtual uint32_t sizeOfReservedPage() const override;
  virtual bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const override;
  virtual void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) override;
  virtual void zeroReservedPage(uint32_t reservedPageId) override;

  virtual void onTimer() override;
  virtual void handleStateTransferMessage(char* msg, uint32_t msgLen, uint16_t senderId) override;

  void addOnTransferringCompleteCallback(const std::function<void(uint64_t)>& cb,
                                         StateTransferCallBacksPriorities priority) override{};
  void addOnFetchingStateChangeCallback(const std::function<void(uint64_t)>& cb) override {}
  void setEraseMetadataFlag() override {}
  void setReconfigurationEngine(
      std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine>) override {}

  virtual void handleIncomingConsensusMessage(const ConsensusMsg msg) override{};
  void reportLastAgreedPrunableBlockId(uint64_t lastAgreedPrunableBlockId) override{};
  virtual ~NullStateTransfer();

 protected:
  uint32_t numOfReservedPages = 0;
  uint32_t sizeOfPage = 0;
  char* reservedPages = nullptr;

  IReplicaForStateTransfer* repApi = nullptr;

  bool running = false;

  bool errorReport = false;

  bool isInitialized() const { return (reservedPages != nullptr); }
};

}  // namespace impl
}  // namespace bftEngine
