// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "ReplicaForStateTransfer.hpp"

namespace bftEngine::impl {

class PersistentStorage;
/**
 *
 */
class ReadOnlyReplica : public ReplicaForStateTransfer {
 public:
  ReadOnlyReplica(const ReplicaConfig&,
                  IStateTransfer*,
                  std::shared_ptr<MsgsCommunicator>,
                  std::shared_ptr<PersistentStorage>,
                  std::shared_ptr<MsgHandlersRegistrator>);

  void start() override;
  void stop() override;
  virtual bool isReadOnly() const override { return true; }

 protected:
  void sendAskForCheckpointMsg();

  void onTransferringCompleteImp(int64_t newStateCheckpoint) override;
  void onReportAboutInvalidMessage(MessageBase* msg, const char* reason) override;

  template <typename T>
  void messageHandler(MessageBase* msg) {
    if (validateMessage(msg) && !isCollectingState())
      onMessage<T>(static_cast<T*>(msg));
    else
      delete msg;
  }

  template <class T>
  void onMessage(T*);

 protected:
  std::shared_ptr<PersistentStorage> ps_;
  // last known stable checkpoint of each peer replica.
  // We sometimes delete checkpoints before lastExecutedSeqNum
  std::map<ReplicaId, CheckpointMsg*> tableOfStableCheckpoints;

  concordUtil::Timers::Handle askForCheckpointMsgTimer_;

  struct Metrics {
    concordMetrics::CounterHandle received_checkpoint_msg_;
    concordMetrics::CounterHandle sent_ask_for_checkpoint_msg_;
    concordMetrics::CounterHandle received_invalid_msg_;
    concordMetrics::GaugeHandle last_executed_seq_num_;
  } ro_metrics_;
};

}  // namespace bftEngine::impl
