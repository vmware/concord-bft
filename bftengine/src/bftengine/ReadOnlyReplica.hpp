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
#include "Timers.hpp"

namespace bftEngine::impl {

class ClientRequestMsg;
/**
 *
 */
class ReadOnlyReplica : public ReplicaForStateTransfer {
 public:
  ReadOnlyReplica(const ReplicaConfig&,
                  std::shared_ptr<IRequestsHandler>,
                  IStateTransfer*,
                  std::shared_ptr<MsgsCommunicator>,
                  std::shared_ptr<MsgHandlersRegistrator>,
                  concordUtil::Timers& timers);

  void start() override;
  void stop() override;
  virtual bool isReadOnly() const override { return true; }

 protected:
  void sendAskForCheckpointMsg();

  void onTransferringCompleteImp(uint64_t newStateCheckpoint) override;
  void onReportAboutInvalidMessage(MessageBase* msg, const char* reason) override;

  template <typename T>
  void messageHandler(MessageBase* msg) {
    T* trueTypeObj = new T(msg);
    delete msg;
    if (validateMessage(trueTypeObj))
      onMessage<T>(trueTypeObj);
    else
      delete trueTypeObj;
  }

  template <class T>
  void onMessage(T*);

 protected:
  concordUtil::Timers::Handle askForCheckpointMsgTimer_;

  struct Metrics {
    concordMetrics::CounterHandle received_checkpoint_msg_;
    concordMetrics::CounterHandle sent_ask_for_checkpoint_msg_;
    concordMetrics::CounterHandle received_invalid_msg_;
    concordMetrics::GaugeHandle last_executed_seq_num_;
  } ro_metrics_;

  void executeReadOnlyRequest(concordUtils::SpanWrapper& parent_span, const ClientRequestMsg& m);
};

}  // namespace bftEngine::impl
