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

#include "bftengine/MsgsCommunicator.hpp"
#include "communication/ICommunication.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "MsgsCommunicator.hpp"
#include "client/reconfiguration/poll_based_state_client.hpp"
#include "client/reconfiguration/client_reconfiguration_engine.hpp"
#include "bftclient/bft_client.h"

namespace bftEngine::bcst::asyncCRE {

class Communication : public bft::communication::ICommunication {
 public:
  Communication(std::shared_ptr<MsgsCommunicator> msgsCommunicator,
                std::shared_ptr<MsgHandlersRegistrator> msgHandlers);

  int getMaxMessageSize() override { return 128 * 1024; }  // 128KB
  int start() override;
  int stop() override;
  bool isRunning() const override;
  bft::communication::ConnectionStatus getCurrentConnectionStatus(bft::communication::NodeNum node) override;
  int send(bft::communication::NodeNum destNode, std::vector<uint8_t>&& msg) override;
  std::set<bft::communication::NodeNum> send(std::set<bft::communication::NodeNum> dests,
                                             std::vector<uint8_t>&& msg) override;
  void setReceiver(bft::communication::NodeNum receiverNum, bft::communication::IReceiver* receiver) override;
  void dispose(bft::communication::NodeNum i) override {}

 private:
  std::shared_ptr<MsgsCommunicator> msgsCommunicator_;
  bool is_running_ = false;
  bft::communication::IReceiver* receiver_ = nullptr;
};
class CreFactory {
 public:
  static std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine> create(
      std::shared_ptr<MsgsCommunicator> msgsCommunicator, std::shared_ptr<MsgHandlersRegistrator> msgHandlers);
};
}  // namespace bftEngine::bcst::asyncCRE
