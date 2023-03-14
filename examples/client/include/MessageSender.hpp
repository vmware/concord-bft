// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
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

#include "MessageParser.hpp"
#include "SetupClient.hpp"
#include "IMessageConsumer.hpp"
#include "IMessageGenerator.hpp"

namespace concord::osexample {

class MessageSender {
 public:
  MessageSender(std::shared_ptr<SetupClient> setupClient, std::shared_ptr<MessageConfig> msgConfig);
  ~MessageSender(){};

  std::unique_ptr<IMessageGenerator> getMessageGenerator(uint16_t executionEngineType);
  std::unique_ptr<IMessageConsumer> getMessageConsumer(uint16_t executionEngineType);

  void stopBftClient() {
    {
      std::lock_guard<std::mutex> lock(bftClientLock_);
      if (bftClient_) {
        bftClient_->stop();
      }
    }
  }
  void createAndSendWriteRequest();
  void createAndSendReadRequest();
  void sendRequests();

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("osexample::MessageSender"));
    return logger_;
  }
  uint64_t generateUniqueSequenceNumber();
  void sendReadRequest(bft::client::Msg& msg);
  void sendWriteRequest(bft::client::Msg& msg);

 private:
  // limited to the size lastMilli shifted.
  const u_int64_t lastCountLimit_ = 0x3FFFFF;
  // lastMilliOfUniqueFetchID_ holds the last SN generated,
  uint64_t lastMilliOfUniqueFetchID_ = 0;
  // lastCount used to preserve uniqueness.
  uint32_t lastCountOfUniqueFetchID_ = 0;

  std::unique_ptr<IMessageGenerator> msgGenerator_;
  std::unique_ptr<IMessageConsumer> msgConsumer_;
  std::unique_ptr<bft::client::Client> bftClient_;
  std::shared_ptr<MessageConfig> msgConfig_;
  mutable std::mutex bftClientLock_;
};
}  // End of namespace concord::osexample
