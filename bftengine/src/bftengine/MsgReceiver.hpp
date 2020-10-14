// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "PrimitiveTypes.hpp"
#include "communication/ICommunication.hpp"
#include "IncomingMsgsStorage.hpp"

namespace bftEngine::impl {

class MsgReceiver : public bft::communication::IReceiver {
 public:
  explicit MsgReceiver(std::shared_ptr<IncomingMsgsStorage>& storage);
  virtual ~MsgReceiver() = default;

  void onNewMessage(bft::communication::NodeNum sourceNode, const char* const message, size_t messageLength) override;
  void onConnectionStatusChanged(const bft::communication::NodeNum node,
                                 const bft::communication::ConnectionStatus newStatus) override;

 private:
  std::shared_ptr<IncomingMsgsStorage> incomingMsgsStorage_;
};

}  // namespace bftEngine::impl
