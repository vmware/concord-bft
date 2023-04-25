
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
#include "MsgHandlersRegistrator.hpp"
#include "MsgsCommunicator.hpp"
#include "client/reconfiguration/client_reconfiguration_engine.hpp"
#include "crypto/signer.hpp"

namespace bftEngine::bcst::asyncCRE {
class CreFactory {
 public:
  static std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine> create(
      std::shared_ptr<MsgsCommunicator> msgsCommunicator,
      std::shared_ptr<MsgHandlersRegistrator> msgHandlers,
      std::unique_ptr<concord::crypto::ISigner> transactionSigner);
};

class ReplicaCRESigner : public concord::crypto::ISigner {
 public:
  size_t signBuffer(const concord::Byte* dataIn, size_t dataLen, concord::Byte* sigOutBuffer) const override;
  size_t signatureLength() const override;
  std::string getPrivKey() const override;
};

}  // namespace bftEngine::bcst::asyncCRE
