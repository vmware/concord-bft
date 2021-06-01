// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "RequestHandler.h"
#include <sstream>

#include "bftengine/KeyExchangeManager.hpp"
#include "SigManager.hpp"

using concord::messages::ReconfigurationRequest;
using concord::messages::ReconfigurationResponse;

namespace bftEngine {

void RequestHandler::execute(IRequestsHandler::ExecutionRequestsQueue& requests,
                             const std::string& batchCid,
                             concordUtils::SpanWrapper& parent_span) {
  for (auto& req : requests) {
    if (req.flags & KEY_EXCHANGE_FLAG) {
      KeyExchangeMsg ke = KeyExchangeMsg::deserializeMsg(req.request, req.requestSize);
      LOG_DEBUG(GL, "BFT handler received KEY_EXCHANGE msg " << ke.toString());
      auto resp = impl::KeyExchangeManager::instance().onKeyExchange(ke, req.executionSequenceNum, req.cid);
      if (resp.size() <= req.maxReplySize) {
        std::copy(resp.begin(), resp.end(), req.outReply);
        req.outActualReplySize = resp.size();
      } else {
        LOG_ERROR(GL, "KEY_EXCHANGE response is too large, response " << resp);
        req.outActualReplySize = 0;
      }
      req.outExecutionStatus = 0;
    } else if (req.flags & MsgFlag::RECONFIG_FLAG) {
      ReconfigurationRequest rreq;
      deserialize(std::vector<std::uint8_t>(req.request, req.request + req.requestSize), rreq);
      ReconfigurationResponse rsi_res = reconfig_dispatcher_.dispatch(rreq, req.executionSequenceNum);
      // Serialize response
      ReconfigurationResponse res;
      res.success = rsi_res.success;
      std::vector<uint8_t> serialized_response;
      concord::messages::serialize(serialized_response, res);

      std::vector<uint8_t> serialized_rsi_response;
      concord::messages::serialize(serialized_rsi_response, rsi_res);
      if (serialized_rsi_response.size() + serialized_response.size() <= req.maxReplySize) {
        std::copy(serialized_response.begin(), serialized_response.end(), req.outReply);
        std::copy(
            serialized_rsi_response.begin(), serialized_rsi_response.end(), req.outReply + serialized_response.size());
        req.outActualReplySize = serialized_response.size() + serialized_rsi_response.size();
        req.outReplicaSpecificInfoSize = serialized_rsi_response.size();
      } else {
        std::string error("Reconfiguration response is too large");
        LOG_ERROR(GL, error);
        std::copy(error.cbegin(), error.cend(), std::back_inserter(rsi_res.additional_data));
        req.outActualReplySize = 0;
      }
      req.outExecutionStatus = 0;  // stop further processing of this request
    }
    if (req.flags & READ_ONLY_FLAG) {
      // Backward compatible with read only flag prior BC-5126
      req.flags = READ_ONLY_FLAG;
    }
    // The primary can send an object to persist on chain e.g public_keys, configuration file, etc
    // this object pass consensus and need to be validated that is equal to the object that is stored in memory of the
    // replica (per Ittai advice).
    if (req.flags & bftEngine::MsgFlag::VALIDATE_ON_CHAIN_OBJECT_FLAG) {
      // if replica recieved client keys, it means that the primary has detected that its keys are outdated or empty.
      // the recieved keys must be equal to the sigmanager keys.
      if (req.flags & bftEngine::MsgFlag::CLIENTS_PUB_KEYS_FLAG) {
        std::string recieved_keys(req.request, req.requestSize);
        auto current_keys = impl::SigManager::instance()->ClientsPublicKeys();
        if (recieved_keys != current_keys) {
          LOG_INFO(ON_CHAIN_LOG, "Published Clients keys and replica client keys do not match");
          ConcordAssertEQ(recieved_keys, current_keys);
        }
        impl::KeyExchangeManager::instance().commitClientsPublicKeys(recieved_keys);
        req.outExecutionStatus = 0;
        req.outReply[0] = '1';
        req.outActualReplySize = 1;
      }
    }
  }
  if (userRequestsHandler_) return userRequestsHandler_->execute(requests, batchCid, parent_span);
  return;
}

}  // namespace bftEngine
