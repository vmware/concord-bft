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
#include <optional>
#include <sstream>

#include "bftengine/KeyExchangeManager.hpp"
#include "SigManager.hpp"

#include <ccron/cron_table_registry.hpp>
#include "ccron_msgs.cmf.hpp"

using concord::messages::ReconfigurationRequest;
using concord::messages::ReconfigurationResponse;

namespace bftEngine {

void RequestHandler::execute(IRequestsHandler::ExecutionRequestsQueue& requests,
                             std::optional<Timestamp> timestamp,
                             const std::string& batchCid,
                             concordUtils::SpanWrapper& parent_span) {
  for (auto& req : requests) {
    if (req.flags & KEY_EXCHANGE_FLAG) {
      KeyExchangeMsg ke = KeyExchangeMsg::deserializeMsg(req.request, req.requestSize);
      LOG_INFO(KEY_EX_LOG, "BFT handler received KEY_EXCHANGE msg " << ke.toString());
      auto resp = impl::KeyExchangeManager::instance().onKeyExchange(ke, req.executionSequenceNum, req.cid);
      if (resp.size() <= req.maxReplySize) {
        std::copy(resp.begin(), resp.end(), req.outReply);
        req.outActualReplySize = resp.size();
      } else {
        LOG_ERROR(KEY_EX_LOG, "KEY_EXCHANGE response is too large, response " << resp);
        req.outActualReplySize = 0;
      }
      req.outExecutionStatus = 0;
    } else if (req.flags & MsgFlag::RECONFIG_FLAG) {
      ReconfigurationRequest rreq;
      deserialize(std::vector<std::uint8_t>(req.request, req.request + req.requestSize), rreq);
      ReconfigurationResponse rsi_res = reconfig_dispatcher_.dispatch(rreq, req.executionSequenceNum);
      // in case of read request return only a success part of and replica specific info in the response
      // and the rest as additional data, since it may differ between replicas
      if (req.flags & MsgFlag::READ_ONLY_FLAG) {
        // Serialize response
        ReconfigurationResponse res;
        res.success = rsi_res.success;
        res.additional_data = rsi_res.additional_data;
        std::vector<uint8_t> serialized_response;
        concord::messages::serialize(serialized_response, res);

        std::vector<uint8_t> serialized_rsi_response;
        concord::messages::serialize(serialized_rsi_response, rsi_res);
        if (serialized_rsi_response.size() + serialized_response.size() <= req.maxReplySize) {
          std::copy(serialized_response.begin(), serialized_response.end(), req.outReply);
          std::copy(serialized_rsi_response.begin(),
                    serialized_rsi_response.end(),
                    req.outReply + serialized_response.size());
          req.outActualReplySize = serialized_response.size() + serialized_rsi_response.size();
          req.outReplicaSpecificInfoSize = serialized_rsi_response.size();
        } else {
          std::string error("Reconfiguration ro response is too large");
          LOG_ERROR(GL, error);
          std::copy(error.cbegin(), error.cend(), std::back_inserter(rsi_res.additional_data));
          req.outActualReplySize = 0;
        }
      } else {  // in case of write request return the whole response
        std::vector<uint8_t> serialized_rsi_response;
        concord::messages::serialize(serialized_rsi_response, rsi_res);
        if (serialized_rsi_response.size() <= req.maxReplySize) {
          std::copy(serialized_rsi_response.begin(), serialized_rsi_response.end(), req.outReply);
          req.outActualReplySize = serialized_rsi_response.size();
        } else {
          std::string error("Reconfiguration response is too large");
          LOG_ERROR(GL, error);
          std::copy(error.cbegin(), error.cend(), std::back_inserter(rsi_res.additional_data));
          req.outActualReplySize = 0;
        }
      }
      req.outExecutionStatus = 0;  // stop further processing of this request
    } else if (req.flags & TICK_FLAG) {
      // Make sure the reply always contains one dummy 0 byte. Needed as empty replies are not supported at that stage.
      // Also, set replica specific information size to 0.
      req.outActualReplySize = 1;
      req.outReply[0] = '\0';
      req.outReplicaSpecificInfoSize = 0;

      // Default is success (0).
      req.outExecutionStatus = 0;

      if (req.flags & READ_ONLY_FLAG) {
        LOG_ERROR(GL, "Received a read-only Tick, ignoring");
        req.outExecutionStatus = 1;
      } else if (cron_table_registry_) {
        using namespace concord::cron;
        auto payload = ClientReqMsgTickPayload{};
        auto req_ptr = reinterpret_cast<const uint8_t*>(req.request);
        deserialize(req_ptr, req_ptr + req.requestSize, payload);
        const auto tick = Tick{payload.component_id, req.executionSequenceNum};
        (*cron_table_registry_)[payload.component_id].evaluate(tick);
      } else {
        LOG_ERROR(GL, "Received a Tick, but the cron table registry is not initialized");
        req.outExecutionStatus = 2;
      }
    }

    if (req.flags & READ_ONLY_FLAG) {
      // Backward compatible with read only flag prior BC-5126
      req.flags = READ_ONLY_FLAG;
    }
    // Replicas can publish an object e.g public_keys, configuration file, etc
    // this object pass consensus, and replicas can perform action against is as:
    // - validated that is equal to the object that is stored in memory of the replica.
    // - save it to reserved pages.
    // - proxy it to the application command handler.
    if (req.flags & bftEngine::MsgFlag::CLIENTS_PUB_KEYS_FLAG) {
      std::string recieved_keys(req.request, req.requestSize);
      std::optional<std::string> bootstrap_keys;
      if (req.flags & bftEngine::MsgFlag::PUBLISH_ON_CHAIN_OBJECT_FLAG) {
        LOG_INFO(KEY_EX_LOG, "Recieved initial publish clients keys request");
        bootstrap_keys = impl::SigManager::instance()->getClientsPublicKeys();
      } else {
        LOG_INFO(KEY_EX_LOG, "Recieved publish clients keys request");
      }
      impl::KeyExchangeManager::instance().onPublishClientsKeys(recieved_keys, bootstrap_keys);
      req.outExecutionStatus = 0;
      req.outReply[0] = '1';
      req.outActualReplySize = 1;
    }
  }
  if (userRequestsHandler_) return userRequestsHandler_->execute(requests, timestamp, batchCid, parent_span);
  return;
}

}  // namespace bftEngine
