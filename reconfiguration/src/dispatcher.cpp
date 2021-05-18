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

#include "reconfiguration/dispatcher.hpp"

using namespace concord::messages;

using std::holds_alternative;

namespace concord::reconfiguration {

#define ADDITIONAL_DATA(resp, x)                                                     \
  {                                                                                  \
    std::ostringstream oss;                                                          \
    oss << (x);                                                                      \
    std::string str = oss.str();                                                     \
    std::copy(str.cbegin(), str.cend(), std::back_inserter((resp).additional_data)); \
  }

ReconfigurationResponse Dispatcher::dispatch(const ReconfigurationRequest& request, uint64_t sequence_num) {
  ReconfigurationResponse rresp;
  rresp.success = true;
  concord::messages::ReconfigurationErrorMsg error_msg;
  try {
    for (auto& handler : reconfig_handlers_) {
      // Each reconfiguration handler handles only what it can validate
      if (!handler->verifySignature(request, error_msg)) continue;
      if (holds_alternative<WedgeCommand>(request.command)) {
        rresp.success |= handler->handle(std::get<WedgeCommand>(request.command), sequence_num, error_msg);
      } else if (holds_alternative<WedgeStatusRequest>(request.command)) {
        WedgeStatusResponse wedge_response;
        rresp.success &= handler->handle(std::get<WedgeStatusRequest>(request.command), wedge_response, error_msg);
        rresp.response.emplace<WedgeStatusResponse>(wedge_response);
      } else if (holds_alternative<GetVersionCommand>(request.command)) {
        LOG_INFO(getLogger(), "GetVersionCommand");
        concord::messages::GetVersionResponse response;
        rresp.success &= handler->handle(std::get<GetVersionCommand>(request.command), response, error_msg);
        rresp.response.emplace<concord::messages::GetVersionResponse>(response);
      } else if (holds_alternative<DownloadCommand>(request.command)) {
        LOG_INFO(getLogger(), "DownloadCommand");
        rresp.success &= handler->handle(std::get<DownloadCommand>(request.command), sequence_num, error_msg);
      } else if (holds_alternative<InstallCommand>(request.command)) {
        LOG_INFO(getLogger(), "InstallCommand");
        rresp.success &= handler->handle(std::get<InstallCommand>(request.command), sequence_num, error_msg);
      } else if (holds_alternative<InstallStatusCommand>(request.command)) {
        LOG_INFO(getLogger(), "InstallStatusCommand");
        InstallStatusResponse response;
        rresp.success &= handler->handle(std::get<InstallStatusCommand>(request.command), response, error_msg);
        rresp.response.emplace<InstallStatusResponse>(response);
      } else if (holds_alternative<KeyExchangeCommand>(request.command)) {
        rresp.success &= handler->handle(std::get<KeyExchangeCommand>(request.command), error_msg, sequence_num);
      } else if (holds_alternative<AddRemoveCommand>(request.command)) {
        rresp.success &= handler->handle(std::get<AddRemoveCommand>(request.command), error_msg, sequence_num);
      }
    }

    if (!rresp.success) {
      ADDITIONAL_DATA(rresp, "Request signature verification failure");
      rresp.response.emplace<concord::messages::ReconfigurationErrorMsg>(error_msg);
      return rresp;
    }
    for (auto& handler : pruning_handlers_) {
      if (!handler->verifySignature(request, error_msg)) continue;
      if (holds_alternative<LatestPrunableBlockRequest>(request.command)) {
        LOG_INFO(getLogger(), "LatestPrunableBlockRequest");
        LatestPrunableBlock last_pruneable_block;
        rresp.success &=
            handler->handle(std::get<LatestPrunableBlockRequest>(request.command), last_pruneable_block, error_msg);
        rresp.response.emplace<LatestPrunableBlock>(last_pruneable_block);
      } else if (holds_alternative<PruneRequest>(request.command)) {
        LOG_INFO(getLogger(), "PruneRequest");
        kvbc::BlockId latest_prunable_block_id = 0;
        rresp.success &=
            handler->handle(std::get<PruneRequest>(request.command), latest_prunable_block_id, sequence_num, error_msg);
        ADDITIONAL_DATA(rresp, std::to_string(latest_prunable_block_id));
      } else if (holds_alternative<PruneStatusRequest>(request.command)) {
        LOG_INFO(getLogger(), "PruneStatus");
        PruneStatus status;
        rresp.success &= handler->handle(std::get<PruneStatusRequest>(request.command), status, error_msg);
        rresp.response.emplace<PruneStatus>(status);
      }
    }
  } catch (const std::exception& e) {
    ADDITIONAL_DATA(rresp,
                    "Reconfiguration request " + std::to_string(sequence_num) + " failed, exception: " + e.what());
  }
  // If there was any error, replace the reply with the error message.
  if (!rresp.success) {
    LOG_ERROR(getLogger(), "Error while reconfiguration " + error_msg.error_msg);
    rresp.response.emplace<concord::messages::ReconfigurationErrorMsg>(error_msg);
  }
  return rresp;
}

}  // namespace concord::reconfiguration
