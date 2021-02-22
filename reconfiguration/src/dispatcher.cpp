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

#define ADDITIONAL_DATA(resp, x)                                                   \
  {                                                                                \
    std::ostringstream oss;                                                        \
    oss << x;                                                                      \
    std::string str = oss.str();                                                   \
    std::copy(str.cbegin(), str.cend(), std::back_inserter(resp.additional_data)); \
  }

ReconfigurationResponse Dispatcher::dispatch(const ReconfigurationRequest& request, uint64_t sequence_num) {
  ReconfigurationResponse rresp;
  rresp.success = false;
  try {
    for (auto handler : reconfig_handlers_) rresp.success |= handler->verifySignature(request);
    if (holds_alternative<WedgeCommand>(request.command)) {
      const_cast<WedgeCommand&>(std::get<WedgeCommand>(request.command)).stop_seq_num = sequence_num;
      for (auto handler : reconfig_handlers_) rresp.success |= handler->handle(std::get<WedgeCommand>(request.command));
    } else if (holds_alternative<WedgeStatusRequest>(request.command)) {
      WedgeStatusResponse wedge_response;
      for (auto handler : reconfig_handlers_)
        rresp.success |= handler->handle(std::get<WedgeStatusRequest>(request.command), wedge_response);
      rresp.response.emplace<WedgeStatusResponse>(wedge_response);
    } else if (holds_alternative<GetVersionCommand>(request.command)) {
      LOG_INFO(getLogger(), "GetVersionCommand");
      for (auto handler : reconfig_handlers_)
        rresp.success |= handler->handle(std::get<GetVersionCommand>(request.command));
    } else if (holds_alternative<DownloadCommand>(request.command)) {
      LOG_INFO(getLogger(), "DownloadCommand");
      for (auto handler : reconfig_handlers_)
        rresp.success |= handler->handle(std::get<DownloadCommand>(request.command));
      if (rresp.success) ADDITIONAL_DATA(rresp, "Downloading");
    } else if (holds_alternative<UpgradeCommand>(request.command)) {
      LOG_INFO(getLogger(), "UpgradeCommand");
      for (auto handler : reconfig_handlers_)
        rresp.success |= handler->handle(std::get<UpgradeCommand>(request.command));
    } else if (holds_alternative<LatestPrunableBlockRequest>(request.command)) {
      LOG_INFO(getLogger(), "LatestPrunableBlockRequest");
      LatestPrunableBlock last_pruneable_block;
      for (auto handler : pruning_handlers_)
        rresp.success |= handler->handle(std::get<LatestPrunableBlockRequest>(request.command), last_pruneable_block);
      rresp.response.emplace<LatestPrunableBlock>(last_pruneable_block);
    } else if (holds_alternative<PruneRequest>(request.command)) {
      LOG_INFO(getLogger(), "PruneRequest");
      kvbc::BlockId latest_prunable_block_id = 0;
      for (auto handler : pruning_handlers_)
        rresp.success |= handler->handle(std::get<PruneRequest>(request.command), latest_prunable_block_id);
      ADDITIONAL_DATA(rresp, "latest_prunable_block_id: " << latest_prunable_block_id);
    } else if (holds_alternative<PruneStatusRequest>(request.command)) {
      LOG_INFO(getLogger(), "PruneStatus");
      PruneStatus status;
      for (auto handler : pruning_handlers_)
        rresp.success |= handler->handle(std::get<PruneStatusRequest>(request.command), status);
      rresp.response.emplace<PruneStatus>(status);
    } else {
      LOG_ERROR(getLogger(), "Reconfiguration request validation failed: No command set");
    }
  } catch (const std::exception& e) {
    ADDITIONAL_DATA(rresp, "Reconfiguration request " << sequence_num << " failed, exception: " << e.what());
  }
  return rresp;
}

}  // namespace concord::reconfiguration
