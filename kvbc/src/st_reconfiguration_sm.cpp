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

#include "st_reconfiguraion_sm.hpp"
#include "hex_tools.h"
#include "endianness.hpp"
#include "ControlStateManager.hpp"
#include "bftengine/EpochManager.hpp"

namespace concord::kvbc {

template <typename T>
void StReconfigurationHandler::deserializeCmfMessage(T &msg, const std::string &strval) {
  std::vector<uint8_t> bytesval(strval.begin(), strval.end());
  concord::messages::deserialize(bytesval, msg);
}

uint64_t StReconfigurationHandler::getStoredBftSeqNum(BlockId bid) {
  auto value = ro_storage_.get(kvbc::kConcordInternalCategoryId, std::string{kvbc::keyTypes::bft_seq_num_key}, bid);
  auto sequenceNum = uint64_t{0};
  if (value) {
    const auto &data = std::get<categorization::VersionedValue>(*value).data;
    ConcordAssertEQ(data.size(), sizeof(uint64_t));
    sequenceNum = concordUtils::fromBigEndianBuffer<uint64_t>(data.data());
  }
  return sequenceNum;
}

uint64_t StReconfigurationHandler::getStoredEpochNumber(BlockId bid) {
  auto value =
      ro_storage_.get(kvbc::kConcordInternalCategoryId, std::string{kvbc::keyTypes::reconfiguration_epoch_key}, bid);
  auto epoch = uint64_t{0};
  if (value) {
    const auto &data = std::get<categorization::VersionedValue>(*value).data;
    ConcordAssertEQ(data.size(), sizeof(uint64_t));
    epoch = concordUtils::fromBigEndianBuffer<uint64_t>(data.data());
  }
  return epoch;
}

void StReconfigurationHandler::stCallBack(uint64_t current_cp_num) {
  // Handle reconfiguration state changes if exist
  handleStoredCommand<concord::messages::DownloadCommand>(std::string{kvbc::keyTypes::reconfiguration_download_key},
                                                          current_cp_num);
  handleStoredCommand<concord::messages::InstallCommand>(std::string{kvbc::keyTypes::reconfiguration_install_key},
                                                         current_cp_num);
  handleStoredCommand<concord::messages::KeyExchangeCommand>(std::string{kvbc::keyTypes::reconfiguration_key_exchange},
                                                             current_cp_num);
  handleStoredCommand<concord::messages::AddRemoveCommand>(std::string{kvbc::keyTypes::reconfiguration_add_remove},
                                                           current_cp_num);
  handleStoredCommand<concord::messages::AddRemoveWithWedgeCommand>(
      std::string{kvbc::keyTypes::reconfiguration_add_remove, 0x1}, current_cp_num);
  if (bftEngine::ReplicaConfig::instance().pruningEnabled_) {
    handleStoredCommand<concord::messages::PruneRequest>(std::string{kvbc::keyTypes::reconfiguration_pruning_key, 0x1},
                                                         current_cp_num);
  }
  handleStoredCommand<concord::messages::WedgeCommand>(std::string{kvbc::keyTypes::reconfiguration_wedge_key},
                                                       current_cp_num);
}

void StReconfigurationHandler::pruneOnStartup() {
  if (bftEngine::ReplicaConfig::instance().pruningEnabled_) {
    handleStoredCommand<concord::messages::PruneRequest>(std::string{kvbc::keyTypes::reconfiguration_pruning_key, 0x1},
                                                         0);
  }
}
template <typename T>
bool StReconfigurationHandler::handleStoredCommand(const std::string &key, uint64_t current_cp_num) {
  auto res = ro_storage_.getLatest(kvbc::kConcordInternalCategoryId, key);
  if (res.has_value()) {
    auto blockid = ro_storage_.getLatestVersion(kvbc::kConcordInternalCategoryId, key).value().version;
    auto seqNum = getStoredBftSeqNum(blockid);
    auto strval = std::visit([](auto &&arg) { return arg.data; }, *res);
    T cmd;
    deserializeCmfMessage(cmd, strval);
    return handle(cmd, seqNum, current_cp_num, blockid);
  }
  return false;
}

bool StReconfigurationHandler::handle(const concord::messages::WedgeCommand &,
                                      uint64_t bft_seq_num,
                                      uint64_t current_cp_num,
                                      uint64_t bid) {
  auto my_last_known_epoch = bftEngine::EpochManager::instance().getSelfEpochNumber();
  auto last_known_global_epoch = bftEngine::EpochManager::instance().getGlobalEpochNumber();
  auto command_epoch = getStoredEpochNumber(bid);
  auto cp_sn = checkpointWindowSize * current_cp_num;
  auto wedge_point = (bft_seq_num + 2 * checkpointWindowSize);
  wedge_point = wedge_point - (wedge_point % checkpointWindowSize);

  LOG_INFO(GL, KVLOG(my_last_known_epoch, last_known_global_epoch, command_epoch, cp_sn, wedge_point));
  ConcordAssert(last_known_global_epoch >= my_last_known_epoch);

  if (my_last_known_epoch == command_epoch && my_last_known_epoch == last_known_global_epoch) {
    bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
    return true;
  }

  if (command_epoch < last_known_global_epoch &&
      bftEngine::ControlStateManager::instance().getCheckpointToStopAt().has_value()) {
    LOG_INFO(GL, "unwedge due to higher epoch number after state transfer");
    bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(0);
    bftEngine::IControlHandler::instance()->resetState();
  }
  bftEngine::EpochManager::instance().setSelfEpochNumber(bftEngine::EpochManager::instance().getGlobalEpochNumber());
  return true;
}
bool StReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeCommand &command,
                                      uint64_t bft_seq_num,
                                      uint64_t current_cp_num,
                                      uint64_t bid) {
  return handleWedgeCommands(
      command, bid, current_cp_num, bft_seq_num, command.bft_support, true, command.restart, command.restart);
}

bool StReconfigurationHandler::handle(const concord::messages::RestartCommand &command,
                                      uint64_t bft_seq_num,
                                      uint64_t current_cp_num,
                                      uint64_t bid) {
  return handleWedgeCommands(
      command, bid, current_cp_num, bft_seq_num, command.bft_support, true, command.restart, command.restart);
}
template <typename T>
bool StReconfigurationHandler::handleWedgeCommands(const T &cmd,
                                                   uint64_t bid,
                                                   uint64_t current_cp,
                                                   uint64_t bft_seq_num,
                                                   bool bft_support,
                                                   bool remove_metadata,
                                                   bool restart,
                                                   bool unwedge) {
  auto my_last_known_epoch = bftEngine::EpochManager::instance().getSelfEpochNumber();
  auto last_known_global_epoch = bftEngine::EpochManager::instance().getGlobalEpochNumber();
  auto command_epoch = getStoredEpochNumber(bid);
  auto cp_sn = checkpointWindowSize * current_cp;
  auto wedge_point = (bft_seq_num + 2 * checkpointWindowSize);
  wedge_point = wedge_point - (wedge_point % checkpointWindowSize);

  LOG_INFO(GL, KVLOG(my_last_known_epoch, last_known_global_epoch, command_epoch, cp_sn, wedge_point));
  ConcordAssert(last_known_global_epoch >= my_last_known_epoch);
  // If we are in an already advanced epoch, we don't need to do anything
  if (my_last_known_epoch > command_epoch) {
    return true;
  }
  if (my_last_known_epoch == command_epoch && my_last_known_epoch == last_known_global_epoch && cp_sn < wedge_point)
    return true;  // We still need to complete another state transfer

  // If we reached to this point, we are defiantly going to run the addRemove command,
  // so lets invoke all original reconfiguration handlers from the product layer (without concord-bft's ones)
  concord::messages::ReconfigurationResponse response;
  for (auto &h : orig_reconf_handlers_) {
    h->handle(cmd, bft_seq_num, UINT32_MAX, response);
  }

  if (my_last_known_epoch < last_known_global_epoch) {
    // now, we cannot rely on the received sequence number (as it may be reused), we simply want to stop immediately
    auto fake_seq_num = cp_sn - 2 * checkpointWindowSize;
    bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(fake_seq_num);
    bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack([=]() {
      if (remove_metadata) bftEngine::ControlStateManager::instance().markRemoveMetadata(false);
      // We want to rely on the new transferred epoch and not to start a new one (in case someone marked it)
      if (unwedge) bftEngine::EpochManager::instance().setNewEpochFlag(false);
      bftEngine::ControlStateManager::instance().restart();
    });
    return true;
  }
  if (my_last_known_epoch == command_epoch && cp_sn == wedge_point) {
    // Now we want to act normally as we just managed to catch the "correct state" from our point of view.
    // So lets simple run manually the concord-bft's reconfiguration handler.
    bftEngine::ControlStateManager::instance().setRestartBftFlag(bft_support);
    if (bft_support) {
      if (remove_metadata)
        bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
            [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
      if (unwedge)
        bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
            [=]() { bftEngine::EpochManager::instance().setNewEpochFlag(true); });
      if (restart) {
        bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
            [=]() { bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(); });
      }
    } else {
      if (remove_metadata)
        bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
            [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
      if (unwedge)
        bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
            [=]() { bftEngine::EpochManager::instance().setNewEpochFlag(true); });
      if (cmd.restart) {
        bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
            [=]() { bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(); });
      }
    }
  }
  return true;
}

bool StReconfigurationHandler::handle(const concord::messages::PruneRequest &command,
                                      uint64_t bft_seq_num,
                                      uint64_t,
                                      uint64_t) {
  // Actual pruning will be done from the lowest latestPruneableBlock returned by the replicas. It means, that even
  // on every state transfer there might be at most one relevant pruning command. Hence it is enough to take the latest
  // saved command and try to execute it
  bool succ = true;
  concord::messages::ReconfigurationResponse response;
  for (auto &h : orig_reconf_handlers_) {
    // If it was written to the blockchain, it means that this is a valid request.
    succ &= h->handle(command, bft_seq_num, UINT32_MAX, response);
  }
  return succ;
}

}  // namespace concord::kvbc