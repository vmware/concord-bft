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
#include "bftengine/EpochsManager.hpp"

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

uint64_t StReconfigurationHandler::getEpochNumber(BlockId bid) {
  auto value =
      ro_storage_.get(kvbc::kConcordInternalCategoryId, std::string{kvbc::keyTypes::reconfiguration_epoch_prefix}, bid);
  auto epochNum = uint64_t{0};
  if (value) {
    const auto &data = std::get<categorization::VersionedValue>(*value).data;
    ConcordAssertEQ(data.size(), sizeof(uint64_t));
    epochNum = concordUtils::fromBigEndianBuffer<uint64_t>(data.data());
  }
  return epochNum;
}

void StReconfigurationHandler::stCallBack(uint64_t current_cp_num) {
  // Handle reconfiguration state changes if exist
  handlerStoredCommand<concord::messages::WedgeCommand>(std::string{kvbc::keyTypes::reconfiguration_wedge_key},
                                                        current_cp_num);
  handlerStoredCommand<concord::messages::DownloadCommand>(std::string{kvbc::keyTypes::reconfiguration_download_key},
                                                           current_cp_num);
  handlerStoredCommand<concord::messages::InstallCommand>(std::string{kvbc::keyTypes::reconfiguration_install_key},
                                                          current_cp_num);
  handlerStoredCommand<concord::messages::KeyExchangeCommand>(std::string{kvbc::keyTypes::reconfiguration_key_exchange},
                                                              current_cp_num);
  handlerStoredCommand<concord::messages::AddRemoveCommand>(std::string{kvbc::keyTypes::reconfiguration_add_remove},
                                                            current_cp_num);
  handlerStoredCommand<concord::messages::AddRemoveWithWedgeCommand>(
      std::string{kvbc::keyTypes::reconfiguration_add_remove, 0x1}, current_cp_num);
  handlerStoredCommand<concord::messages::PruneRequest>(std::string{kvbc::keyTypes::reconfiguration_pruning_key, 0x1},
                                                        current_cp_num);
}

template <typename T>
bool StReconfigurationHandler::handlerStoredCommand(const std::string &key, uint64_t current_cp_num) {
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

bool StReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeCommand &command,
                                      uint64_t bft_seq_num,
                                      uint64_t current_cp_num,
                                      uint64_t bid) {
  auto self_epoch_num = bftEngine::EpochManager::instance().getSelfEpoch();
  auto command_epoch_num = getEpochNumber(bid);
  auto maxEpochNumber = bftEngine::EpochManager::instance().getHighestQuorumedEpoch();
  LOG_INFO(GL, KVLOG(self_epoch_num, command_epoch_num, maxEpochNumber));
  if (maxEpochNumber == -1) {
    LOG_ERROR(GL, "unable to get an agreed epoch number");
    return false;
  }

  auto cp_sn = checkpointWindowSize * current_cp_num;
  auto wedge_point = (bft_seq_num + 2 * checkpointWindowSize);
  wedge_point = wedge_point - (wedge_point % checkpointWindowSize);
  LOG_INFO(GL, KVLOG(bft_seq_num, cp_sn, wedge_point));
  // We have already executed this command, no need to execute it again.
  if (self_epoch_num > command_epoch_num) return true;
  // We will probably have more ST, no need to do anything until we done
  if (maxEpochNumber > 0 && command_epoch_num < (uint64_t)maxEpochNumber - 1) return true;
  // We still need to do nothing, we are not on the wedge point.
  if (self_epoch_num == (uint64_t)maxEpochNumber && cp_sn < wedge_point) return true;
  // If we are on the same epoch and on the wedge point, act like a normal replica
  if (self_epoch_num == (uint64_t)maxEpochNumber && cp_sn == wedge_point) {
    if (command.bft) {
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    } else {
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    }
    return true;
  }
  // We just join the network, we know nothing about previous epochs. Then just anounce it and join
  if (!bftEngine::ControlStateManager::instance().isNewEpoch()) {
    bftEngine::EpochManager::instance().reserveEpochNumberForLaterUse((uint64_t)maxEpochNumber);
    return true;
  }
  // Else, we are in a previous epoch, we need to resync ourselves immidietly.
  bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
      [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });

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
    succ &= h->handle(command, bft_seq_num, response);
  }
  return succ;
}

void RoStReconfigurationHandler::stCallBack(uint64_t cp_number) {
  static std::string epoch_key = "last-known-epoch";
  // With read only replica, we only care if the epoch has changed, if it did, we need to invoke the relevant callbacks
  auto highest_known_epoch = bftEngine::EpochManager::instance().getHighestQuorumedEpoch();
  std::string epoch_str;
  db_adapter_.GetMetadata(epoch_key, epoch_str);
  uint64_t currentEpoch = 0;
  if (!epoch_str.empty()) {
    LOG_DEBUG(GL, "unable to get the latest known epoch by the read only replica");
    currentEpoch = concord::util::to<uint64_t>(epoch_str);
  }
  LOG_INFO(GL, "epochs " << KVLOG(currentEpoch, highest_known_epoch));
  if (currentEpoch < (uint64_t)highest_known_epoch) {
    auto newEpoch = std::to_string(highest_known_epoch);
    LOG_INFO(GL, "new epoch is " << newEpoch);
    db_adapter_.WriteMetadata(epoch_key, newEpoch);
  }
  // Note that read only replica saves only the last reachable abd last block in the metadata, hence we don't need to
  // remove old metadata from the local storage
  // bftEngine::ControlStateManager::some_method_for_restart
}
}  // namespace concord::kvbc