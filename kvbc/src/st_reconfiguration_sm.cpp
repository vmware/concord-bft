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

void StReconfigurationHandler::pruneOnStartup() {
  handlerStoredCommand<concord::messages::PruneRequest>(std::string{kvbc::keyTypes::reconfiguration_pruning_key, 0x1},
                                                        0);
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
  if (bftEngine::ControlStateManager::instance().isNewEpoch()) {
    LOG_INFO(GL, "need to start a new epoch, lets start it first");
    return false;
  }
  auto self_epoch_num = bftEngine::EpochManager::instance().getSelfEpoch();
  auto command_epoch_num = getEpochNumber(bid);
  auto maxEpochNumber = bftEngine::EpochManager::instance().getHighestQuorumedEpoch();
  if (maxEpochNumber == -1) {
    LOG_ERROR(GL, "unable to get the highest epoch number in the system");
    return false;
  }
  LOG_INFO(GL, KVLOG(self_epoch_num, command_epoch_num, maxEpochNumber));
  auto cp_sn = checkpointWindowSize * current_cp_num;
  auto wedge_point = (bft_seq_num + 2 * checkpointWindowSize);
  wedge_point = wedge_point - (wedge_point % checkpointWindowSize);
  LOG_INFO(GL, KVLOG(bft_seq_num, cp_sn, wedge_point));

  /*
   * (1) if we already in an advanced epoch, we need to do noting
   */
  if (self_epoch_num > command_epoch_num) return true;

  /*
   * (2) if we are in the same epoch, but not in the wedge point yet, then lets wait for another state transfer
   * (this can be happen if we started state transfer for the last checkpoint before the wedge point
   */
  if (self_epoch_num == command_epoch_num && cp_sn < wedge_point) return true;

  /*
   * If (1) and (2) did not hit, it means that we need to run the original reconfiguration handlers.
   * Notice, that we shouldn't run concord-bft's handler (and it is not registered) as it writes to the reserved pages
   * and we are in the middle of state transfer.
   */
  concord::messages::ReconfigurationResponse response;
  for (auto &h : orig_reconf_handlers_) {
    h->handle(command, bft_seq_num, response);
  }

  /*
   * (3) Now, if we are in the same epoch and in the wedge point and as the other replicas,
   * handle it as the original concord-bft's handler
   */
  if (self_epoch_num == command_epoch_num && self_epoch_num == (uint64_t)maxEpochNumber && cp_sn == wedge_point) {
    if (command.bft) {
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    } else {
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    }
    return true;
  }

  /*
   * (4) The only other possibility is that we are in a previous epoch, in this case, we want to wedge (without writing
   * anything to the reserved pages) and immediately remove the metadata and restart. There is no reason to wait here
   * for the restart algorithm as it will never happen (the other replicas are already in an advanced epoch)
   */
  if (self_epoch_num < command_epoch_num || self_epoch_num < (uint64_t)maxEpochNumber) {
    // now, we cannot rely on the received sequence number, we simply want to stop immediately
    auto fake_seq_num = cp_sn - 2 * checkpointWindowSize;
    bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(fake_seq_num, false);
    bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack([=]() {
      bftEngine::ControlStateManager::instance().markRemoveMetadata();
      // TODO(YB): Call here to the restart callback
    });
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
    succ &= h->handle(command, bft_seq_num, response);
  }
  return succ;
}

}  // namespace concord::kvbc