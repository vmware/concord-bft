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

#include <endianness.hpp>
#include <future>
#include "bftengine/ControlStateManager.hpp"
#include "pruning_handler.hpp"
#include "categorization/versioned_kv_category.h"
#include "reconfiguration/pruning_utils.hpp"

namespace concord::kvbc::pruning {

const std::string PruningHandler::last_agreed_prunable_block_id_key_{0x24};

PruningHandler::PruningHandler(kvbc::IReader& ro_storage,
                               kvbc::IBlockAdder& blocks_adder,
                               kvbc::IBlocksDeleter& blocks_deleter,
                               bftEngine::IStateTransfer& state_transfer,
                               bool run_async)
    : logger_{logging::getLogger("concord.pruning")},
      signer_{bftEngine::ReplicaConfig::instance().replicaPrivateKey},
      verifier_{bftEngine::ReplicaConfig::instance().publicKeysOfReplicas},
      ro_storage_{ro_storage},
      blocks_adder_{blocks_adder},
      blocks_deleter_{blocks_deleter},
      replica_id_{bftEngine::ReplicaConfig::instance().replicaId},
      run_async_{run_async} {
  pruning_enabled_ = bftEngine::ReplicaConfig::instance().pruningEnabled_;
  num_blocks_to_keep_ = bftEngine::ReplicaConfig::instance().numBlocksToKeep_;
  duration_to_keep_minutes_ = bftEngine::ReplicaConfig::instance().durationToKeppMinutes_;

  // Make sure that blocks from old genesis through the last agreed block are
  // pruned. That might be violated if there was a crash during pruning itself.
  // Therefore, call it every time on startup to ensure no old blocks are
  // present before we allow the system to proceed.
  pruneThroughLastAgreedBlockId();

  // If a replica has missed Prune commands for whatever reason, we still need
  // to execute them. We do that by saving pruning data in the state and later
  // using it to prune relevant blocks when we receive it from state transfer.
  state_transfer.addOnTransferringCompleteCallback(
      [this](uint64_t checkpoint_number) { pruneOnStateTransferCompletion(checkpoint_number); });
}

bool PruningHandler::handle(const concord::messages::LatestPrunableBlockRequest& latest_prunable_block_request,
                            concord::messages::LatestPrunableBlock& latest_prunable_block) {
  // If pruning is disabled, return 0. Otherwise, be conservative and prune the
  // smaller block range.

  const auto latest_prunable_block_id =
      pruning_enabled_ ? std::min(latestBasedOnNumBlocksConfig(), latestBasedOnTimeRangeConfig()) : 0;
  latest_prunable_block.replica = replica_id_;
  latest_prunable_block.block_id = latest_prunable_block_id;
  signer_.sign(latest_prunable_block);
  return true;
}

bool PruningHandler::handle(const concord::messages::PruneRequest& request, kvbc::BlockId& bid) {
  if (!pruning_enabled_) {
    const auto msg = "PruningHandler pruning is disabled, returning an error on PruneRequest";
    LOG_WARN(logger_, msg);
    bid = 0;
    return false;
  }

  const auto sender = request.sender;

  if (!verifier_.verify(request)) {
    LOG_WARN(logger_,
             "PruningHandler failed to verify PruneRequest from principal_id "
                 << sender
                 << " on the grounds that the pruning request did not include "
                    "LatestPrunableBlock responses from the required replicas, or "
                    "on the grounds that some non-empty subset of those "
                    "LatestPrunableBlock messages did not bear correct signatures "
                    "from the claimed replicas.");
    bid = 0;
    return false;
  }

  const auto latest_prunable_block_id = agreedPrunableBlockId(request);
  // Make sure we have persisted the agreed prunable block ID before proceeding.
  // Rationale is that we want to be able to pick up in case of a crash.
  persistLastAgreedPrunableBlockId(latest_prunable_block_id);
  // Execute actual pruning.
  pruneThroughBlockId(latest_prunable_block_id);
  bid = latest_prunable_block_id;
  return true;
}

kvbc::BlockId PruningHandler::latestBasedOnNumBlocksConfig() const {
  const auto last_block_id = ro_storage_.getLastBlockId();
  if (last_block_id < num_blocks_to_keep_) {
    return 0;
  }
  return last_block_id - num_blocks_to_keep_;
}

kvbc::BlockId PruningHandler::latestBasedOnTimeRangeConfig() const {
  /*
   * Currently time records are not saved by concordbft layer.
   * The user may ovveride this method to have time based search.
   */
  const auto last_block_id = ro_storage_.getLastBlockId();
  if (duration_to_keep_minutes_ > 0) LOG_WARN(logger_, "time based pruning is not supported by default");
  return last_block_id;
}

kvbc::BlockId PruningHandler::agreedPrunableBlockId(const concord::messages::PruneRequest& prune_request) const {
  const auto latest_prunable_blocks = prune_request.latest_prunable_block;
  const auto begin = std::cbegin(latest_prunable_blocks);
  const auto end = std::cend(latest_prunable_blocks);
  ConcordAssertNE(begin, end);
  return std::min_element(begin, end, [](const auto& a, const auto& b) { return (a.block_id < b.block_id); })->block_id;
}

std::optional<kvbc::BlockId> PruningHandler::lastAgreedPrunableBlockId() const {
  auto opt_val = ro_storage_.getLatest(kvbc::kConcordInternalCategoryId, last_agreed_prunable_block_id_key_);
  // if it's not found return nullopt, if any other error occurs storage throws.
  if (!opt_val) {
    return std::nullopt;
  }
  auto val = std::get<kvbc::categorization::VersionedValue>(*opt_val);
  return concordUtils::fromBigEndianBuffer<kvbc::BlockId>(val.data.data());
}

void PruningHandler::persistLastAgreedPrunableBlockId(kvbc::BlockId block_id) const {
  concord::kvbc::categorization::VersionedUpdates ver_updates;
  ver_updates.addUpdate(std::string{last_agreed_prunable_block_id_key_},
                        concordUtils::toBigEndianStringBuffer(block_id));
  concord::kvbc::categorization::Updates updates;
  updates.add(kvbc::kConcordInternalCategoryId, std::move(ver_updates));
  try {
    blocks_adder_.add(std::move(updates));
  } catch (...) {
    throw std::runtime_error{"PruningHandler failed to persist last agreed prunable block ID"};
  }
}

void PruningHandler::pruneThroughBlockId(kvbc::BlockId block_id) const {
  const auto genesis_block_id = ro_storage_.getGenesisBlockId();
  if (block_id >= genesis_block_id) {
    bftEngine::ControlStateManager::instance().setPruningProcess(true);
    // last_scheduled_block_for_pruning_ is being updated only here, thus, once
    // we set the control_state_manager, no other write request will be executed
    // and we can set it without grabing the mutex
    last_scheduled_block_for_pruning_ = block_id;
    auto prune = [this](kvbc::BlockId until) {
      try {
        blocks_deleter_.deleteBlocksUntil(until);
      } catch (std::exception& e) {
        LOG_FATAL(logger_, e.what());
        std::terminate();
      } catch (...) {
        LOG_FATAL(logger_, "Error while running pruning");
        std::terminate();
      }
      // We grab a mutex to handle the case in which we ask for pruning status
      // concurrently
      std::lock_guard lock(pruning_status_lock_);
      bftEngine::ControlStateManager::instance().setPruningProcess(false);
    };
    if (run_async_) {
      LOG_INFO(logger_, "running pruning in async mode");
      async_pruning_res_ = std::async(prune, block_id + 1);
      (void)async_pruning_res_;
    } else {
      LOG_INFO(logger_, "running pruning in sync mode");
      prune(block_id + 1);
    }
  }
}

void PruningHandler::pruneThroughLastAgreedBlockId() const {
  const auto last_agreed = lastAgreedPrunableBlockId();
  if (last_agreed.has_value()) {
    pruneThroughBlockId(*last_agreed);
  }
}

void PruningHandler::pruneOnStateTransferCompletion(uint64_t checkpoint_number) const noexcept {
  try {
    pruneThroughLastAgreedBlockId();
  } catch (const std::exception& e) {
    LOG_FATAL(logger_,
              "PruningHandler stopping replica due to failure to prune blocks on "
              "state transfer completion, reason: "
                  << e.what());
    std::exit(-1);
  } catch (...) {
    LOG_FATAL(logger_,
              "PruningHandler stopping replica due to failure to prune blocks on "
              "state transfer completion");
    std::exit(-1);
  }
}

bool PruningHandler::handle(const concord::messages::PruneStatusRequest&,
                            concord::messages::PruneStatus& prune_status) {
  LOG_INFO(logger_, "Pruning status is " << KVLOG(prune_status.in_progress));
  std::lock_guard lock(pruning_status_lock_);
  prune_status.last_pruned_block =
      last_scheduled_block_for_pruning_.has_value() ? last_scheduled_block_for_pruning_.value() : 0;
  prune_status.in_progress = bftEngine::ControlStateManager::instance().getPruningProcessStatus();
  return true;
}
}  // namespace concord::kvbc::pruning
