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

#include "util/endianness.hpp"
#include <future>
#include "bftengine/ControlStateManager.hpp"
#include "pruning_handler.hpp"
#include "categorization/versioned_kv_category.h"
#include "kvbc_key_types.hpp"
#include "SigManager.hpp"

namespace concord::kvbc::pruning {

using bftEngine::ReplicaConfig;

void PruningSigner::sign(concord::messages::LatestPrunableBlock& block) {
  auto& sigManager = *bftEngine::impl::SigManager::instance();
  std::ostringstream oss;
  std::string ser;
  oss << block.replica << block.block_id;
  ser = oss.str();
  std::vector<uint8_t> signature(sigManager.getMySigLength());
  sigManager.sign(
      sigManager.getReplicaLastExecutedSeq(), ser.data(), ser.size(), reinterpret_cast<char*>(signature.data()));

  block.signature = std::move(signature);
}

PruningSigner::PruningSigner() {}

PruningVerifier::PruningVerifier() {}

bool PruningVerifier::verify(const concord::messages::LatestPrunableBlock& block) const {
  // LatestPrunableBlock can only be sent by replicas and not by client proxies.
  if (!SigManager::instance()->hasVerifier(block.replica)) {
    return false;
  }
  std::ostringstream oss;
  std::string ser;
  oss << block.replica << block.block_id;
  ser = oss.str();
  std::string sig_str(block.signature.begin(), block.signature.end());
  return verify(block.replica, ser, sig_str);
}

bool PruningVerifier::verify(const concord::messages::PruneRequest& request) const {
  const auto& repInfo = SigManager::instance()->getReplicasInfo();
  const auto expected_response_count = repInfo.getNumberOfReplicas() + repInfo.getNumberOfRoReplicas();
  if (request.latest_prunable_block.size() != static_cast<size_t>(expected_response_count)) {
    LOG_ERROR(
        PRUNING_LOG,
        "Invalid number of latest prunable block responses in prune request" << KVLOG(
            request.latest_prunable_block.size(), repInfo.getNumberOfReplicas(), repInfo.getNumberOfRoReplicas()));
    return false;
  }

  // PruneRequest can only be sent by client proxies and not by replicas.
  if (repInfo.isIdOfReplica(request.sender)) {
    LOG_ERROR(PRUNING_LOG, "Invalid sender of prune request, sender id:" << request.sender);
    return false;
  }

  // Note PruningVerifier does not handle verification of the operator's
  // signature authorizing this pruning order, as the operator's signature is a
  // dedicated application-level signature rather than one of the Concord-BFT
  // principals' main signatures.

  // Verify that *all* replicas have responded with valid responses.
  for (auto& block : request.latest_prunable_block) {
    auto currentId = block.replica;
    if (!verify(block)) {
      LOG_ERROR(PRUNING_LOG, "Failed to verify latest prunable block of replica id:" << currentId);
      return false;
    }

    if (!(repInfo.isIdOfReplica(currentId) || repInfo.isIdOfPeerRoReplica(currentId))) {
      LOG_ERROR(PRUNING_LOG, "latest prunable block is not of a replica nor an RO replica:" << currentId);
      return false;
    }
  }

  return true;
}

bool PruningVerifier::verify(std::uint64_t sender, const std::string& ser, const std::string& signature) const {
  if (!SigManager::instance()->hasVerifier(sender)) {
    return false;
  }

  auto& sigManager = *bftEngine::impl::SigManager::instance();
  return sigManager.verifySig(sender, ser, signature);
}

PruningHandler::PruningHandler(const std::string& operator_pkey_path,
                               concord::crypto::SignatureAlgorithm type,
                               kvbc::IReader& ro_storage,
                               kvbc::IBlockAdder& blocks_adder,
                               kvbc::IBlocksDeleter& blocks_deleter,
                               bool run_async)
    : concord::reconfiguration::OperatorCommandsReconfigurationHandler{operator_pkey_path, type},
      signer_{},
      verifier_{},
      ro_storage_{ro_storage},
      blocks_adder_{blocks_adder},
      blocks_deleter_{blocks_deleter},
      block_metadata_{ro_storage},
      replica_id_{bftEngine::ReplicaConfig::instance().replicaId},
      run_async_{run_async} {
  pruning_enabled_ = bftEngine::ReplicaConfig::instance().pruningEnabled_;
  num_blocks_to_keep_ = bftEngine::ReplicaConfig::instance().numBlocksToKeep_;
}

bool PruningHandler::handle(const concord::messages::LatestPrunableBlockRequest& latest_prunable_block_request,
                            uint64_t,
                            uint32_t,
                            const std::optional<bftEngine::Timestamp>&,
                            concord::messages::ReconfigurationResponse& rres) {
  // If pruning is disabled, return 0. Otherwise, be conservative and prune the
  // smaller block range.
  if (!pruning_enabled_) {
    return true;
  }
  std::lock_guard lock(pruning_status_lock_);
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) {
    concord::messages::ReconfigurationErrorMsg error_msg;
    error_msg.error_msg = "latestPruneableBlock can't retrieved while pruning is going on";
    rres.response = error_msg;
    return false;
  }
  concord::messages::LatestPrunableBlock latest_prunable_block;
  const auto latest_prunable_block_id = pruning_enabled_ ? latestBasedOnNumBlocksConfig() : 0;
  if (latest_prunable_block_id > 1)
    latest_prunable_block.bft_sequence_number = getBlockBftSequenceNumber(latest_prunable_block_id);
  latest_prunable_block.replica = replica_id_;
  latest_prunable_block.block_id = latest_prunable_block_id;
  signer_.sign(latest_prunable_block);
  rres.response = latest_prunable_block;
  return true;
}

bool PruningHandler::handle(const concord::messages::PruneRequest& request,
                            uint64_t bftSeqNum,
                            uint32_t,
                            const std::optional<bftEngine::Timestamp>&,
                            concord::messages::ReconfigurationResponse& rres) {
  if (!pruning_enabled_) return true;

  const auto sender = request.sender;

  if (!verifier_.verify(request)) {
    auto error = "PruningHandler failed to verify PruneRequest from principal_id " + std::to_string(sender) +
                 " on the grounds that the pruning request did not include "
                 "LatestPrunableBlock responses from the required replicas, or "
                 "on the grounds that some non-empty subset of those "
                 "LatestPrunableBlock messages did not bear correct signatures "
                 "from the claimed replicas.";
    concord::messages::ReconfigurationErrorMsg error_msg;
    LOG_WARN(PRUNING_LOG, error);
    error_msg.error_msg = error;
    rres.response = error_msg;
    return false;
  }

  const auto latest_prunable_block_id = agreedPrunableBlockId(request);

  // Execute actual pruning.
  pruneThroughBlockId(latest_prunable_block_id);
  std::ostringstream oss;
  oss << std::to_string(latest_prunable_block_id);
  std::string str = oss.str();
  std::copy(str.cbegin(), str.cend(), std::back_inserter((rres).additional_data));
  return true;
}

kvbc::BlockId PruningHandler::latestBasedOnNumBlocksConfig() const {
  const auto last_block_id = ro_storage_.getLastBlockId();
  if (last_block_id < num_blocks_to_keep_) {
    return 0;
  }
  return last_block_id - num_blocks_to_keep_;
}

kvbc::BlockId PruningHandler::agreedPrunableBlockId(const concord::messages::PruneRequest& prune_request) const {
  const auto latest_prunable_blocks = prune_request.latest_prunable_block;
  const auto begin = std::cbegin(latest_prunable_blocks);
  const auto end = std::cend(latest_prunable_blocks);
  ConcordAssertNE(begin, end);
  return std::min_element(begin, end, [](const auto& a, const auto& b) { return (a.block_id < b.block_id); })->block_id;
}

void PruningHandler::pruneThroughBlockId(kvbc::BlockId block_id) const {
  const auto genesis_block_id = ro_storage_.getGenesisBlockId();
  if (block_id >= genesis_block_id) {
    bftEngine::ControlStateManager::instance().setPruningProcess(true);
    auto prune = [this](kvbc::BlockId until) {
      try {
        blocks_deleter_.deleteBlocksUntil(until, false);
      } catch (std::exception& e) {
        LOG_FATAL(PRUNING_LOG, e.what());
        std::terminate();
      } catch (...) {
        LOG_FATAL(PRUNING_LOG, "Error while running pruning");
        std::terminate();
      }
      // We grab a mutex to handle the case in which we ask for pruning status
      // concurrently
      std::lock_guard lock(pruning_status_lock_);
      bftEngine::ControlStateManager::instance().setPruningProcess(false);
    };
    if (run_async_) {
      LOG_INFO(PRUNING_LOG, "running pruning in async mode");
      async_pruning_res_ = std::async(prune, block_id + 1);
      (void)async_pruning_res_;
    } else {
      LOG_INFO(PRUNING_LOG, "running pruning in sync mode");
      prune(block_id + 1);
    }
  }
}

bool PruningHandler::handle(const concord::messages::PruneStatusRequest&,
                            uint64_t,
                            uint32_t,
                            const std::optional<bftEngine::Timestamp>&,
                            concord::messages::ReconfigurationResponse& rres) {
  if (!pruning_enabled_) return true;
  if (!std::holds_alternative<concord::messages::PruneStatus>(rres.response)) {
    rres.response = concord::messages::PruneStatus{};
  }
  concord::messages::PruneStatus& prune_status = std::get<concord::messages::PruneStatus>(rres.response);
  std::lock_guard lock(pruning_status_lock_);
  const auto genesis_id = ro_storage_.getGenesisBlockId();
  prune_status.last_pruned_block = (genesis_id > INITIAL_GENESIS_BLOCK_ID ? genesis_id - 1 : 0);
  prune_status.in_progress = bftEngine::ControlStateManager::instance().getPruningProcessStatus();
  rres.response = prune_status;
  LOG_INFO(PRUNING_LOG, "Pruning status is " << KVLOG(prune_status.in_progress));
  return true;
}

uint64_t PruningHandler::getBlockBftSequenceNumber(kvbc::BlockId bid) const {
  auto opt_value = ro_storage_.get(
      concord::kvbc::categorization::kConcordInternalCategoryId, std::string{kvbc::keyTypes::bft_seq_num_key}, bid);
  uint64_t sequenceNum = 0;
  if (!opt_value) {
    LOG_WARN(PRUNING_LOG, "Unable to get block");
    return sequenceNum;
  }
  auto value = std::get<concord::kvbc::categorization::VersionedValue>(*opt_value);
  if (value.data.empty()) {
    LOG_WARN(PRUNING_LOG, "value has zero-length");
    return sequenceNum;
  }
  sequenceNum = concordUtils::fromBigEndianBuffer<std::uint64_t>(value.data.data());
  LOG_DEBUG(PRUNING_LOG, "sequenceNum = " << sequenceNum);
  return sequenceNum;
}
}  // namespace concord::kvbc::pruning
