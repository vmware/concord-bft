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
#include "kvbc_key_types.hpp"

namespace concord::kvbc::pruning {

void RSAPruningSigner::sign(concord::messages::LatestPrunableBlock& block) {
  std::ostringstream oss;
  std::string ser;
  oss << block.replica << block.block_id;
  ser = oss.str();
  std::string signature = signer_->sign(ser);
  block.signature = std::vector<uint8_t>(signature.begin(), signature.end());
}

RSAPruningSigner::RSAPruningSigner(const string& key)
    : signer_{std::make_unique<concord::util::crypto::RSASigner>(
          key, concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat)} {}

RSAPruningVerifier::RSAPruningVerifier(const std::set<std::pair<uint16_t, const std::string>>& replicasPublicKeys) {
  auto i = 0u;
  for (auto& [idx, pkey] : replicasPublicKeys) {
    replicas_.push_back(Replica{idx,
                                std::make_unique<concord::util::crypto::RSAVerifier>(
                                    pkey, concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat)});
    const auto ins_res = replica_ids_.insert(replicas_.back().principal_id);
    if (!ins_res.second) {
      throw std::runtime_error{"RSAPruningVerifier found duplicate replica principal_id: " +
                               std::to_string(replicas_.back().principal_id)};
    }

    const auto& replica = replicas_.back();
    principal_to_replica_idx_[replica.principal_id] = i;
    i++;
  }
}

bool RSAPruningVerifier::verify(const concord::messages::LatestPrunableBlock& block) const {
  // LatestPrunableBlock can only be sent by replicas and not by client proxies.
  if (replica_ids_.find(block.replica) == std::end(replica_ids_)) {
    return false;
  }
  std::ostringstream oss;
  std::string ser;
  oss << block.replica << block.block_id;
  ser = oss.str();
  std::string sig_str(block.signature.begin(), block.signature.end());
  return verify(block.replica, ser, sig_str);
}

bool RSAPruningVerifier::verify(const concord::messages::PruneRequest& request) const {
  if (request.latest_prunable_block.size() != static_cast<size_t>(replica_ids_.size())) {
    return false;
  }

  // PruneRequest can only be sent by client proxies and not by replicas.
  if (replica_ids_.find(request.sender) != std::end(replica_ids_)) {
    return false;
  }

  // Make sure pruning parameters are in range.
  if (request.tick_period_seconds <= 0 || request.batch_blocks_num <= 0) {
    return false;
  }

  // Note RSAPruningVerifier does not handle verification of the operator's
  // signature authorizing this pruning order, as the operator's signature is a
  // dedicated application-level signature rather than one of the Concord-BFT
  // principals' RSA signatures.

  // Verify that *all* replicas have responded with valid responses.
  auto replica_ids_to_verify = replica_ids_;
  for (auto& block : request.latest_prunable_block) {
    if (!verify(block)) {
      return false;
    }
    auto it = replica_ids_to_verify.find(block.replica);
    if (it == std::end(replica_ids_to_verify)) {
      return false;
    }
    replica_ids_to_verify.erase(it);
  }
  return replica_ids_to_verify.empty();
}

bool RSAPruningVerifier::verify(std::uint64_t sender, const std::string& ser, const std::string& signature) const {
  auto it = principal_to_replica_idx_.find(sender);
  if (it == std::cend(principal_to_replica_idx_)) {
    return false;
  }

  return getReplica(it->second).verifier->verify(ser, signature);
}

const RSAPruningVerifier::Replica& RSAPruningVerifier::getReplica(ReplicaVector::size_type idx) const {
  return replicas_[idx];
}

PruningHandler::PruningHandler(kvbc::IReader& ro_storage,
                               kvbc::IBlockAdder& blocks_adder,
                               kvbc::IBlocksDeleter& blocks_deleter,
                               bool run_async)
    : logger_{logging::getLogger("concord.pruning")},
      signer_{bftEngine::ReplicaConfig::instance().replicaPrivateKey},
      verifier_{bftEngine::ReplicaConfig::instance().publicKeysOfReplicas},
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
    LOG_WARN(logger_, error);
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

bool PruningHandler::handle(const concord::messages::PruneStatusRequest&,
                            uint64_t,
                            uint32_t,
                            concord::messages::ReconfigurationResponse& rres) {
  if (!pruning_enabled_) return true;
  concord::messages::PruneStatus prune_status;
  std::lock_guard lock(pruning_status_lock_);
  prune_status.last_pruned_block =
      last_scheduled_block_for_pruning_.has_value() ? last_scheduled_block_for_pruning_.value() : 0;
  prune_status.in_progress = bftEngine::ControlStateManager::instance().getPruningProcessStatus();
  rres.response = prune_status;
  LOG_INFO(logger_, "Pruning status is " << KVLOG(prune_status.in_progress));
  return true;
}

uint64_t PruningHandler::getBlockBftSequenceNumber(kvbc::BlockId bid) const {
  auto opt_value =
      ro_storage_.get(concord::kvbc::kConcordInternalCategoryId, std::string{kvbc::keyTypes::bft_seq_num_key}, bid);
  uint64_t sequenceNum = 0;
  if (!opt_value) {
    LOG_WARN(logger_, "Unable to get block");
    return sequenceNum;
  }
  auto value = std::get<concord::kvbc::categorization::VersionedValue>(*opt_value);
  if (value.data.empty()) {
    LOG_WARN(logger_, "value has zero-length");
    return sequenceNum;
  }
  sequenceNum = concordUtils::fromBigEndianBuffer<std::uint64_t>(value.data.data());
  LOG_DEBUG(logger_, "sequenceNum = " << sequenceNum);
  return sequenceNum;
}
}  // namespace concord::kvbc::pruning
