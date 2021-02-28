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

#pragma once

#include "Logger.hpp"
#include "Replica.hpp"
#include "concord.cmf.hpp"
#include "ireconfiguration.hpp"
#include "OpenTracing.hpp"
#include "pruning_utils.hpp"
namespace concord::reconfiguration::pruning {
class PruningHandler : public IPruningHandler {
  // This class implements the KVB pruning state machine. Main functionalities
  // include executing pruning based on configuration policy and replica states as
  // well as providing read-only information to the operator node.
  //
  // The following configuration options are honored by the state machine:
  //  * pruning_enabled - a system-wide configuration option that indicates if
  //  pruning is enabled for the replcia. If set to false,
  //  LatestPrunableBlockRequest will return 0 as a latest block(indicating no
  //  blocks can be pruned) and PruneRequest will return an error. If not
  //  specified, a value of false is assumed.
  //
  //  * pruning_num_blocks_to_keep - a system-wide configuration option that
  //  specifies a minimum number of blocks to always keep in storage when pruning.
  //  If not specified, a value of 0 is assumed. If
  //  pruning_duration_to_keep_minutes is specified too, the more conservative
  //  pruning range of the two options will be used (the one that prunes less
  //  blocks).
  //
  //  * pruning_duration_to_keep_minutes - a system-wide configuration option that
  //  specifies a time range (in minutes) from now to the past that determines
  //  which blocks to keep and which are older than (now -
  //  pruning_duration_to_keep_minutes) and can, therefore, be pruned. If not
  //  specified, a value of 0 is assumed. If pruning_num_blocks_to_keep is
  //  specified too, the more conservative pruning range of the two will be used
  //  (the one that prunes less blocks). This option requires the time service to
  //  be enabled.
  //
  // The LatestPrunableBlockRequest command returns the latest block ID from the
  // replica's storage that is safe to prune. If no blocks can be pruned, 0 is
  // returned.
  //
  // The PruneRequest command prunes blocks from storage by:
  //  - verifying the PruneRequest message's signature
  //  - verifying that the number of LatestPrunableBlock messages in the
  //  PruneRequest is equal to the number of replicas in the system
  //  - verifying the signatures of individual LatestPrunableBlock messages in the
  //  PruneRequest.
  // If all above conditions are met, the state machine will prune blocks from the
  // genesis block up to the the minimum of all the block IDs in
  // LatestPrunableBlock messages in the PruneRequest message.
 public:
  // Construct by providing an interface to the storage engine, state transfer,
  // configuration and tracing facilities. Note this constructor may throw an
  // exception if there is an issue with the configuration (for example, if the
  // configuration enables pruning but does not provide a purning operator
  // public key).
  PruningHandler(const kvbc::IReader &,
                 kvbc::IBlockAdder &,
                 kvbc::IBlocksDeleter &,
                 bftEngine::IStateTransfer &,
                 bool run_async = false);
  bool handle(const concord::messages::LatestPrunableBlockRequest &, concord::messages::LatestPrunableBlock &) override;
  bool handle(const concord::messages::PruneRequest &, kvbc::BlockId &) override;
  bool handle(const concord::messages::PruneStatusRequest &, concord::messages::PruneStatus &) override;
  static std::string lastAgreedPrunableBlockIdKey() { return last_agreed_prunable_block_id_key_; }

 private:
  kvbc::BlockId latestBasedOnNumBlocksConfig() const;
  kvbc::BlockId latestBasedOnTimeRangeConfig() const;

  kvbc::BlockId agreedPrunableBlockId(const concord::messages::PruneRequest &) const;
  // Returns the last agreed prunable block ID from storage, if existing.
  std::optional<kvbc::BlockId> lastAgreedPrunableBlockId() const;
  void persistLastAgreedPrunableBlockId(kvbc::BlockId block_id) const;
  // Prune blocks in the [genesis, block_id] range (both inclusive).
  // Throws on errors.
  void pruneThroughBlockId(kvbc::BlockId block_id) const;
  void pruneThroughLastAgreedBlockId() const;
  void pruneOnStateTransferCompletion(uint64_t checkpoint_number) const noexcept;
  logging::Logger logger_;
  RSAPruningSigner signer_;
  RSAPruningVerifier verifier_;
  const kvbc::IReader &ro_storage_;
  kvbc::IBlockAdder &blocks_adder_;
  kvbc::IBlocksDeleter &blocks_deleter_;

  bool pruning_enabled_{false};
  std::uint64_t replica_id_{0};
  std::uint64_t num_blocks_to_keep_{0};
  std::uint32_t duration_to_keep_minutes_{0};
  bool run_async_{false};
  static const std::string last_agreed_prunable_block_id_key_;
  mutable std::optional<kvbc::BlockId> last_scheduled_block_for_pruning_;
  mutable std::mutex pruning_status_lock_;
  mutable std::future<void> async_pruning_res_;
};
}  // namespace concord::reconfiguration::pruning
