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
#include "db_interfaces.h"
#include "reconfiguration/ireconfiguration.hpp"
#include "crypto_utils.hpp"
#include "block_metadata.hpp"
#include "kvbc_key_types.hpp"
#include <future>
#include "reconfiguration/reconfiguration_handler.hpp"

namespace concord::kvbc::pruning {

// This class signs pruning messages via the replica's private key that it gets
// through the configuration. Message contents used to generate the signature
// are generated via the mechanisms provided in pruning_serialization.hpp/cpp .
class RSAPruningSigner {
 public:
  // Construct by passing the configuration for the node the signer is running
  // on.
  RSAPruningSigner(const std::string &key);
  // Sign() methods sign the passed message and store the signature in the
  // 'signature' field of the message. An exception is thrown on error.
  //
  // Note RSAPruningSigner does not handle signing of PruneRequest messages on
  // behalf of the operator, as the operator's signature is a dedicated-purpose
  // application-level signature rather than a Concord-BFT Principal's RSA
  // signature.
  void sign(concord::messages::LatestPrunableBlock &);

 private:
  std::unique_ptr<concord::util::crypto::ISigner> signer_;
};

// This class verifies pruning messages that were signed by serializing message
// contents via mechanisms provided in pruning_serialization.hpp/cpp . Public
// keys for verification are extracted from the passed configuration.
//
// Idea is to use the principal_id as an ID that identifies senders in pruning
// messages since it is unique across clients and replicas.
class RSAPruningVerifier {
 public:
  // Construct by passing the system configuration.
  RSAPruningVerifier(const std::set<std::pair<uint16_t, const std::string>> &replicasPublicKeys);
  // Verify() methods verify that the message comes from the advertised sender.
  // Methods return true on successful verification and false on unsuccessful.
  // An exception is thrown on error.
  //
  // Note RSAPruningVerifier::Verify(const com::vmware::concord::PruneRequest&)
  // handles verification of the LatestPrunableBlock message(s) contained within
  // the PruneRequest, but does not itself handle verification of the issuing
  // operator's signature of the pruning command, as the operator's signature is
  // a dedicated application-level signature rather than one of the Concord-BFT
  // Principal's RSA signatures.
  bool verify(const concord::messages::LatestPrunableBlock &) const;
  bool verify(const concord::messages::PruneRequest &) const;

 private:
  struct Replica {
    std::uint64_t principal_id{0};
    std::unique_ptr<concord::util::crypto::IVerifier> verifier;
  };

  bool verify(std::uint64_t sender, const std::string &ser, const std::string &signature) const;

  using ReplicaVector = std::vector<Replica>;

  // Get a replica from the replicas vector by its index.
  const Replica &getReplica(ReplicaVector::size_type idx) const;

  // A vector of all the replicas in the system.
  ReplicaVector replicas_;
  // We map a principal_id to a replica index in the replicas_ vector to be able
  // to verify a message through the Replica's verifier that is associated with
  // its public key.
  std::unordered_map<std::uint64_t, ReplicaVector::size_type> principal_to_replica_idx_;

  // Contains a set of replica principal_ids for use in verification. Filled in
  // once during construction.
  std::unordered_set<std::uint64_t> replica_ids_;
};
class PruningHandler : public concord::reconfiguration::BftReconfigurationHandler {
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
  //  - verifying other parameters in PruneRequest
  // If all above conditions are met, the state machine will prune blocks from the
  // genesis block up to the the minimum of all the block IDs in
  // LatestPrunableBlock messages in the PruneRequest message.
 public:
  // Construct by providing an interface to the storage engine, state transfer,
  // configuration and tracing facilities. Note this constructor may throw an
  // exception if there is an issue with the configuration (for example, if the
  // configuration enables pruning but does not provide a purning operator
  // public key).
  PruningHandler(kvbc::IReader &, kvbc::IBlockAdder &, kvbc::IBlocksDeleter &, bool run_async = false);
  bool handle(const concord::messages::LatestPrunableBlockRequest &,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse &) override;
  bool handle(const concord::messages::PruneRequest &,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse &) override;
  bool handle(const concord::messages::PruneStatusRequest &,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse &) override;

 protected:
  kvbc::BlockId latestBasedOnNumBlocksConfig() const;
  kvbc::BlockId agreedPrunableBlockId(const concord::messages::PruneRequest &) const;

  // Prune blocks in the [genesis, block_id] range (both inclusive).
  // Throws on errors.
  void pruneThroughBlockId(kvbc::BlockId block_id) const;
  uint64_t getBlockBftSequenceNumber(kvbc::BlockId) const;
  logging::Logger logger_;
  RSAPruningSigner signer_;
  RSAPruningVerifier verifier_;
  kvbc::IReader &ro_storage_;
  kvbc::IBlockAdder &blocks_adder_;
  kvbc::IBlocksDeleter &blocks_deleter_;
  BlockMetadata block_metadata_;

  bool pruning_enabled_{false};
  std::uint64_t replica_id_{0};
  std::uint64_t num_blocks_to_keep_{0};
  bool run_async_{false};
  mutable std::optional<kvbc::BlockId> last_scheduled_block_for_pruning_;
  mutable std::mutex pruning_status_lock_;
  mutable std::future<void> async_pruning_res_;
};

/*
 * The read only pruning handler knows to answer only getLatestPruneableBlock.
 * As it doesn't participant the consensus, the only thing we want to know is what the latest reachable block is.
 */
class ReadOnlyReplicaPruningHandler : public concord::reconfiguration::BftReconfigurationHandler {
 public:
  ReadOnlyReplicaPruningHandler(IReader &ro_storage)
      : ro_storage_{ro_storage},
        signer_{bftEngine::ReplicaConfig::instance().replicaPrivateKey},
        pruning_enabled_{bftEngine::ReplicaConfig::instance().pruningEnabled_},
        replica_id_{bftEngine::ReplicaConfig::instance().replicaId} {}
  bool handle(const concord::messages::LatestPrunableBlockRequest &,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse &rres) override {
    if (!pruning_enabled_) return true;
    concord::messages::LatestPrunableBlock latest_prunable_block;
    const auto latest_prunable_block_id = pruning_enabled_ ? ro_storage_.getLastBlockId() : 0;
    latest_prunable_block.bft_sequence_number = 0;  // Read only replica doesn't know the block sequence number
    latest_prunable_block.replica = replica_id_;
    latest_prunable_block.block_id = latest_prunable_block_id;
    signer_.sign(latest_prunable_block);
    LOG_INFO(GL,
             KVLOG(latest_prunable_block.bft_sequence_number,
                   latest_prunable_block.replica = replica_id_,
                   latest_prunable_block.block_id));
    rres.response = latest_prunable_block;
    return true;
  }
  // Read only replica doesn't perform the actual pruning
  bool handle(const concord::messages::PruneRequest &,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse &) override {
    return true;
  }
  // Read only replica doesn't know the pruning status
  bool handle(const concord::messages::PruneStatusRequest &,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse &) override {
    return true;
  }

 private:
  IReader &ro_storage_;
  RSAPruningSigner signer_;
  bool pruning_enabled_{false};
  std::uint64_t replica_id_{0};
};
}  // namespace concord::kvbc::pruning
