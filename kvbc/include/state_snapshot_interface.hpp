// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once
#include <optional>
#include <functional>
#include <string>

#include "kv_types.hpp"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "blockchain_misc.hpp"

namespace concord::kvbc {

class IDBCheckpoint {
 public:
  // Trims the DB snapshot such that its last reachable block is equal to `block_id_at_checkpoint`.
  // This method is supposed to be called on DB snapshots only and not on the actual blockchain.
  // Precondition1: The current KeyValueBlockchain instance points to a DB snapshot.
  // Precondition2: `block_id_at_checkpoint` >= INITIAL_GENESIS_BLOCK_ID
  // Precondition3: `block_id_at_checkpoint` <= getLastReachableBlockId()
  virtual void trimBlocksFromCheckpoint(BlockId block_id_at_checkpoint) = 0;

  // Notify storage the start and end of the checkpoint process
  virtual void checkpointInProcess(bool, kvbc::BlockId) = 0;

  virtual ~IDBCheckpoint() = default;
};

// State snapshot support.
class IKVBCStateSnapshot {
 public:
  // Computes and persists the public state hash by:
  //  h0 = hash("")
  //  h1 = hash(h0 || hash(k1) || v1)
  //  h2 = hash(h1 || hash(k2) || v2)
  //  ...
  //  hN = hash(hN-1 || hash(kN) || vN)
  //
  // This method is supposed to be called on DB snapshots only and not on the actual blockchain.
  // Precondition: The current KeyValueBlockchain instance points to a DB snapshot.
  virtual void computeAndPersistPublicStateHash(BlockId checkpoint_block_id, const Converter& value_converter) = 0;

  // Returns the public state keys as of the current point in the blockchain's history.
  // Returns std::nullopt if no public keys have been persisted.
  virtual std::optional<concord::kvbc::categorization::PublicStateKeys> getPublicStateKeys() const = 0;

  // Iterate over all public key values, calling the given function multiple times with two parameters:
  // * key
  // * value
  virtual void iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f) const = 0;

  // Iterate over public key values from the key after `after_key`, calling the given function multiple times with two
  // parameters:
  // * key
  // * value
  //
  // If `after_key` is not a public key, false is returned and no iteration is done (no calls to `f`). Else, iteration
  // is done and the returned value is true, even if there are 0 public keys to actually iterate.
  virtual bool iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f,
                                           const std::string& after_key) const = 0;

  virtual ~IKVBCStateSnapshot() = default;
};

}  // namespace concord::kvbc