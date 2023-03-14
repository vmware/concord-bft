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

#include <string>

#include "db_interfaces.h"
#include "categorization/base_types.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "state_snapshot_interface.hpp"
#include "categorization/kv_blockchain.h"
#include "rocksdb/native_client.h"

namespace concord::kvbc::adapter::common::statesnapshot {
class KVBCStateSnapshot : public concord::kvbc::IKVBCStateSnapshot {
 public:
  explicit KVBCStateSnapshot(const concord::kvbc::IReader* reader,
                             const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client)
      : reader_{reader}, native_client_(native_client) {
    ConcordAssertNE(reader_, nullptr);
  }

  ////////////////////////////IKVBCStateSnapshot////////////////////////////////////////////////////////////////////////
  // Computes and persists the public state hash by:
  //  h0 = hash("")
  //  h1 = hash(h0 || hash(k1) || v1)
  //  h2 = hash(h1 || hash(k2) || v2)
  //  ...
  //  hN = hash(hN-1 || hash(kN) || vN)
  //
  // This method is supposed to be called on DB snapshots only and not on the actual blockchain.
  // Precondition: The current KeyValueBlockchain instance points to a DB snapshot.
  void computeAndPersistPublicStateHash(
      BlockId checkpoint_block_id,
      const Converter& value_converter = [](std::string&& s) -> std::string { return std::move(s); }) override final;

  // Returns the public state keys as of the current point in the blockchain's history.
  // Returns std::nullopt if no public keys have been persisted.
  std::optional<concord::kvbc::categorization::PublicStateKeys> getPublicStateKeys() const override final;

  // Iterate over all public key values, calling the given function multiple times with two parameters:
  // * key
  // * value
  void iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f) const override final;

  // Iterate over public key values from the key after `after_key`, calling the given function multiple times with two
  // parameters:
  // * key
  // * value
  //
  // If `after_key` is not a public key, false is returned and no iteration is done (no calls to `f`). Else, iteration
  // is done and the returned value is true, even if there are 0 public keys to actually iterate.
  bool iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f,
                                   const std::string& after_key) const override final;

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  virtual ~KVBCStateSnapshot() { reader_ = nullptr; }

 private:
  bool iteratePublicStateKeyValuesImpl(const std::function<void(std::string&&, std::string&&)>& f,
                                       const std::optional<std::string>& after_key) const;

 private:
  const concord::kvbc::IReader* reader_{nullptr};
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
};
}  // namespace concord::kvbc::adapter::common::statesnapshot
