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
#include <vector>
#include <memory>

#include "blockchain_misc.hpp"
#include "kv_types.hpp"
#include "categorization/base_types.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "db_interfaces.h"
#include "categorization/kv_blockchain.h"
#include "kvbc_adapter/replica_adapter_auxilliary_types.hpp"

namespace concord::kvbc::adapter::categorization {

class BlocksDeleterAdapter : public concord::kvbc::IBlocksDeleter {
 public:
  virtual ~BlocksDeleterAdapter() { kvbc_ = nullptr; }
  explicit BlocksDeleterAdapter(std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain> &kvbc,
                                const std::optional<aux::AdapterAuxTypes> &aux_types = std::nullopt);

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlocksDeleter implementation
  void deleteGenesisBlock() override final;
  BlockId deleteBlocksUntil(BlockId until, bool delete_files_in_range) override final;
  void deleteLastReachableBlock() override final { return kvbc_->deleteLastReachableBlock(); }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

 private:
  concord::kvbc::categorization::KeyValueBlockchain *kvbc_{nullptr};

  struct Recorders {
    static constexpr uint64_t MAX_VALUE_MICROSECONDS = 2ULL * 1000ULL * 1000ULL;  // 2 seconds
    const std::string component_ = "iblockdeleter";

    Recorders() {
      auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.registerComponent(component_, {delete_batch_blocks_duration});
    }

    ~Recorders() {}

    DEFINE_SHARED_RECORDER(
        delete_batch_blocks_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  };
  Recorders histograms_;
};

}  // namespace concord::kvbc::adapter::categorization