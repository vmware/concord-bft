// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
#pragma once

#include <chrono>
#include <random>
#include <unordered_map>
#include <vector>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <optional>
#include <atomic>

#include <boost/program_options.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>

#include "rocksdb/native_client.h"
#include "Logger.hpp"
#include "categorization/updates.h"
#include "blockchain_adapter.hpp"
#include "thread_pool.hpp"

class BookKeeper {
 public:
  BookKeeper()
      : logger_(logging::getLogger("concord.kvbc.bc.migration")),
        max_read_batch_id_(0u),
        max_write_batch_id_(0u),
        read_completed_(false) {}
  int migrate(int argc, char* argv[]);

 private:
  void initialize_migration();
  void migrate(const std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain>& from,
               std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain>& to);
  void read_from(const std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain>& from,
                 uint64_t batch_id,
                 uint64_t from_block_id,
                 uint64_t last_block_id);
  void store_to(std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain>& to);
  bool parse_args(int argc, char* argv[]);
  bool verify_config();

 private:
  logging::Logger logger_;

  boost::program_options::options_description config_desc_;
  boost::program_options::variables_map config_;

  std::shared_ptr<BlockchainAdapter<concord::kvbc::V4_BLOCKCHAIN>> v4_kvbc_;
  std::shared_ptr<BlockchainAdapter<concord::kvbc::CATEGORIZED_BLOCKCHAIN>> cat_kvbc_;

  std::mutex batch_mutex_;
  std::condition_variable batch_cond_var_;
  std::mutex read_batch_mutex_;
  std::condition_variable read_batch_cond_var_;
  std::shared_ptr<std::unordered_map<uint64_t, std::shared_ptr<std::vector<concord::kvbc::categorization::Updates>>>>
      single_batch_;
  uint64_t number_of_batches_allowed_in_cycle_ = 0;

  std::unique_ptr<concord::util::ThreadPool> batch_thread_pool_;
  std::unique_ptr<std::thread> storage_thread_;

  std::atomic<uint64_t> max_read_batch_id_;
  std::atomic<uint64_t> max_write_batch_id_;
  std::atomic<bool> read_completed_;
  uint64_t max_number_of_batches_ = 0;
};
