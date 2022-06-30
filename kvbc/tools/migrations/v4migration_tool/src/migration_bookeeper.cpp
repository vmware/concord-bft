// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <pthread.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

#include "migration_bookeeper.hpp"

using namespace boost::program_options;
using namespace concord::storage::rocksdb;
using namespace concord::util;

int BookKeeper::migrate(int argc, char* argv[]) {
  if (!parse_args(argc, argv)) {
    return EXIT_FAILURE;
  }

  initialize_migration();

  LOG_INFO(logger_, "Starting the migration");
  if (config_["migrate-to-v4"].as<bool>()) {
    migrate(cat_kvbc_->getAdapter(), v4_kvbc_->getAdapter());
  } else {
    migrate(v4_kvbc_->getAdapter(), cat_kvbc_->getAdapter());
  }

  LOG_INFO(logger_, "Successfully completed Migration, thanks for your time !!!");
  return EXIT_SUCCESS;
}

void BookKeeper::initialize_migration() {
  number_of_batches_allowed_in_cycle_ = config_["max-point-lookup-batches"].as<std::uint64_t>();
  const auto read_only = true;
  const auto link_st_chain = true;
  const auto migrate_to_v4 = config_["migrate-to-v4"].as<bool>();

  auto rodb = NativeClient::newClient(
      config_["input-rocksdb-path"].as<std::string>(), read_only, NativeClient::DefaultOptions{});
  auto wodb = NativeClient::newClient(
      config_["output-rocksdb-path"].as<std::string>(), !read_only, NativeClient::DefaultOptions{});
  v4_kvbc_ = std::make_shared<BlockchainAdapter<concord::kvbc::V4_BLOCKCHAIN>>(
      migrate_to_v4 ? wodb : rodb, migrate_to_v4 ? link_st_chain : !link_st_chain);
  cat_kvbc_ = std::make_shared<BlockchainAdapter<concord::kvbc::CATEGORIZED_BLOCKCHAIN>>(
      migrate_to_v4 ? rodb : wodb, migrate_to_v4 ? !link_st_chain : link_st_chain);
  single_batch_ = std::make_shared<decltype(single_batch_)::element_type>();
  storage_thread_ = std::make_unique<std::thread>([this, migrate_to_v4]() {
    if (migrate_to_v4) {
      store_to(v4_kvbc_->getAdapter());
    } else {
      store_to(cat_kvbc_->getAdapter());
    }
  });
  if (std::thread::hardware_concurrency() > 2) {
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(1, &cpu_set);
    int rc = pthread_setaffinity_np(storage_thread_->native_handle(), sizeof(cpu_set_t), &cpu_set);
    if (rc != 0) {
      LOG_WARN(logger_, "Cannot create a CPU affinity for storage thread, so this process might run slow.");
    }
  }
  const auto point_lookup_threads = config_["point-lookup-threads"].as<std::uint64_t>();
  batch_thread_pool_ = std::make_unique<ThreadPool>(static_cast<std::uint32_t>(point_lookup_threads));
}

void BookKeeper::migrate(const std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain>& from,
                         std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain>& to) {
  auto start_block_id_ = from->getGenesisBlockId();
  auto last_block_id_ = from->getLastBlockId();
  max_read_batch_id_ = 1;
  std::vector<std::future<void>> tasks;
  auto point_lookup_batch_size = config_["point-lookup-batch-size"].as<std::uint64_t>();
  max_number_of_batches_ = (last_block_id_ - start_block_id_) / point_lookup_batch_size;
  if (((last_block_id_ - start_block_id_) % point_lookup_batch_size) != 0) {
    max_number_of_batches_++;
  }

  for (uint64_t block_id = start_block_id_; block_id <= last_block_id_;) {
    auto last_block_id_for_this_batch =
        ((block_id + point_lookup_batch_size) <= last_block_id_) ? block_id + point_lookup_batch_size : last_block_id_;
    tasks.push_back(batch_thread_pool_->async(
        [this, &from](auto l_batch_id, auto l_from_block_id, auto l_last_block_id) {
          read_from(from, l_batch_id, l_from_block_id, l_last_block_id);
        },
        max_read_batch_id_.load(),
        block_id,
        last_block_id_for_this_batch));
    if (tasks.size() >= number_of_batches_allowed_in_cycle_) {
      for (auto const& t : tasks) {
        t.wait();
      }
      tasks.clear();
      if (max_read_batch_id_.load() > max_write_batch_id_.load()) {
        std::unique_lock<std::mutex> lock(read_batch_mutex_);
        bool wait_for_batch_size_reduction = true;
        while (wait_for_batch_size_reduction) {
          wait_for_batch_size_reduction = !(read_batch_cond_var_.wait_for(lock, std::chrono::seconds(2), [this]() {
            return max_read_batch_id_.load() <= max_write_batch_id_.load();
          }));
        }
      }
    }
    ++max_read_batch_id_;
    block_id = last_block_id_for_this_batch + 1;
  }

  for (auto const& t : tasks) {
    t.wait();
  }
  read_completed_.store(true);
  if (storage_thread_->joinable()) {
    storage_thread_->join();
  }
}

void BookKeeper::read_from(const std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain>& from,
                           uint64_t batch_id,
                           uint64_t from_block_id,
                           uint64_t last_block_id) {
  auto batch = std::make_shared<std::vector<concord::kvbc::categorization::Updates>>();
  for (uint64_t block_id = from_block_id; block_id <= last_block_id; ++block_id) {
    batch->push_back((from->getBlockUpdates(block_id)).value());
  }
  {
    std::unique_lock<std::mutex> lock(batch_mutex_);
    ConcordAssertNE(single_batch_, nullptr);
    single_batch_->emplace(batch_id, batch);
    batch_cond_var_.notify_all();
  }
}

void BookKeeper::store_to(std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain>& to) {
  max_write_batch_id_ = 1;
  int completed_percent = -1;
  while (true) {
    bool wait_pred_result = false;
    {
      std::unique_lock<std::mutex> lock(batch_mutex_);
      auto max_batch_id = max_read_batch_id_.load();
      if ((max_batch_id != 0) && read_completed_.load() && (max_write_batch_id_.load() >= max_batch_id)) {
        return;
      }
      wait_pred_result = batch_cond_var_.wait_for(
          lock, std::chrono::seconds(1), [this]() { return max_write_batch_id_.load() <= max_read_batch_id_.load(); });

      max_batch_id = max_read_batch_id_.load();
      if ((max_batch_id != 0) && read_completed_.load() && (max_write_batch_id_.load() >= max_batch_id)) {
        return;
      }
    }

    if (wait_pred_result) {
      std::unique_lock<std::mutex> lock(batch_mutex_);
      auto curr_batch_id = max_write_batch_id_.load();
      ConcordAssertNE(single_batch_, nullptr);
      if (single_batch_->count(curr_batch_id) > 0) {
        auto prev_completed_percent = completed_percent;
        completed_percent = static_cast<int>(
            (static_cast<double>(curr_batch_id) / static_cast<double>(max_number_of_batches_)) * 100.0);
        if (prev_completed_percent != completed_percent) {
          LOG_INFO(logger_,
                   "Migration is approximately completed to " << completed_percent
                                                              << "%. Please note this progress is indicative."
                                                              << KVLOG(curr_batch_id, max_number_of_batches_));
        }
        auto p_updates_vec = (*single_batch_)[curr_batch_id];
        lock.unlock();
        for (auto update : *p_updates_vec) {
          // Todo: Add active keys to latest_keys cf and then do the add to the to BC.
          to->add(std::move(update));
        }
        p_updates_vec.reset();
        lock.lock();
        single_batch_->erase(curr_batch_id);
        ++max_write_batch_id_;
        if (single_batch_->size() == 0) {
          single_batch_.reset();
          single_batch_ = std::make_shared<decltype(single_batch_)::element_type>();
          read_batch_cond_var_.notify_all();
        }
      }
    } else {
      LOG_WARN(logger_,
               "Writing to DB is not happening for "
                   << KVLOG(max_write_batch_id_.load(), wait_pred_result, completed_percent));
    }
  }
}

bool BookKeeper::parse_args(int argc, char* argv[]) {
  const auto system_threads =
      unsigned{std::thread::hardware_concurrency() > 0 ? std::thread::hardware_concurrency() : 1};

  // clang-format off
  config_desc_.add_options()
  ("help", "Show usage.")
  ("input-rocksdb-path",
   value<std::string>(),
   "The path to the RocksDB data directory from where we need to read.")
  ("output-rocksdb-path",
   value<std::string>(),
   "The path to the RocksDB data directory to which migrated DB will go. Please note that checkpoints will not be migrated")
  ("point-lookup-batch-size",
   value<std::uint64_t>()->default_value(1000),
   "The number of keys to accumulate and then read via RocksDB MultiGet(). Will be rounded if needed.")
  ("max-point-lookup-batches",
   value<std::uint64_t>()->default_value(50),
   "The number of keys to accumulate and then read via RocksDB MultiGet(). Will be rounded if needed.")
  ("point-lookup-threads",
   value<std::uint64_t>()->default_value(system_threads),
   "Number of threads that execute point lookups in parallel.")
  ("migrate-to-v4",
   value<bool>()->default_value(true),
   "This tells whether we want to migrate a categorized blockchain to v4 blockchain. If its false then reverse will happen");
  // clang-format on

  store(parse_command_line(argc, argv, config_desc_), config_);
  notify(config_);
  if (config_.count("help")) {
    LOG_INFO(logger_, "\n" << config_desc_);
    return false;
  }
  return verify_config();
}

bool BookKeeper::verify_config() {
  if (config_["input-rocksdb-path"].empty()) {
    LOG_INFO(logger_,
             "--input-rocksdb-path is missing. Please read the help below and pass the mandatory parameters. : ");
    LOG_INFO(logger_, "\n" << config_desc_);
    return false;
  }

  if (config_["output-rocksdb-path"].empty()) {
    LOG_INFO(logger_,
             "--output-rocksdb-path is missing. Please read the help below and pass the mandatory parameters. : ");
    LOG_INFO(logger_, "\n" << config_desc_);
    return false;
  }

  if (config_["point-lookup-batch-size"].as<std::uint64_t>() < 1) {
    LOG_ERROR(logger_, "point-lookup-batch-size must be greater than or equal to 1");
    return false;
  } else if (config_["point-lookup-threads"].as<std::uint64_t>() < 1) {
    LOG_ERROR(logger_, "point-lookup-threads must be greater than or equal to 1");
    return false;
  } else if (config_["max-point-lookup-batches"].as<std::uint64_t>() < 1) {
    LOG_ERROR(logger_, "max-point-lookup-batches must be greater than or equal to 1");
    return false;
  }

  return true;
}
