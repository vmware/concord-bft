// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "assertUtils.hpp"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "categorization/column_families.h"
#include "categorization/details.h"
#include "hex_tools.h"
#include "kv_types.hpp"
#include "rocksdb/native_client.h"
#include "sha_hash.hpp"
#include "thread_pool.hpp"
#include "benchmark/state_snapshot_benchmarks/multi_get_batch.hpp"
#include "concord_kvbc.pb.h"
#include "kvbc_app_filter/kvbc_app_filter.h"
#include "categorization/categorized_reader.h"

#include <boost/program_options.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>

#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

namespace po = boost::program_options;

using concord::kvbc::categorization::detail::BLOCK_MERKLE_LATEST_KEY_VERSION_CF;
using concord::kvbc::categorization::detail::BLOCK_MERKLE_KEYS_CF;
using namespace concordUtils;
using namespace concord::kvbc;
using namespace concord::kvbc::categorization;
using namespace concord::kvbc::categorization::detail;
using namespace concord::storage::rocksdb;
using namespace concord::util;
using concord::benchmark::MultiGetBatch;
using com::vmware::concord::kvbc::ValueWithTrids;
using concord::kvbc::categorization::CategorizedReader;
using concord::kvbc::categorization::KeyValueBlockchain;
using concord::kvbc::KvbAppFilter;

namespace concord::state_snapshot_tool {

std::pair<po::options_description, po::variables_map> parseArgs(int argc, char* argv[]) {
  const auto kSystemThreads =
      unsigned{std::thread::hardware_concurrency() > 0 ? std::thread::hardware_concurrency() : 1};

  auto desc = po::options_description("Allowed options");

  // clang-format off
  desc.add_options()
    ("help", "Show usage.")

    ("rocksdb-path",
      po::value<std::string>(),
      "The path to the RocksDB data directory.")

    ("rocksdb-cache-size",
      po::value<std::int64_t>()->default_value(4294967296), // 4GB
      "RocksDB block cache size in bytes.")

    ("point-lookup-batch-size",
      po::value<std::int64_t>()->default_value(1000),
      "The number of keys to accumulate and then read via RocksDB MultiGet(). Will be rounded if needed.")

    ("point-lookup-threads",
      po::value<std::int64_t>()->default_value(kSystemThreads),
      "Number of threads that execute MultiGet() point lookups in parallel.")

    ("rocksdb-config-file",
      po::value<std::string>(),
      "The path to the RocksDB configuration file.")

    ("client-id",
      po::value<std::string>()->default_value(""),
      "Client-id for which verifiable state snapshot is requested")
    
    ("ext-event-group-id",
      po::value<std::int64_t>()->default_value(0),
      "External event group id or offset for streaming state keys proof");

  // clang-format on

  auto config = po::variables_map{};
  po::store(po::parse_command_line(argc, argv, desc), config);
  po::notify(config);
  return std::make_pair(desc, config);
}

void completeRocksdbConfiguration(::rocksdb::Options& db_options,
                                  std::vector<::rocksdb::ColumnFamilyDescriptor>& cf_descs,
                                  size_t cache_size) {
  auto table_options = ::rocksdb::BlockBasedTableOptions{};
  table_options.block_cache = ::rocksdb::NewLRUCache(cache_size);
  table_options.filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10, false));
  db_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Use the same block cache and table options for all column familes for now.
  for (auto& d : cf_descs) {
    auto* cf_table_options =
        reinterpret_cast<::rocksdb::BlockBasedTableOptions*>(d.options.table_factory->GetOptions());
    cf_table_options->block_cache = table_options.block_cache;
    cf_table_options->filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10, false));
  }
}

int run(int argc, char* argv[]) {
  const auto [desc, config] = parseArgs(argc, argv);

  if (config.count("help")) {
    std::cout << desc << std::endl;
    return EXIT_SUCCESS;
  }

  if (config["rocksdb-path"].empty() || config["rocksdb-config-file"].empty()) {
    std::cerr << desc << std::endl;
    return EXIT_FAILURE;
  }

  const auto rocksdb_path = config["rocksdb-path"].as<std::string>();
  auto point_lookup_batch_size = config["point-lookup-batch-size"].as<std::int64_t>();
  const auto point_lookup_threads = config["point-lookup-threads"].as<std::int64_t>();
  const auto rocksdb_cache_size = config["rocksdb-cache-size"].as<std::int64_t>();
  const auto rocksdb_conf = config["rocksdb-config-file"].as<std::string>();
  const auto client_id = (config["client-id"].empty() ? std::string{} : config["client-id"].as<std::string>());
  const auto ext_ev_group_id = config["ext-event-group-id"].as<std::int64_t>();

  if (point_lookup_batch_size < 1) {
    std::cerr << "point-lookup-batch-size must be greater than or equal to 1" << std::endl;
    return EXIT_FAILURE;
  } else if (point_lookup_threads < 1) {
    std::cerr << "point-lookup-threads must be greater than or equal to 1" << std::endl;
    return EXIT_FAILURE;
  } else if (rocksdb_cache_size < 8192) {
    std::cerr << "rocksdb-cache-size must be greater than or equal to 8192" << std::endl;
    return EXIT_FAILURE;
  }

  auto num_of_keys = uint64_t{0};

  // Make the point lookup batch size divisible by the number of threads for simplicity.
  while (point_lookup_batch_size % point_lookup_threads) {
    point_lookup_batch_size++;
  }
  auto thread_pool = ThreadPool{static_cast<std::uint32_t>(point_lookup_threads)};

  std::cout << "Hash state keys {H(H(k1)|H(v1) | H(k2)|H(v2) .....H(kn|vn))} acl-filtered for client id: " << client_id
            << ", with a point lookup batch size = " << point_lookup_batch_size
            << ", point lookup threads = " << point_lookup_threads
            << ", RocksDB block cache size = " << rocksdb_cache_size << " bytes, configuration file = " << rocksdb_conf
            << ", DB path = " << rocksdb_path << std::endl;

  auto complete_init = [rocksdb_cache_size](auto& db_options, auto& cf_descs) {
    completeRocksdbConfiguration(db_options, cf_descs, rocksdb_cache_size);
  };

  auto opts = NativeClient::UserOptions{rocksdb_conf, complete_init};
  const auto read_only = true;
  auto db = NativeClient::newClient(config["rocksdb-path"].as<std::string>(), read_only, opts);
  const auto link_st_chain = false;
  const auto kvbc = std::make_shared<const KeyValueBlockchain>(db, link_st_chain);
  const auto reader = CategorizedReader{kvbc};
  const auto filter = KvbAppFilter{&reader, client_id};
  auto num_of_pvt_keys = uint64_t{0};
  auto num_of_public_keys = uint64_t{0};
  // Start with an arbitrary hash - SHA2-256('a').
  auto current_hash = SHA2_256{}.digest("a", 1);

  auto get_block_id_from_ext_ev_gr_id = [&](uint64_t ext_evg_id) -> std::optional<uint64_t> {
    if (ext_evg_id == 0) {
      return reader.getLastBlockId();
    } else {
      auto [global_eg_id, is_previous_public, private_eg_id, public_eg_id] = filter.findGlobalEventGroupId(ext_evg_id);
      (void)is_previous_public;
      (void)private_eg_id;
      (void)public_eg_id;
      const auto opt = reader.getLatest(concord::kvbc::categorization::kExecutionEventGroupDataCategory,
                                        concordUtils::toBigEndianStringBuffer(global_eg_id));
      if (not opt) {
        std::ostringstream msg;
        msg << "Failed to get global event group " << global_eg_id;
        throw std::runtime_error(msg.str());
      }
      const auto imm_val = std::get_if<concord::kvbc::categorization::ImmutableValue>(&(opt.value()));
      if (not imm_val) {
        std::ostringstream msg;
        msg << "Failed to convert stored global event group " << ext_evg_id;
        throw std::runtime_error(msg.str());
      }
      return imm_val->block_id;
    }
  };
  const auto offset = get_block_id_from_ext_ev_gr_id(ext_ev_group_id).value_or(reader.getLastBlockId());

  auto multi_get_batch = MultiGetBatch<Buffer>{static_cast<std::uint64_t>(point_lookup_batch_size),
                                               static_cast<std::uint32_t>(point_lookup_threads)};

  auto print_kv_with_acl = [&](const Buffer& buff, const auto& value, const auto& trids, bool is_public) {
    // In actual application, we would probably stream state keys {k , hash(v) } over grpc
    // here we are just printing it
    std::ostringstream oss;
    const auto hash = Hash(SHA2_256().digest(value.data(), value.size()));
    oss << "key: " << bufferToHex(buff.data(), buff.size()) << std::endl;
    oss << "hash(val): " << bufferToHex(hash.data(), hash.size()) << std::endl;
    if (is_public)
      oss << "public" << std::endl;
    else
      oss << "private " << client_id << std::endl;
    oss << std::endl;
    std::cout << oss.str();
  };
  auto print_result = [&]() {
    std::ostringstream oss;
    oss << "Block id range: [" << reader.getGenesisBlockId() << " " << offset << "]" << std::endl;
    oss << "client_id: " << client_id << std::endl;
    oss << "number of public state keys: " << num_of_public_keys << std::endl;
    oss << "number of private state keys: " << num_of_pvt_keys << std::endl;
    oss << "Final hash: " << bufferToHex(current_hash.data(), current_hash.size()) << std::endl;
    std::cout << oss.str();
  };

  auto has_access = [&num_of_pvt_keys, &num_of_public_keys, &client_id](const auto& trids) -> bool {
    bool allowed = false;
    if (trids.size()) {
      for (const auto& trid : trids) {
        if (client_id == trid) {
          num_of_pvt_keys++;
          allowed = true;
        }
      }
    } else {
      // public state key
      num_of_public_keys++;
      allowed = true;
    }
    return allowed;
  };
  auto hash_state_kv = [&current_hash](const auto& key, const auto& val) {
    auto h = SHA2_256{};
    h.init();
    h.update(current_hash.data(), current_hash.size());
    h.update(key.data(), key.size());
    h.update(val.data(), val.size());
    current_hash = h.finish();
  };

  auto read_batch = [&]() {
    if (multi_get_batch.empty()) {
      return;
    }

    auto futures = std::vector<std::future<void>>{};
    for (auto i = 0ull; i < multi_get_batch.numSubBatches(); ++i) {
      const auto& serialized_keys = multi_get_batch.serializedKeys(i);
      if (serialized_keys.empty()) {
        break;
      }
      auto& value_slices = multi_get_batch.valueSlices(i);
      auto& statuses = multi_get_batch.statuses(i);
      futures.push_back(
          thread_pool.async([&]() { db->multiGet(BLOCK_MERKLE_KEYS_CF, serialized_keys, value_slices, statuses); }));
    }

    auto key_idx = 0;
    for (auto i = 0ull; i < futures.size(); ++i) {
      futures[i].wait();

      const auto& serialized_keys = multi_get_batch.serializedKeys(i);
      const auto& value_slices = multi_get_batch.valueSlices(i);
      const auto& statuses = multi_get_batch.statuses(i);
      for (auto j = 0ull; j < serialized_keys.size(); ++j) {
        ConcordAssert(statuses[j].ok());
        num_of_keys++;
        auto dbvalue = concord::kvbc::categorization::DbValue{};
        try {
          auto* start = reinterpret_cast<const std::uint8_t*>(value_slices[j].data());
          auto* end = reinterpret_cast<const std::uint8_t*>(value_slices[j].data() + value_slices[j].size());
          concord::kvbc::categorization::deserialize(start, end, dbvalue);
        } catch (const std::runtime_error& de) {
          throw std::runtime_error{"Deserialization failed "};
        }
        if (dbvalue.deleted) {
          continue;
        }

        auto value = std::string{};
        auto trids = std::vector<std::string>{};
        ValueWithTrids proto;
        if (!proto.ParseFromArray(dbvalue.data.data(), dbvalue.data.length())) {
          std::stringstream msg;
          msg << "Couldn't decode ValueWithTrids for ";
          throw std::runtime_error(msg.str());
        }
        if (!proto.has_value()) {
          std::stringstream msg;
          msg << "Couldn't find value in ValueWithTrids for ";
          throw std::runtime_error(msg.str());
        }
        auto v = std::unique_ptr<std::string>(proto.release_value());
        value.assign(std::move(*v));
        for (auto& t : proto.trid()) {
          trids.emplace_back(t);
        }

        if (has_access(trids)) {
          hash_state_kv(serialized_keys[j], value);
        }
        // print all state keys - hash(val), trid
        // print_kv_with_acl(serialized_keys[j], value, trids, (trids.size() ? false : true));
        ++key_idx;
      }
    }
  };

  auto it = db->getIterator(BLOCK_MERKLE_LATEST_KEY_VERSION_CF);
  it.first();
  while (it) {
    auto ver_key = VersionedKey{};

    const auto key_view = it.keyView();
    const auto value_view = it.valueView();
    ConcordAssertEQ(value_view.size(), sizeof(BlockId));

    auto key_hash = hash(key_view);
    // Fill in the versioned key that we will use for lookup in the BLOCK_MERKLE_KEYS_CF column family.
    std::copy(key_hash.cbegin(), key_hash.cend(), ver_key.key_hash.value.begin());

    // Get the key version.
    auto version = LatestKeyVersion{};
    deserialize(value_view, version);
    const auto tagged_version = TaggedVersion{version.block_id};

    // Move the iterator.
    it.next();
    // If the key is deleted, we skip it.
    if (tagged_version.deleted) {
      continue;
    }
    if (tagged_version.version > offset) {
      continue;
    }

    ver_key.version = tagged_version.version;
    multi_get_batch.push_back(ver_key);

    if (multi_get_batch.size() == static_cast<std::uint32_t>(point_lookup_batch_size)) {
      read_batch();
      multi_get_batch.clear();
    }
  }
  (void)print_kv_with_acl;
  read_batch();
  print_result();

  return EXIT_SUCCESS;
}
}  // namespace concord::state_snapshot_tool
int main(int argc, char* argv[]) {
  try {
    return concord::state_snapshot_tool::run(argc, argv);
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Unknown error" << std::endl;
  }
  return EXIT_FAILURE;
}