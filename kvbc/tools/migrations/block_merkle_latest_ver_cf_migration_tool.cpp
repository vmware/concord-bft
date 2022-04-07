#include "migrations/block_merkle_latest_ver_cf_migration.h"

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>

#include <exception>
#include <iostream>
#include <string>
#include <utility>

namespace {

namespace po = boost::program_options;
namespace fs = boost::filesystem;

std::pair<po::options_description, po::variables_map> parseArgs(int argc, char* argv[]) {
  auto desc = po::options_description(
      "Migrates a RocksDB DB from using key hashes in the `block_merkle_latest_key_version` to using raw keys.\n"
      "Allowed options");

  // clang-format off
  desc.add_options()
    ("help", "Show help.")

    ("parallel-block-reads",
     po::value<std::int64_t>()->default_value(40),
     "This will be the number of blocks read parallely in a single batch.")

    ("rocksdb-path",
      po::value<std::string>(),
      "The path to the RocksDB data directory.")

    ("temp-export-path",
      po::value<std::string>(),
      "The path to a temporary export directory. Must be on the same filesystem as rocksdb-path.");
  // clang-format on

  auto config = po::variables_map{};
  po::store(po::parse_command_line(argc, argv, desc), config);
  po::notify(config);
  return std::make_pair(desc, config);
}

int run(int argc, char* argv[]) {
  using namespace concord::kvbc::migrations;

  const auto [desc, config] = parseArgs(argc, argv);

  if (config.count("help")) {
    std::cout << desc << std::endl;
    return EXIT_SUCCESS;
  }

  const size_t block_read_batch_size =
      (config["parallel-block-reads"].as<std::int64_t>() < 1) ? 40 : config["parallel-block-reads"].as<std::int64_t>();

  if (config["rocksdb-path"].empty()) {
    std::cerr << desc << std::endl;
    return EXIT_FAILURE;
  }
  const auto rocksdb_path = config["rocksdb-path"].as<std::string>();

  if (!fs::exists(rocksdb_path)) {
    std::cerr << "RocksDB database doesn't exist at " << rocksdb_path << std::endl;
    return EXIT_FAILURE;
  }

  if (config["temp-export-path"].empty()) {
    std::cerr << desc << std::endl;
    return EXIT_FAILURE;
  }
  const auto temp_export_path = config["temp-export-path"].as<std::string>();

  if (fs::equivalent(rocksdb_path, temp_export_path)) {
    std::cerr << "Error! 'rocksdb-path path' cannot be the same as 'temp-export-path'." << std::endl;
    return EXIT_FAILURE;
  }

  auto migration = BlockMerkleLatestVerCfMigration{rocksdb_path, temp_export_path, block_read_batch_size};
  const auto status = migration.execute();
  switch (status) {
    case BlockMerkleLatestVerCfMigration::ExecutionStatus::kExecuted:
      std::cout << "Success! Migration executed successfully!" << std::endl;
      break;

    case BlockMerkleLatestVerCfMigration::ExecutionStatus::kNotNeededOrAlreadyExecuted:
      std::cout << "Success! Migration not needed or already executed!" << std::endl;
      break;
  }

  return EXIT_SUCCESS;
}

}  // namespace

int main(int argc, char* argv[]) {
  try {
    return run(argc, argv);
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Unknown error" << std::endl;
  }
  return EXIT_FAILURE;
}
