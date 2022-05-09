// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "Logger.hpp"
#include <functional>
#include <boost/dll.hpp>
#include <boost/program_options.hpp>

#include "integrity_checker.hpp"
#include "db_restore.hpp"

namespace po = boost::program_options;
using namespace std::placeholders;
using concord::kvbc::tools::IntegrityChecker;
using concord::kvbc::tools::DBRestore;
// clang-format off
int main(int argc, char** argv) {
  logging::initLogger("logging.properties");
  logging::Logger logger = logging::getLogger("concord.kvbc.tools.object_store");
  auto checker = std::make_shared<IntegrityChecker>(logger);
  auto restorer = std::make_shared<DBRestore>(checker, logger);

  po::options_description global{boost::dll::program_location().filename().string() +
                                " MANDATORY_OPTIONS <validate|restore> OPTIONS"};
  global.add_options()("help", "produce a help message");

  po::options_description mandatory{"MANDATORY_OPTIONS"};
  mandatory.add_options()
  ("keys-file",
   po::value<fs::path>()->required()->notifier(std::bind(std::mem_fn(&IntegrityChecker::initKeysConfig),
                                                         checker.get(),
                                                         _1)),
  "crypto keys configuration file path")
  ("s3-config-file",
  po::value<fs::path>()->required()->notifier(std::bind(std::mem_fn(&IntegrityChecker::initS3Config),
                                                        checker.get(),
                                                        _1)),
  "s3 configuration file path");

  po::options_description command{"Command"};
  command.add_options()("command", po::value<std::string>()->required()  , "command to execute")
                       ("args"   , po::value<std::vector<std::string> >(), "command arguments");
  po::positional_options_description positional;
  positional.add("command", 1).add("args", -1);

  po::options_description check_options{"validate OPTIONS"};
  check_options.add_options()
    ("all"                              , "validate the integrity of an entire blockchain")
    ("range", po::value<std::uint64_t>(), "validate the integrity of a blockchain range from tail to block_id")
    ("key"  , po::value<std::string>()  , "validate a key");


  po::options_description restore_options{"restore OPTIONS"};
  restore_options.add_options()
      ("db-path",
      po::value<fs::path>()->required()->notifier(std::bind(std::mem_fn(&DBRestore::initRocksDB), restorer.get(), _1)),
      "rocksDB directory path");

  auto usage = [logger, global, mandatory, check_options, restore_options]() {
    LOG_INFO(logger, "\nUsage: " << global << "\n" << mandatory << "\n" << check_options << "\n" << restore_options);
  };

  global.add(mandatory).add(command);
  try{
    po::parsed_options parsed = po::command_line_parser(argc, argv).options(global)
                                                                   .positional(positional)
                                                                   .allow_unregistered()
                                                                   .run();
    po::variables_map var_map;
    po::store(parsed, var_map);
    if(var_map.count("help")){
      usage();
      exit(1);
    }
    // handle mandatory options
    po::notify(var_map);
    std::string cmd = var_map["command"].as<std::string>();
    // Collect all the unrecognized options from the first pass.
    // This will include the (positional) command name, so we need to erase that.
    std::vector<std::string> opts = po::collect_unrecognized(parsed.options, po::include_positional);
    opts.erase(opts.begin());
    var_map.clear();
    if (cmd == "validate"){
      po::store(po::command_line_parser(opts).options(check_options).run(), var_map);
      po::notify(var_map);
       if (var_map.count("key")) {
         checker->validateKey(var_map["key"].as<std::string>());
       } else if (var_map.count("all")) {
         checker->validateAll();
       } else if (var_map.count("range")) {
         checker->validateRange(var_map["range"].as<std::uint64_t>());
       } else {
         LOG_ERROR(logger, check_options);
         exit(1);
       }
    }else if (cmd == "restore"){
      po::store(po::command_line_parser(opts).options(restore_options).run(), var_map);
      po::notify(var_map);
      restorer->restore();
    }else{
      usage();
    }
  } catch (const std::exception& e) {
    LOG_ERROR(GL, e.what());
    //usage();
    exit(1);
  }
  exit(0);
}
// clang-format on
