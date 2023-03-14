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

#include "migration_bookeeper.hpp"

int main(int argc, char* argv[]) {
  static logging::Logger logger(logging::getLogger("concord.migration.v4.main"));
  try {
    BookKeeper v4_migration;
    return v4_migration.migrate(argc, argv);
  } catch (const std::exception& e) {
    LOG_ERROR(logger, "Error: " << e.what());
  } catch (...) {
    LOG_ERROR(logger, "Unknown Error");
  }
  return EXIT_FAILURE;
}
