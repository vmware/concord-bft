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

#include "s3/integrity_checker.hpp"

int main(int argc, char** argv) {
  try {
    logging::initLogger("logging.properties");
    concord::storage::s3::IntegrityChecker checker(argc, argv);
    checker.check();
    exit(0);
  } catch (const std::exception& e) {
    LOG_ERROR(GL, e.what());
    exit(1);
  }
}
