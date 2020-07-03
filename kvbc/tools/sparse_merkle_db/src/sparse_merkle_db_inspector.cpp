// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "sparse_merkle_db_inspector.hpp"

#include <iostream>

using namespace concord::kvbc::tools::sparse_merkle_db;

int main(int argc, char *argv[]) { return run(command_line_arguments(argc, argv), std::cout, std::cerr); }
