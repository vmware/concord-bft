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

#pragma once

#include "test_comm_config.hpp"
#include "test_parameters.hpp"
#include "commonDefs.h"
#include "communication/CommFactory.hpp"
#include "communication/CommDefs.hpp"
#include "SimpleClient.hpp"
#include "histogram.hpp"
#include "TimeUtils.hpp"
#include "assertUtils.hpp"
#include "utils.hpp"

using namespace bftEngine;
using namespace bft::communication;
using namespace std;

using bftEngine::impl::getMonotonicTimeMicro;

class SimpleTestClient {
 private:
  ClientParams cp;
  logging::Logger clientLogger;

 public:
  SimpleTestClient(ClientParams& clientParams, logging::Logger& logger) : cp{clientParams}, clientLogger{logger} {}
  bool run();
};
