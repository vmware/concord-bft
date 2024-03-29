// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "util/MetricsServer.hpp"
#include "util/Metrics.hpp"

#include <iostream>
#include <unistd.h>

using namespace std;
using namespace concordMetrics;

int main() {
  cout << "Starting MetricsServer" << endl;
  concordMetrics::Server server(6161);
  server.Start();

  // We don't join the thread until server.Stop(), so keep the main thread
  // running.
  while (1) {
    sleep(1);
  }
}
