
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

#include <unistd.h>

#include "diagnostics.h"
#include "diagnostics_server.h"

using namespace concord::diagnostics;

static constexpr uint16_t PORT = 6888;

int main() {
  Registrar registrar;

  StatusHandler handler1("handler1", "handler 1 description", []() { return "handler1 called"; });
  StatusHandler handler2("handler2", "handler 2 description", []() { return "handler2 called"; });

  registrar.registerStatusHandler(handler1);
  registrar.registerStatusHandler(handler2);

  concord::diagnostics::Server diagnostics_server;
  diagnostics_server.start(registrar, INADDR_LOOPBACK, PORT);

  // Keep the diagnostics_server alive indefinitely
  while (true) {
    sleep(1);
  }

  return 0;
}
