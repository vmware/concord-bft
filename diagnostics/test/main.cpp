
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

#include <chrono>
#include <random>
#include <thread>
#include <unistd.h>

#include "diagnostics.h"
#include "diagnostics_server.h"

using namespace std::chrono_literals;

using namespace concord::diagnostics;

static constexpr uint16_t PORT = 6888;

// 5 Minutes
static constexpr int64_t MAX_VALUE_MICROSECONDS = 1000 * 1000 * 60 * 5;

int main() {
  Registrar registrar;

  StatusHandler handler1("handler1", "handler 1 description", []() { return "handler1 called"; });
  StatusHandler handler2("handler2", "handler 2 description", []() { return "handler2 called"; });

  registrar.status.registerHandler(handler1);
  registrar.status.registerHandler(handler2);

  auto recorder1 = std::make_shared<Recorder>("histogram1", 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  auto recorder2 = std::make_shared<Recorder>("histogram2", 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  auto recorder3 = std::make_shared<Recorder>("histogram3", 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  auto recorder4 = std::make_shared<Recorder>("histogram4", 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);

  registrar.perf.registerComponent("component1", {recorder1, recorder2});
  registrar.perf.registerComponent("component2", {recorder3, recorder4});

  concord::diagnostics::Server diagnostics_server;
  diagnostics_server.start(registrar, INADDR_LOOPBACK, PORT);

  // Periodically update histograms
  auto thread1 = std::thread([&recorder1, &recorder2]() mutable {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(1, 1000000);
    while (true) {
      std::this_thread::sleep_for(50ms);
      recorder1->record(distrib(gen));
      recorder2->record(distrib(gen));
    }
  });
  auto thread2 = std::thread([&recorder3, &recorder4]() mutable {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(1, 1000000);
    while (true) {
      std::this_thread::sleep_for(50ms);
      recorder3->record(distrib(gen));
      recorder4->record(distrib(gen));
    }
  });

  thread1.join();
  thread2.join();

  return 0;
}
