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

#include <cstring>
#include <iostream>
#include <future>
#include <thread>

#include <sys/types.h>
#include <sys/socket.h>

#include "diagnostics_server.h"
#include "gtest/gtest.h"
#include "diagnostics.h"
#include "protocol.h"

using namespace concord::diagnostics;

static const std::string sync_handler_name("sync_test_handler");
static const std::string sync_handler_description("A syncrhonous status handler for this test");
static const std::string sync_handler_status("Some synchronous status");

static const std::string async_handler_name("async_test_handler");
static const std::string async_handler_description("An asyncrhonous status handler for this test");
static const std::string async_handler_status("Some asynchronous status");

static const std::string async_handler_name2("async_test_handler2");

static const std::string keylist = async_handler_name + "\n" + async_handler_name2 + "\n" + sync_handler_name + "\n";

StatusHandler sync_handler(sync_handler_name, sync_handler_description, []() { return sync_handler_status; });

StatusHandler async_handler(async_handler_name, async_handler_description, []() {
  // This mimics some arbitrary asynch behavior, such as sending an internal message containing a
  // promise to the replica thread, and then waiting on the future.
  std::promise<std::string> promise;
  auto future = promise.get_future();
  std::thread t([&promise]() { promise.set_value(async_handler_status); });
  t.join();
  return future.get();
});

StatusHandler async_handler2(async_handler_name2, async_handler_description, []() {
  // This mimics some arbitrary asynch behavior, such as sending an internal message containing a
  // promise to the replica thread, and then waiting on the future.
  std::promise<std::string> promise;
  auto future = promise.get_future();
  auto f = std::async([promise = std::move(promise)]() mutable { promise.set_value(async_handler_status); });
  f.wait();
  return future.get();
});

TEST(diagnostics_tests, status_registration) {
  Registrar registrar;
  registrar.status.registerHandler(sync_handler);
  registrar.status.registerHandler(async_handler);
  registrar.status.registerHandler(async_handler2);
  ASSERT_EQ(sync_handler_status, registrar.status.get(sync_handler_name));
  ASSERT_EQ(sync_handler_description, registrar.status.describe(sync_handler_name));
  ASSERT_EQ(async_handler_status, registrar.status.get(async_handler_name));
  ASSERT_EQ(async_handler_description, registrar.status.describe(async_handler_name));
  ASSERT_EQ(async_handler_status, registrar.status.get(async_handler_name2));
  ASSERT_EQ(async_handler_description, registrar.status.describe(async_handler_name2));

  ASSERT_EQ("*--STATUS_NOT_FOUND--*", registrar.status.get("no such handler"));
  ASSERT_EQ("*--DESCRIPTION_NOT_FOUND--*", registrar.status.describe("no such handler"));

  ASSERT_EQ(keylist, registrar.status.listKeys());
  std::cout << registrar.status.describe() << std::endl;
  std::cout << registrar.status.listKeys() << std::endl;
}

// This tests is really just for visual confirmation, since strings are always returned.
TEST(diagnostics_tests, protocol) {
  Registrar registrar;
  registrar.status.registerHandler(sync_handler);
  registrar.status.registerHandler(async_handler);
  registrar.status.registerHandler(async_handler2);

  // No parameters trigger usage to be displayed.
  ASSERT_EQ(0, std::memcmp("Usage:", run({}, registrar).c_str(), 6));

  // Bad subject triggers usage
  ASSERT_EQ(0, std::memcmp("Usage:", run({"no_such_subject"}, registrar).c_str(), 6));

  // Good subject, bad command triggers usage
  ASSERT_EQ(0, std::memcmp("Usage:", run({"status", "bad_command"}, registrar).c_str(), 6));

  // Listing keys works
  ASSERT_EQ(keylist, run({"status", "list"}, registrar));

  // Describing a status handler works
  ASSERT_EQ(async_handler_description + "\n", run({"status", "describe", async_handler_name}, registrar));

  // Multiple descriptions works
  auto expected = async_handler_description + "\n" + async_handler_description + "\n";
  ASSERT_EQ(expected, run({"status", "describe", async_handler_name, async_handler_name2}, registrar));

  // Getting status works
  ASSERT_EQ(async_handler_status + "\n", run({"status", "get", async_handler_name}, registrar));

  // Getting status for multiple handlers works
  expected = async_handler_status + "\n" + async_handler_status + "\n";
  ASSERT_EQ(expected, run({"status", "get", async_handler_name, async_handler_name2}, registrar));
}

TEST(diagnostics_server_tests, shutdown_cleanly_from_destructor) {
  static constexpr uint16_t PORT = 6888;
  Registrar registrar;
  concord::diagnostics::Server diagnostics_server;
  diagnostics_server.start(registrar, INADDR_LOOPBACK, PORT);
}

TEST(diagnostics_server_tests, shutdown_cleanly_from_stop) {
  static constexpr uint16_t PORT = 6888;
  Registrar registrar;
  concord::diagnostics::Server diagnostics_server;
  diagnostics_server.start(registrar, INADDR_LOOPBACK, PORT);
  diagnostics_server.stop();
}

void connect_to_ds(uint16_t port) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_TRUE(sock);
  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  std::cout << "Connecting to diagnostics server" << std::endl;
  int rv = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
  if (rv == 0) {
    std::cout << "Connected to diagnostics server" << std::endl;
  } else {
    std::cout << "Failed to connect to diagnostics server" << std::endl;
  }
}

TEST(diagnostics_server_tests, connect_does_not_trigger_crash) {
  static constexpr uint16_t PORT = 6889;
  Registrar registrar;
  {
    concord::diagnostics::Server diagnostics_server;
    diagnostics_server.start(registrar, INADDR_LOOPBACK, PORT);

    while (!diagnostics_server.listening_) {
      sleep(1);
    }
    connect_to_ds(PORT);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
