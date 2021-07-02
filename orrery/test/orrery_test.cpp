// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <atomic>
#include <iostream>

#include "gtest/gtest.h"

#include "orrery/executor.h"
#include "orrery/environment.h"
#include "orrery_msgs.cmf.hpp"
#include "orrery/world.h"

namespace concord::orrery::test {

std::atomic_size_t st_msgs_received = 0;
std::atomic_size_t replica_msgs_received = 0;
std::atomic_size_t control_msgs_received = 0;

class StateTransferComponent {
 public:
  StateTransferComponent(World&& world) : world_(std::move(world)) {}
  ComponentId id{ComponentId::state_transfer};

  void handle(ComponentId from, StateTransferMsg&& msg) {
    std::cout << "Handling state transfer msg with id: " << msg.id << std::endl;
    st_msgs_received++;
    world_.send(ComponentId::replica, ConsensusMsg{});
  }
  void handle(ComponentId from, ControlMsg&& msg) { control_msgs_received++; }

 private:
  World world_;
};

class ReplicaComponent {
 public:
  ReplicaComponent(World&& world) : world_(std::move(world)) {}
  ComponentId id{ComponentId::replica};

  void handle(ComponentId from, ConsensusMsg&& msg) {
    std::cout << "Handling consensus msg with id: " << msg.id << std::endl;
    replica_msgs_received++;
  }
  void handle(ComponentId from, ControlMsg&& msg) { control_msgs_received++; }

 private:
  World world_;
};

// Retry condition every 1ms, timeout after 5s
template <typename Predicate>
bool waitFor(Predicate pred) {
  for (int i = 0; i < 5000; i++) {
    if (pred()) {
      return true;
    }
    usleep(1000);
  }
  return false;
}

TEST(orrery_test, basic) {
  auto exec1 = Executor("replica_executor");
  auto exec2 = Executor("state_transfer_executor");

  // Assign components to executors/mailboxes
  auto env = Environment();
  env.add(ComponentId::replica, exec1.mailbox());
  env.add(ComponentId::state_transfer, exec2.mailbox());

  // Inform the executors about the environment
  exec1.init(env);
  exec2.init(env);

  auto replica_world = World(env, ComponentId::replica);
  auto st_world = World(env, ComponentId::state_transfer);

  // Create polymorphic versions of components and give ownership to the executors
  exec1.add(ComponentId::replica,
            std::make_unique<Component<ReplicaComponent>>(ReplicaComponent{std::move(replica_world)}));
  exec2.add(ComponentId::state_transfer,
            std::make_unique<Component<StateTransferComponent>>(StateTransferComponent{std::move(st_world)}));

  auto mailbox1 = exec1.mailbox();
  auto mailbox2 = exec2.mailbox();

  ASSERT_EQ(0, replica_msgs_received);
  ASSERT_EQ(0, st_msgs_received);
  ASSERT_EQ(0, control_msgs_received);

  // Simulate sending a message from one component to another
  mailbox1.put(Envelope{ComponentId::replica, ComponentId::state_transfer, AllMsgs{ConsensusMsg{}}});

  // Send an invalid message
  mailbox1.put(Envelope{ComponentId::replica, ComponentId::state_transfer, AllMsgs{StateTransferMsg{}}});

  auto thread1 = std::move(exec1).start();
  auto thread2 = std::move(exec2).start();

  ASSERT_TRUE(waitFor([]() { return replica_msgs_received == 1; }));

  // Send a state transfer message which should trigger the state transfer component sending a message to the replica
  // component.
  mailbox2.put(Envelope{ComponentId::state_transfer, ComponentId::replica, AllMsgs{StateTransferMsg{}}});
  ASSERT_TRUE(waitFor([]() { return st_msgs_received == 1; }));
  ASSERT_TRUE(waitFor([]() { return replica_msgs_received == 2; }));

  // Shutdown both executors with a broadcast
  auto shutdown = Envelope{ComponentId::broadcast, ComponentId::replica, AllMsgs{ControlMsg{ControlCmd::shutdown}}};
  auto shutdown2 = shutdown;
  mailbox1.put(std::move(shutdown));
  mailbox2.put(std::move(shutdown2));

  // Ensure both shutdown messages were received by their respective components. Executor's pass
  // shutdown messages to components, in case they need to do any cleanup.
  ASSERT_TRUE(waitFor([]() { return control_msgs_received == 2; }));

  thread1.join();
  thread2.join();
}

}  // namespace concord::orrery::test

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
