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

#include "gtest/gtest.h"

#include "IncomingMsgsStorageImp.hpp"
#include "MsgHandlersRegistrator.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>

namespace {

using namespace bftEngine::impl;
using namespace std::chrono_literals;

class incoming_msgs_storage_test : public ::testing::Test {
  void SetUp() override {
    reg_->registerMsgHandler(msg_id_, [this](MessageBase* msg) { consumer_(msg); });
    storage_.emplace(reg_, msg_wait_timeout_, replica_id_);
    storage_->start();
  }

  void TearDown() override { storage_->stop(); }

 protected:
  auto newMsg() const { return std::make_unique<MessageBase>(sender_, msg_id_, msg_size_); }
  auto newMsg(std::uint16_t msg_id) const { return std::make_unique<MessageBase>(sender_, msg_id, msg_size_); }
  static std::unique_ptr<MessageBase> own(MessageBase* msg) { return std::unique_ptr<MessageBase>{msg}; }
  auto waitTillMsgConsumed() { return consumer_.get_future().get(); }
  std::string buffer() const { return std::string(maxMessageSize<MessageBase>(), '\0'); }

 protected:
  const std::uint16_t replica_id_{0};
  const std::uint16_t msg_id_{0};
  const NodeIdType sender_{0};
  const std::chrono::milliseconds msg_wait_timeout_{100ms};
  const MsgSize msg_size_{sizeof(MessageBase::Header)};
  std::packaged_task<std::unique_ptr<MessageBase>(MessageBase*)> consumer_{[](MessageBase* msg) { return own(msg); }};
  const std::shared_ptr<MsgHandlersRegistrator> reg_ = std::make_shared<MsgHandlersRegistrator>();
  std::optional<IncomingMsgsStorageImp> storage_;
};

TEST_F(incoming_msgs_storage_test, push_external_without_callback) {
  ASSERT_TRUE(storage_->pushExternalMsg(newMsg()));
  auto msg = waitTillMsgConsumed();
  ASSERT_EQ(msg_size_, msg->size());
  ASSERT_EQ(sender_, msg->senderId());
  ASSERT_EQ(msg_id_, msg->type());
}

TEST_F(incoming_msgs_storage_test, push_external_raw_without_callback) {
  auto msg_before = newMsg();
  auto buf = buffer();
  auto ptr = buf.data();
  MessageBase::serializeMsg(ptr, msg_before.get());
  ASSERT_TRUE(storage_->pushExternalMsgRaw(buf.data(), buf.size()));
  auto msg_after = waitTillMsgConsumed();
  ASSERT_EQ(msg_size_, msg_after->size());
  ASSERT_EQ(sender_, msg_after->senderId());
  ASSERT_EQ(msg_id_, msg_after->type());
}

TEST_F(incoming_msgs_storage_test, push_external_with_callback) {
  auto popped = std::atomic_bool{false};
  ASSERT_TRUE(storage_->pushExternalMsg(newMsg(), [&popped]() { popped = true; }));
  auto msg = waitTillMsgConsumed();
  ASSERT_EQ(msg_size_, msg->size());
  ASSERT_EQ(sender_, msg->senderId());
  ASSERT_EQ(msg_id_, msg->type());
  ASSERT_TRUE(popped);
}

// Push a message with `msg_id_` that is consumed by a consumer that pushes another message with `msg_id_ + 1` that has
// a callback. Wait for both consumers and make sure the callback was called.
TEST_F(incoming_msgs_storage_test, push_external_from_consumer_thread) {
  // Create a local registrator, storage and consumer to override the default test ones.
  auto popped = std::atomic_bool{false};
  auto reg = std::make_shared<MsgHandlersRegistrator>();
  auto storage = std::unique_ptr<IncomingMsgsStorageImp>{};
  auto consumer1 = std::packaged_task<std::unique_ptr<MessageBase>(MessageBase*)>{[&](MessageBase* msg) {
    storage->pushExternalMsg(newMsg(msg_id_ + 1), [&popped]() { popped = true; });
    return own(msg);
  }};
  auto consumer2 =
      std::packaged_task<std::unique_ptr<MessageBase>(MessageBase*)>{[&](MessageBase* msg) { return own(msg); }};
  reg->registerMsgHandler(msg_id_, [&](MessageBase* msg) { consumer1(msg); });
  reg->registerMsgHandler(msg_id_ + 1, [&](MessageBase* msg) { consumer2(msg); });
  storage = std::make_unique<IncomingMsgsStorageImp>(reg, msg_wait_timeout_, replica_id_);
  storage->start();
  storage->pushExternalMsg(newMsg(msg_id_));
  consumer1.get_future().wait();
  consumer2.get_future().wait();
  ASSERT_TRUE(popped);
  storage->stop();
}

TEST_F(incoming_msgs_storage_test, push_external_raw_with_callback) {
  auto popped = std::atomic_bool{false};
  auto msg_before = newMsg();
  auto buf = buffer();
  auto ptr = buf.data();
  MessageBase::serializeMsg(ptr, msg_before.get());
  ASSERT_TRUE(storage_->pushExternalMsgRaw(buf.data(), buf.size(), [&popped]() { popped = true; }));
  auto msg_after = waitTillMsgConsumed();
  ASSERT_EQ(msg_size_, msg_after->size());
  ASSERT_EQ(sender_, msg_after->senderId());
  ASSERT_EQ(msg_id_, msg_after->type());
  ASSERT_TRUE(popped);
}

TEST_F(incoming_msgs_storage_test, push_external_callback_not_called_before_consume) {
  storage_->stop();
  auto popped = std::atomic_bool{false};
  ASSERT_TRUE(storage_->pushExternalMsg(newMsg(), [&popped]() { popped = true; }));
  ASSERT_FALSE(popped);
}

TEST_F(incoming_msgs_storage_test, push_external_raw_callback_not_called_before_consume) {
  storage_->stop();
  auto popped = std::atomic_bool{false};
  auto msg = newMsg();
  auto buf = buffer();
  auto ptr = buf.data();
  MessageBase::serializeMsg(ptr, msg.get());
  ASSERT_TRUE(storage_->pushExternalMsgRaw(buf.data(), buf.size(), [&popped]() { popped = true; }));
  ASSERT_FALSE(popped);
}

}  // namespace
