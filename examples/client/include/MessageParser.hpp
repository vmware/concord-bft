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

#pragma once

#include "Logger.hpp"
#include <yaml-cpp/yaml.h>

namespace concord::osexample {
enum MessageType : uint16_t { WRITE = 0, READ = 1, BOTH = 2 };

struct MessageConfig {
  uint16_t messageType = MessageType::WRITE;
  uint16_t senderNodeId = 5;  // default client id for a single client
  uint64_t reqTimeoutMilli = 1000;
  uint16_t numberOfOperation = 1;
  uint16_t numOfWrites = 1;
  uint16_t numOfKeysInReadSet = 1;
};

// This class is responsible to parse the the msg config file to create a MessageConfig.
// This MessageParser is used to create a message config for Key-Value message or ethereum related msgs
// that is used to send to the replica.
class MessageParser {
 public:
  MessageParser(const std::string& msgConfigFile) : msgConfigFileName_(msgConfigFile) {}

  std::shared_ptr<MessageConfig> getConfigurationForMessage() {
    try {
      auto yaml = YAML::LoadFile(msgConfigFileName_);

      std::shared_ptr<MessageConfig> msgConfig = std::make_shared<MessageConfig>();

      // Preparing msgConfig object
      msgConfig->messageType = yaml["type"].as<std::uint16_t>();
      msgConfig->numberOfOperation = yaml["number_of_operations"].as<std::uint16_t>();
      msgConfig->senderNodeId = yaml["sender_node_id"].as<std::uint16_t>();
      msgConfig->reqTimeoutMilli = yaml["req_timeout_milli"].as<std::uint64_t>();
      msgConfig->numOfWrites = yaml["num_of_writes"].as<std::uint64_t>();
      msgConfig->numOfKeysInReadSet = yaml["num_of_keys_in_readset"].as<std::uint64_t>();

      return msgConfig;
    } catch (std::exception& e) {
      LOG_ERROR(getLogger(), "Failed to load msg config file" << e.what());
      return nullptr;
    }
  }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("osexample::MessageParser"));
    return logger_;
  }

 private:
  std::string msgConfigFileName_;
};
}  // namespace concord::osexample
