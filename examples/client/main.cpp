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

#include "SetupClient.hpp"
#include "MessageSender.hpp"
#include "MessageParser.hpp"

using namespace concord::osexample;

int main(int argc, char **argv) {
  try {
    std::shared_ptr<SetupClient> setupClient = std::make_shared<SetupClient>();
    ConcordAssert(setupClient != nullptr);
    setupClient->ParseClientArgs(argc, argv);
    logging::initLogger(setupClient->getLogPropertiesFile());

    // parse message config file
    MessageParser msgParser(setupClient->getMsgConfigFile());
    std::shared_ptr<MessageConfig> msgConfig = msgParser.getConfigurationForMessage();
    ConcordAssert(msgConfig != nullptr);

    // Create MessageSender object and initialize bft client
    std::unique_ptr<MessageSender> msgSender = std::make_unique<MessageSender>(setupClient, msgConfig);
    ConcordAssert(msgSender != nullptr);

    // create and send client request
    msgSender->sendRequests();
    msgSender->stopBftClient();

    LOG_INFO(GL, "All Good! ");

  } catch (const std::exception &e) {
    LOG_ERROR(GL, "Exception: " << e.what());
  }
}
