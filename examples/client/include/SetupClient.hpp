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

#include "communication/CommFactory.hpp"
#include "tests/config/test_comm_config.hpp"
#include "bftclient/bft_client.h"
#include "secrets_manager_impl.h"
#include "secrets_manager_plain.h"
#include "secrets_manager_enc.h"

namespace concord::osexample {

enum ExecutionEngineType : uint16_t { KV = 0, WASM = 1, ETHEREUM = 2 };

struct ClientParams {
  uint16_t clientId = 4;
  uint16_t numOfReplicas = 4;
  uint16_t numOfClients = 1;
  uint16_t numOfFaulty = 1;
  uint16_t numOfSlow = 0;
  bool measurePerformance = false;
  uint16_t get_numOfReplicas() { return (uint16_t)(3 * numOfFaulty + 2 * numOfSlow + 1); }
};

class SetupClient {
 public:
  SetupClient();
  void ParseClientArgs(int argc, char** argv);
  bft::communication::ICommunication* createCommunication();
  void setupClientParams(int argc, char** argv);
  bft::client::ClientConfig setupClientConfig();
  std::string getLogPropertiesFile() const { return logPropsFile_; }
  ClientParams getClientParams() const { return clientParams_; }
  std::string getMsgConfigFile() { return msgConfigFile_; }
  uint16_t getExecutionEngineType() { return executionEngineType_; }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("osexample::SetupClient"));
    return logger_;
  }

 private:
  std::string logPropsFile_;
  ClientParams clientParams_;
  uint16_t numOfReplicas_;
  std::string msgConfigFile_;
  uint16_t executionEngineType_ = ExecutionEngineType::KV;  // default is KV
};
}  // end namespace concord::osexample
