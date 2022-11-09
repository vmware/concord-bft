// UTT Client API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "admin.hpp"

#include <iostream>

using namespace vmware::concord::utt::admin::api::v1;

Admin::Connection Admin::newConnection() {
  std::string grpcServerAddr = "127.0.0.1:49000";

  std::cout << "Connecting to gRPC server at " << grpcServerAddr << "... ";

  auto chan = grpc::CreateChannel(grpcServerAddr, grpc::InsecureChannelCredentials());

  if (!chan) {
    throw std::runtime_error("Failed to create gRPC channel.");
  }
  auto timeoutSec = std::chrono::seconds(5);
  if (chan->WaitForConnected(std::chrono::system_clock::now() + timeoutSec)) {
    std::cout << "Connected.\n";
  } else {
    throw std::runtime_error("Failed to connect to gRPC server after " + std::to_string(timeoutSec.count()) +
                             " seconds.");
  }

  return AdminService::NewStub(chan);
}

bool Admin::deployApp(Channel& chan) {
  std::cout << "Deploying a new privacy application...\n";

  // Generate a privacy config for a N=4 replica system tolerating F=1 failures
  utt::client::ConfigInputParams params;
  params.validatorPublicKeys = std::vector<std::string>{4, "placeholderPublicKey"};  // N = 3 * F + 1
  params.threshold = 2;                                                              // F + 1
  auto config = utt::client::generateConfig(params);
  if (config.empty()) throw std::runtime_error("Failed to generate a privacy app configuration!");

  AdminRequest req;
  req.mutable_deploy()->set_config(config.data(), config.size());
  chan->Write(req);

  AdminResponse resp;
  chan->Read(&resp);
  if (!resp.has_deploy()) throw std::runtime_error("Expected deploy response from admin service!");
  const auto& deployResp = resp.deploy();

  // Note that keeping the config around in memory is just a temp solution and should not happen in real system
  if (deployResp.has_err()) throw std::runtime_error("Failed to deploy privacy app: " + resp.err());

  std::cout << "\nSuccessfully deployed privacy application\n";
  std::cout << "---------------------------------------------------\n";
  std::cout << "Privacy contract: " << deployResp.privacy_contract_addr() << '\n';
  std::cout << "Token contract: " << deployResp.token_contract_addr() << '\n';

  std::cout << "\nYou are now ready to configure wallets.\n";

  return true;
}

void Admin::createPrivacyBudget(Channel& chan, const std::string& user, uint64_t value) {
  AdminRequest req;
  auto& budgerReq = *req.mutable_create_budget();
  budgerReq.set_user_id(user);
  budgerReq.set_expiration_date(1919241632);
  budgerReq.set_value(value);
  chan->Write(req);

  std::cout << "Budget request for user: " << user << " value: " << value << " was sent to the privacy app\n";

  AdminResponse resp;
  chan->Read(&resp);
  if (!resp.has_create_budget()) throw std::runtime_error("Expected create_budget response from admin service!");
  std::cout << "response: " << resp.create_budget().status() << '\n';
}