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

  std::cout << "Connecting to gRPC server at " << grpcServerAddr << " ...\n";

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
  std::cout << "response case: " << resp.resp_case() << '\n';
  const auto& deployResp = resp.deploy();
  std::cout << "has_privacy_contract_addr:" << deployResp.has_privacy_contract_addr() << '\n';
  std::cout << "has_token_contract_addr:" << deployResp.has_token_contract_addr() << '\n';

  // Note that keeping the config around in memory is just a temp solution and should not happen in real system
  if (deployResp.has_err()) throw std::runtime_error("Failed to deploy privacy app: " + resp.err());

  std::cout << "\nDeployed privacy application\n";
  std::cout << "-----------------------------------\n";
  std::cout << "Privacy contract: " << deployResp.privacy_contract_addr() << '\n';
  std::cout << "Token contract: " << deployResp.token_contract_addr() << '\n';

  return true;
}

void Admin::createPrivacyBudget(Channel& chan) {
  (void)chan;
  // [TODO-UTT] Create budget is done locally, should be done by the system
  // grpc::ClientContext ctx;

  // CreatePrivacyBudgetRequest req;
  // req.set_user_id(userId_);

  // CreatePrivacyBudgetResponse resp;
  // conn->createPrivacyBudget(&ctx, req, &resp);

  // if (resp.has_err()) {
  //   std::cout << "Failed to create privacy budget:" << resp.err() << '\n';
  // } else {
  //   utt::PrivacyBudget budget = std::vector<uint8_t>(resp.budget().begin(), resp.budget().end());
  //   utt::RegistrationSig sig = std::vector<uint8_t>(resp.signature().begin(), resp.signature().end());

  //   std::cout << "Got budget " << budget.size() << " bytes.\n";
  //   std::cout << "Got budget sig " << sig.size() << " bytes.\n";

  //   user_->updatePrivacyBudget(budget, sig);
  // }
}