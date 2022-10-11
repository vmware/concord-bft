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

#include "wallet.hpp"

#include <iostream>

using namespace vmware::concord::utt::wallet::api::v1;

Wallet::Wallet(std::string userId, utt::client::IUserPKInfrastructure& pki) : userId_{std::move(userId)}, pki_{pki} {
  connect();
}

void Wallet::connect() {
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

  grpc_ = WalletService::NewStub(chan);
}

void Wallet::showInfo() const {
  std::cout << "User Id: " << userId_ << '\n';

  if (!checkOperationl()) return;
  std::cout << "Private balance: " << user_->getBalance() << '\n';
  std::cout << "Privacy budget: " << user_->getPrivacyBudget() << '\n';
  std::cout << "Last executed transaction number: " << user_->getLastExecutedTxNum() << '\n';
}

void Wallet::deployApp() {
  if (isOperational_) {
    std::cout << "The wallet has already deployed an app and is operational.\n";
    return;
  }

  // Generate a privacy config for a N=4 replica system tolerating F=1 failures
  utt::client::ConfigInputParams params;
  params.validatorPublicKeys = std::vector<std::string>{4, "placeholderPublicKey"};  // N = 3 * F + 1
  params.threshold = 2;                                                              // F + 1
  auto config = utt::client::generateConfig(params);
  if (config.empty()) throw std::runtime_error("Failed to generate a privacy app configuration!");

  grpc::ClientContext ctx;

  DeployPrivacyAppRequest req;
  req.set_config(config.data(), config.size());

  DeployPrivacyAppResponse resp;
  grpc_->deployPrivacyApp(&ctx, req, &resp);

  // Note that keeping the config around in memory is just a temp solution and should not happen in real system
  if (resp.has_err()) {
    std::cout << "Failed to deploy privacy app: " << resp.err() << '\n';
  } else if (resp.app_id().empty()) {
    std::cout << "Failed to deploy privacy app: empty app id!\n";
  } else {
    // We need the public config part which can typically be obtained from the service, but we derive it for simplicity
    auto publicConfig = utt::client::getPublicConfig(config);
    std::cout << "Successfully deployed privacy app with id: " << resp.app_id() << '\n';

    user_ = utt::client::createUser(userId_, publicConfig, pki_, storage_);
    if (!user_) throw std::runtime_error("Failed to create user!");

    isOperational_ = true;
  }
}

void Wallet::registerUser() {
  if (!checkOperationl()) return;

  auto userRegInput = user_->getRegistrationInput();
  if (userRegInput.empty()) throw std::runtime_error("Failed to create user registration input!");

  grpc::ClientContext ctx;

  RegisterUserRequest req;
  req.set_user_id(userId_);
  req.set_input_rcm(userRegInput.data(), userRegInput.size());
  req.set_user_pk(user_->getPK());

  RegisterUserResponse resp;
  grpc_->registerUser(&ctx, req, &resp);

  if (resp.has_err()) {
    std::cout << "Failed to register user: " << resp.err() << '\n';
  } else {
    utt::RegistrationSig sig = std::vector<uint8_t>(resp.signature().begin(), resp.signature().end());
    std::cout << "Got sig for registration with size: " << sig.size() << '\n';

    utt::S2 s2 = std::vector<uint64_t>(resp.s2().begin(), resp.s2().end());
    std::cout << "Got S2 for registration: [";
    for (const auto& val : s2) std::cout << val << ' ';
    std::cout << "]\n";

    if (!user_->updateRegistration(user_->getPK(), sig, s2)) {
      std::cout << "Failed to update user's registration!\n";
    }
  }
}

void Wallet::requestPrivacyBudget() {
  if (!checkOperationl()) return;
}

bool Wallet::checkOperationl() const {
  if (!isOperational_) {
    std::cout << "You must first deploy a privacy app. Use command 'deploy app'\n";
    return false;
  }
  return true;
}