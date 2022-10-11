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

Wallet::Wallet() { connect(); }

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

void Wallet::showUser(const std::string& userId) {
  auto it = users_.find(userId);
  if (it == users_.end()) {
    std::cout << "User '" << userId << "' does not exist.\n";
    return;
  }

  std::cout << "User Id: " << it->first << '\n';
  std::cout << "Private balance: " << it->second->getBalance() << '\n';
  std::cout << "Privacy budget: " << it->second->getPrivacyBudget() << '\n';
  std::cout << "Last executed transaction number: " << it->second->getLastExecutedTxNum() << '\n';
}

void Wallet::deployApp() {
  if (!deployedAppId_.empty()) {
    std::cout << "Privacy app '" << deployedAppId_ << "' already deployed\n";
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

  // Note that keeping the config around in memory is just for a demo purpose and should not happen in real system
  if (resp.has_err()) {
    std::cout << "Failed to deploy privacy app: " << resp.err() << '\n';
  } else if (resp.app_id().empty()) {
    std::cout << "Failed to deploy privacy app: empty app id!\n";
  } else {
    deployedAppId_ = resp.app_id();
    // We need the public config part which can typically be obtained from the service, but we keep it for simplicity
    deployedPublicConfig_ = std::make_unique<utt::PublicConfig>(utt::client::getPublicConfig(config));
    std::cout << "Successfully deployed privacy app with id: " << deployedAppId_ << '\n';
  }
}

void Wallet::registerUser(const std::string& userId) {
  if (userId.empty()) throw std::runtime_error("Cannot register user with empty user id!");
  if (deployedAppId_.empty()) {
    std::cout << "You must first deploy a privacy app to register a user. Use command 'deploy app'\n";
    return;
  }

  if (users_.find(userId) != users_.end()) {
    std::cout << "User '" << userId << " is already registered!\n";
    return;
  }

  // Print out the list of valid user ids with pre-generated keys if we don't have a match.
  // This is a temp code until we can generate keys on demand for every user id.
  auto userIds = pki_.getUserIds();
  auto it = std::find(userIds.begin(), userIds.end(), userId);
  if (it == userIds.end()) {
    std::cout << "Please use one of the following userIds:\n[";
    for (const auto& userId : userIds) std::cout << userId << ' ';
    std::cout << "]\n";
    return;
  }

  auto user = utt::client::createUser(userId, *deployedPublicConfig_, pki_, storage_);
  if (!user) throw std::runtime_error("Failed to create user!");

  auto userRegInput = user->getRegistrationInput();
  if (userRegInput.empty()) throw std::runtime_error("Failed to create user registration input!");

  grpc::ClientContext ctx;

  RegisterUserRequest req;
  req.set_user_id(userId);
  req.set_input_rcm(userRegInput.data(), userRegInput.size());
  req.set_user_pk(user->getPK());

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

    if (!user->updateRegistration(user->getPK(), sig, s2)) {
      std::cout << "Failed to update user's registration!\n";
    } else {
      std::cout << "Successfully registered user with id '" << user->getUserId() << "'\n";
      users_.emplace(userId, std::move(user));
    }
  }
}