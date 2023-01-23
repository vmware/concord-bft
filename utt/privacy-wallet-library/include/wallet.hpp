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

#pragma once

#include <string>
#include <memory>
#include <tuple>

#include <grpcpp/grpcpp.h>
#include "wallet-api.grpc.pb.h"  // Generated from privacy-wallet-library/proto/api

#include <utt-client-api/ClientApi.hpp>

class PrivacyWalletServiceImpl final : public vmware::concord::privacy::wallet::api::v1::PrivacyWalletService::Service {
 public:
  PrivacyWalletServiceImpl() {}
  ::grpc::Status PrivacyWalletService(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response) override;

 private:
};
class PrivacyWalletService {
 public:
  PrivacyWalletService() : privacy_wallet_service_(std::make_unique<PrivacyWalletServiceImpl>()) {
    utt::client::Initialize();
  }

  ~PrivacyWalletService() { std::cout << " Done.\n"; }

  void StartServer(const std::string& url) {
    std::string server_address(url);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(privacy_wallet_service_.get());
    grpc_server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;
  }

  void Wait() {
    // if (grpc_server_ != nullptr) {
    std::cout << "wait for server to terminate" << std::endl;
    grpc_server_->Wait();
    std::cout << "server wait terminated" << std::endl;
    //}
  }
  void Shutdown() {
    if (grpc_server_ != nullptr) {
      std::cout << "server shutdown" << std::endl;
      grpc_server_->Shutdown();
      std::cout << "server shutdown complete.." << std::endl;
    }
  }

 private:
  std::unique_ptr<grpc::Server> grpc_server_;
  std::unique_ptr<PrivacyWalletServiceImpl> privacy_wallet_service_;
};

class Wallet {
 public:
  Wallet(std::string userId, utt::client::TestUserPKInfrastructure& pki, const utt::PublicConfig& config);

 private:
  std::unique_ptr<utt::client::IStorage> storage_;
  std::string userId_;
  utt::client::TestUserPKInfrastructure& pki_;
  std::unique_ptr<utt::client::User> user_;
  bool registered_ = false;
};