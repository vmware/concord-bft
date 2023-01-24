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

#include "PrivacyService.hpp"
#include "FileBasedUserStorage.hpp"
#include <iostream>
#include <utt-client-api/ClientApi.hpp>

PrivacyWalletService::PrivacyWalletService() : privacy_wallet_service_(std::make_unique<PrivacyWalletServiceImpl>()) {
  utt::client::Initialize();
}

PrivacyWalletService::~PrivacyWalletService() { std::cout << " Destroying privacy wallet service...\n"; }

void PrivacyWalletService::StartServer(const std::string& url) {
  std::string server_address(url);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(privacy_wallet_service_.get());
  grpc_server_ = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;
}

void PrivacyWalletService::Wait() {
  // if (grpc_server_ != nullptr) {
  std::cout << "wait for server to terminate" << std::endl;
  grpc_server_->Wait();
  std::cout << "server wait terminated" << std::endl;
  //}
}
void PrivacyWalletService::Shutdown() {
  if (grpc_server_ != nullptr) {
    std::cout << "server shutdown" << std::endl;
    grpc_server_->Shutdown();
    std::cout << "server shutdown complete.." << std::endl;
  }
}

::grpc::Status PrivacyWalletServiceImpl::PrivacyWalletService(
    ::grpc::ServerContext* context,
    const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
    ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response) {
  auto status = grpc::Status::OK;
  std::cout << "Processing privacy wallet service message.....!" << std::endl;
  if (request->has_privacy_app_config()) {
    return handleApplicationConfigRequest(context, request, response);
  } else {
    std::cout << "unknown request: " << request->DebugString() << std::endl;
    status = grpc::Status(grpc::StatusCode::UNKNOWN, "Unknown error");
  }
  return status;
}

// @FIXME - make this asynchronous..
::grpc::Status PrivacyWalletServiceImpl::handleApplicationConfigRequest(
    ::grpc::ServerContext* /*context*/,
    const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
    ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response) {
  auto status = grpc::Status::OK;
  std::cout << "Processing privacy app config request: " << request->DebugString() << std::endl;

  // Generate a privacy config for a N=4 replica system tolerating F=1 failures
  utt::client::ConfigInputParams params;
  for (auto i = 0; i < request->privacy_app_config().validatorpublickey_size(); i++) {
    params.validatorPublicKeys.emplace_back(request->privacy_app_config().validatorpublickey(i));
  }
  params.threshold = uint16_t(request->privacy_app_config().numvalidators());
  params.useBudget = request->privacy_app_config().budget();  // F + 1
  auto config = utt::client::generateConfig(params);
  if (config.empty()) {
    std::cout << "failed to generate config!" << std::endl;
    status = grpc::Status(grpc::StatusCode::INTERNAL, "Failed to create config");
  } else {
    auto configResp = response->mutable_privacy_app_config_response();
    configResp->set_configuration(config.data(), config.size());
    std::cout << "Successfully send back application config!" << std::endl;
  }
  return status;
}