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
#include <storage/FileBasedUserStorage.hpp>
#include <iostream>
#include <utt-client-api/ClientApi.hpp>
#include <utils/crypto.hpp>

using namespace utt::client::utils::crypto;
namespace utt::walletservice {
PrivacyWalletService::PrivacyWalletService() : privacy_wallet_service_(std::make_unique<PrivacyWalletServiceImpl>()) {
  utt::client::Initialize();
}

PrivacyWalletService::~PrivacyWalletService() { std::cout << " Destroying privacy wallet service...\n"; }

void PrivacyWalletService::StartServer(const std::string& url) {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(url, grpc::InsecureServerCredentials());
  builder.RegisterService(privacy_wallet_service_.get());
  grpc_server_ = builder.BuildAndStart();
  std::cout << "Server listening on " << url << std::endl;
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

const std::string PrivacyWalletServiceImpl::wallet_db_path = "wallet-db";

::grpc::Status PrivacyWalletServiceImpl::PrivacyWalletService(
    ::grpc::ServerContext* context,
    const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
    ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response) {
  auto status = grpc::Status::OK;
  std::cout << "Processing privacy wallet service message.....!" << std::endl;
  if (request->has_privacy_app_config()) {
    return handleApplicationConfigRequest(context, request, response);
  } else if (request->has_privacy_wallet_config_request()) {
    return handleWalletConfigRequest(context, request, response);
  } else if (request->has_user_registration_request()) {
    return handleUserRegistrationRequest(context, request, response);
  } else if (request->has_user_registration_update_request()) {
    return handleUserRegistrationUpdateRequest(context, request, response);
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
  std::cout << "Processing privacy app config request" << request->DebugString() << std::endl;

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

::grpc::Status PrivacyWalletServiceImpl::handleWalletConfigRequest(
    ::grpc::ServerContext* /*context*/,
    const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
    ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response) {
  if (wallet_) {
    std::string err_msg = "wallet is already configured";
    std::cout << err_msg << std::endl;
    response->set_err(err_msg);
    return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, err_msg);
  }
  auto status = grpc::Status::OK;
  const auto& req = request->privacy_wallet_config_request();
  auto public_key = req.public_key();
  auto userId = sha256({public_key.begin(), public_key.end()});
  utt::PublicConfig publicConfig(req.public_application_config().begin(), req.public_application_config().end());
  wallet_ = std::make_unique<Wallet>(
      userId, req.private_key(), public_key, PrivacyWalletServiceImpl::wallet_db_path, publicConfig);
  auto resp = response->mutable_privacy_wallet_config_response();
  resp->set_succ(true);
  return grpc::Status::OK;
}

::grpc::Status PrivacyWalletServiceImpl::handleUserRegistrationRequest(
    ::grpc::ServerContext*,
    const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest*,
    ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response) {
  if (!wallet_) {
    std::string err_msg = "wallet is not configured";
    std::cout << err_msg << std::endl;
    response->set_err(err_msg);
    return grpc::Status(grpc::StatusCode::NOT_FOUND, err_msg);
  }
  auto registration_input = wallet_->generateRegistrationInput();
  if (!registration_input.has_value()) {
    std::string err_msg = "error while generating registration input";
    std::cout << err_msg << std::endl;
    response->set_err(err_msg);
    return grpc::Status(grpc::StatusCode::INTERNAL, err_msg);
  }
  auto reg_response = response->mutable_user_registration_response();
  reg_response->set_rcm1(registration_input->rcm1.data(), registration_input->rcm1.size());
  reg_response->set_rcm1_sig(registration_input->rcm1_sig.data(), registration_input->rcm1_sig.size());
  reg_response->set_pid(wallet_->getUserId());
  return grpc::Status::OK;
}

::grpc::Status PrivacyWalletServiceImpl::handleUserRegistrationUpdateRequest(
    ::grpc::ServerContext*,
    const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
    ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response) {
  if (!wallet_) {
    std::string err_msg = "wallet is not configured";
    std::cout << err_msg << std::endl;
    response->set_err(err_msg);
    return grpc::Status(grpc::StatusCode::NOT_FOUND, err_msg);
  }
  const auto& req = request->user_registration_update_request();
  auto res = wallet_->updateRegistrationCommitment({req.rcm_sig().begin(), req.rcm_sig().end()},
                                                   {req.s2().begin(), req.s2().end()});
  if (!res) {
    std::string err_msg = "user is already registered";
    std::cout << err_msg << std::endl;
    response->set_err(err_msg);
    return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, err_msg);
  }
  auto resp = response->mutable_user_registration_update_response();
  resp->set_succ(true);
  return grpc::Status::OK;
}
}  // namespace utt::walletservice