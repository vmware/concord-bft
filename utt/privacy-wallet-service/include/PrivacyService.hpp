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
#include <string_view>
#include <grpcpp/grpcpp.h>
#include "wallet-api.grpc.pb.h"  // Generated from privacy-wallet-service/proto/api
#include "Wallet.hpp"
#include <storage/IStorage.hpp>
#include <utt-client-api/ClientApi.hpp>

namespace utt::walletservice {
//@TODO hide on its own file..
class PrivacyWalletServiceImpl final : public vmware::concord::privacy::wallet::api::v1::PrivacyWalletService::Service {
 public:
  PrivacyWalletServiceImpl();
  ::grpc::Status PrivacyWalletService(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response) override;

 protected:
  ::grpc::Status handleApplicationConfigRequest(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleWalletConfigRequest(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleUserRegistrationRequest(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleUserRegistrationUpdateRequest(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleUserClaimCoinsRequest(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleUserMintRequest(::grpc::ServerContext* context,
                                       const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
                                       ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleUserBurnRequest(::grpc::ServerContext* context,
                                       const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
                                       ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleUserTransferRequest(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleGetStateRequest(::grpc::ServerContext* context,
                                       const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
                                       ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleSetAppDataRequest(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

  ::grpc::Status handleGetAppDataRequest(
      ::grpc::ServerContext* context,
      const ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest* request,
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse* response);

 private:
  std::pair<utt::Transaction, utt::TxOutputSigs> buildClaimCoinsData(
      const ::vmware::concord::privacy::wallet::api::v1::ClaimCoinsRequest& req);
  std::unique_ptr<Wallet> wallet_;
  static const std::string wallet_db_path;
  std::shared_ptr<utt::client::IStorage> storage_;
};

class PrivacyWalletService {
 public:
  PrivacyWalletService();
  ~PrivacyWalletService();
  void StartServer(const std::string& url);
  void Wait();
  void Shutdown();

 private:
  std::unique_ptr<grpc::Server> grpc_server_;
  std::unique_ptr<PrivacyWalletServiceImpl> privacy_wallet_service_;
};
}  // namespace utt::walletservice