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

#include <grpcpp/grpcpp.h>
#include "api.grpc.pb.h"  // Generated from utt/wallet/proto/api

#include <utt-client-api/ClientApi.hpp>

class Wallet {
 public:
  Wallet();

  void showUser(const std::string& userId);
  void deployApp();
  void registerUser(const std::string& userId);

 private:
  void connect();

  struct DummyUserStorage : public utt::client::IUserStorage {};

  using GrpcService = vmware::concord::utt::wallet::api::v1::WalletService::Stub;

  std::unique_ptr<utt::PublicConfig> deployedPublicConfig_;
  std::string deployedAppId_;
  utt::client::TestUserPKInfrastructure pki_;
  DummyUserStorage storage_;
  std::map<std::string, std::unique_ptr<utt::client::User>> users_;
  std::unique_ptr<GrpcService> grpc_;
};