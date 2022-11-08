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
#include "api.grpc.pb.h"  // Generated from utt/admin/proto/api

#include <utt-client-api/ClientApi.hpp>

namespace AdminApi = vmware::concord::utt::admin::api::v1;

class Admin {
 public:
  using Connection = std::unique_ptr<AdminApi::AdminService::Stub>;
  using Channel = std::unique_ptr<grpc::ClientReaderWriter<AdminApi::AdminRequest, AdminApi::AdminResponse>>;

  static Connection newConnection();

  /// [TODO-UTT] Should be performed by an admin app
  /// @brief Deploy a privacy application
  /// @return The public configuration of the deployed application
  static bool deployApp(Channel& chan);

  /// @brief Request the creation of a privacy budget. The amount of the budget is predetermined by the deployed app.
  /// This operation could be performed entirely by an administrator, but we add it in the admin
  /// for demo purposes.
  static void createPrivacyBudget(Channel& chan, const std::string& user, uint64_t value);
};