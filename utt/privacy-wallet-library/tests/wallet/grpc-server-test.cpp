
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
#include "../../../tests/testUtils.hpp"
#include "gtest/gtest.h"
#include "wallet-api.grpc.pb.h"  // Generated from privacy-wallet-library/proto/api

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include "wallet.hpp"
#include <thread>

namespace {
using namespace libutt::api;
using namespace libutt::api::operations;

class test_privacy_wallet_grpc_service : public libutt::api::testing::test_utt_instance {
 protected:
  void SetUp() override {
    libutt::api::testing::test_utt_instance::setUp(false, true);
    server_.StartServer(grpc_uri_);
  }
  void TearDown() override {
    server_.Wait();
    std::cout << "tear down \n";
  }

 protected:
  const std::string grpc_uri_{"127.0.0.1:50051"};
  PrivacyWalletService server_;
  std::shared_ptr<grpc::Channel> channel_ = grpc::CreateChannel(grpc_uri_, grpc::InsecureChannelCredentials());
  std::unique_ptr<::vmware::concord::privacy::wallet::api::v1::PrivacyWalletService::Stub> stub_ =
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletService::NewStub(channel_);
};

TEST_F(test_privacy_wallet_grpc_service, privacy_app_config) {
  auto context = grpc::ClientContext{};
  ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletRequest request;
  auto appCfg = request.mutable_privacy_app_config();
  appCfg->set_budget(true);
  appCfg->set_numvalidators(2);
  for (auto i = 0; i < 4; i++) {
    appCfg->add_validatorpublickey("placeholderPublicKey");
  }

  auto response = ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse{};

  grpc::Status status = stub_->PrivacyWalletService(&context, request, &response);

  // Act upon its status.
  if (!status.ok()) {
    std::cout << "Failed to get config: " << status.error_message() << std::endl;
    ASSERT_TRUE(true);
  }

  std::cout << "test passed!\n";
  std::cout << "Shutting down the grpc server...\n";
  server_.Shutdown();
  std::cout << "srv thread joined.. \n";
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}