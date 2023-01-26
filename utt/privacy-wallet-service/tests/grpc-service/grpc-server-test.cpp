
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
#include "PrivacyService.hpp"
#include "Wallet.hpp"
#include <thread>

namespace {
using namespace libutt::api;
using namespace libutt::api::operations;
using namespace utt::walletservice;
using namespace ::vmware::concord::privacy::wallet::api::v1;
std::string cert =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDazCCAlMCFEHQ6KJVVZyrj5SnSHZQkkRsvWoYMA0GCSqGSIb3DQEBCwUAMHIx\n"
    "CzAJBgNVBAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1Nb3Vu\n"
    "dGFpbiBWaWV3MRMwEQYDVQQKDApNeSBDb21wYW55MQswCQYDVQQLDAJJVDEUMBIG\n"
    "A1UEAwwLZXhhbXBsZS5jb20wHhcNMjMwMTE5MDQ1NTI0WhcNMjQwMTE5MDQ1NTI0\n"
    "WjByMQswCQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwN\n"
    "TW91bnRhaW4gVmlldzETMBEGA1UECgwKTXkgQ29tcGFueTELMAkGA1UECwwCSVQx\n"
    "FDASBgNVBAMMC2V4YW1wbGUuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB\n"
    "CgKCAQEAua5OPJ1GOYJSxCs8cE/6zO2B3pdhdVrzrbII7aONQAUW2+iYb9m2WiPC\n"
    "be26WQyJeZ2WP0uWvC5D+3YdO4fDPcF5FLGXtMGei8B2Pah+gTE0wzlF6vthK4v0\n"
    "B4SoZ2WRgvHj3tr2O8frhWgtryReJZg4D49UR0WRbJ85HLSVq4mrb0BXY58E2Wqi\n"
    "XnfLMQO/aiJ+LFhnnpVOn3Vf8/XTdLav1hM2CIY0P0I9J9SI5qGSo4Tbn3xu8lOr\n"
    "KjtDFnt4BpLW4wMZ1d5OieGePoqq4j8n2mdjhWurLVDESxlytO8Qcs2zTrRDNxrQ\n"
    "O3D6DKbgjKWcJ5Tq5MgjaWtzUy4fvQIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQBw\n"
    "NhgLBjb7MDLRh0zExs7v2xNztDAU7s50wXTZx87EVbGYzJFT4CpYb7igYv23AGHH\n"
    "fxZIYVRvoD0xndfG2zfEIparrFenp9zmlxddnI/CWgmvIpk5/nGAeKCODHesarvy\n"
    "MIyu8HA2ZachTZhfmXmRtJrtYOHQl/5EErPhSDglK5gHQivtS/D6V59mC88fMi3U\n"
    "3y946/RM3Z+X5PTeICXiKR+BSZ4+bRzUDvxMp+rLBozwaetYT3zZdP4QAn9Si+28\n"
    "3lqvsB0uOj606AcfhA+PZgbEZpyU3HOzDNrV7bk6VU0vE7rAp09u1F58wmA81YQr\n"
    "LhsFkWCqmGWU85I7zacS\n"
    "-----END CERTIFICATE-----";

std::string private_key =
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC5rk48nUY5glLE\n"
    "KzxwT/rM7YHel2F1WvOtsgjto41ABRbb6Jhv2bZaI8Jt7bpZDIl5nZY/S5a8LkP7\n"
    "dh07h8M9wXkUsZe0wZ6LwHY9qH6BMTTDOUXq+2Eri/QHhKhnZZGC8ePe2vY7x+uF\n"
    "aC2vJF4lmDgPj1RHRZFsnzkctJWriatvQFdjnwTZaqJed8sxA79qIn4sWGeelU6f\n"
    "dV/z9dN0tq/WEzYIhjQ/Qj0n1IjmoZKjhNuffG7yU6sqO0MWe3gGktbjAxnV3k6J\n"
    "4Z4+iqriPyfaZ2OFa6stUMRLGXK07xByzbNOtEM3GtA7cPoMpuCMpZwnlOrkyCNp\n"
    "a3NTLh+9AgMBAAECggEANWXSLA5SprE62h1Q+T+W8Z4P7hJ8vYIVd8suVCDnuxR7\n"
    "mWxPgkMK9Os5u+FU6Mz5MBdIoRU82Qs5E7TI/ViypizghDn6VcokrS4BEwREtSSQ\n"
    "duAeok/+hsZtvEfDIlEMQqsLjAhOLaz1p1zpXmfIB2m6HYdrhj+UbbdwdjfcnwKv\n"
    "5tk87EfKOuuJxxFXP0+HkjcwY0cMLohO/XwK8Rwr/5YXXe4pEDMnk2qoZPmsT7wj\n"
    "6FRUkc96GZIxXaIsb7Dr5KId7RWZGqnCMOa5SKq0MjpKDlZf64XozgjuBy8iJ3VD\n"
    "uuIO/s2PhV0mlcD+sd4yBpcUCyDGxTUtHUZ2sm6YYQKBgQDh7wYSi74UECVQxuM+\n"
    "vBTLKvSRn7niHbVKwD5qaYc3GJBZlU+FXC/3v9KBv+CfJXBs3gNc1oz44ME/9vzk\n"
    "iKSgoK0Yqkl8TFv4pKl5Hc+Y5FpOtHAYjPbKbqcsq/DIRX9/S31x/OsO8Y0jmi7f\n"
    "/5OOdy6SDOzzo6xhsXHV/eh6SQKBgQDSY/kSxOE+/OO6f9zVXJ9tRn2GsngkonyK\n"
    "DFnLPbMmQi1+7xiLKluJaTKjVAoCRDw4/DOcgLM1PGwrcZGo5WCQ05SeDd0GHA8E\n"
    "bcVhFARkvaA5BcuiJeTrjsI0NKpaLbOhEidpm9VQ3XUQAUFjrFMmHk5gcuhgy4HJ\n"
    "ztrLhFVZ1QKBgEh7Z0ZR4JQNLfuBIuxAaKdZS4bgaED7aOrnS97VphRt4/lpZk6R\n"
    "aa4gswb/KK/F0hCLFScWiblaWYUM1sr2b2I8yetszhB7atIU+W2qu6wALlyrlH67\n"
    "0nzVDPrO2ntVmHadIEyOaFat9aqjT0B7fLoq0Bz42pe7PZVF2RBe2dNJAoGAVZ1D\n"
    "JSUi+AvW6/TOO7DmW4R83kxP4bCRd2fRPoiMF3yEoQvQ9Ai3mTJK3fX74LI9w361\n"
    "zfD9fCNrbT5Y5N76rdS7vJmtoKfYYJf+4yNPKmOUCMBX/lLnVggQ9UedLvc8Csal\n"
    "bS9x3edQlMO+BT6B05gvksYP1BvcY/AeTwU56kUCgYA3FknRNCWuHT2IWUh48Mil\n"
    "lxFM5ta4OmSfTQpnX3XBWHF9Y4pOOpmVq8gqTM2H5usDcwj50IN07KJp85YLLttf\n"
    "9fSeBocDjfg08irMKJ7p1jB2sTWBrLK/8PniRHrwRlbsOXPZQDvxG/9hGLmDd7Nt\n"
    "ugW9Dp+NZ6L5qPZtbK/8xQ==\n"
    "-----END PRIVATE KEY-----";
class test_privacy_wallet_grpc_service : public libutt::api::testing::test_utt_instance {
 protected:
  void SetUp() override {
    libutt::api::testing::test_utt_instance::setUp(false, true);
    server_.StartServer(grpc_uri_);
  }
  void TearDown() override { server_.Shutdown(); }

  void configureWallet() {
    auto context = grpc::ClientContext{};
    utt::client::ConfigInputParams params;
    params.threshold = 2;
    params.useBudget = false;
    for (auto i = 0; i < 4; i++) {
      params.validatorPublicKeys.push_back("placeholderPublicKey");
    }
    auto config = utt::client::generateConfig(params);
    auto public_config = utt::client::getPublicConfig(config);
    PrivacyWalletRequest request;
    auto wallet_conf = request.mutable_privacy_wallet_config_request();
    wallet_conf->set_cert(cert);
    wallet_conf->set_private_key(private_key);
    wallet_conf->set_public_application_config({public_config.begin(), public_config.end()});
    wallet_conf->set_storage_path(".");
    auto response = ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse{};
    grpc::Status status = stub_->PrivacyWalletService(&context, request, &response);
    ASSERT_TRUE(response.has_privacy_wallet_config_response());
    ASSERT_TRUE(response.privacy_wallet_config_response().succ());
    ASSERT_TRUE(status.ok());
  }

 protected:
  const std::string grpc_uri_{"127.0.0.1:50051"};
  utt::walletservice::PrivacyWalletService server_;
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
}

TEST_F(test_privacy_wallet_grpc_service, test_wallet_config_request) { configureWallet(); }

TEST_F(test_privacy_wallet_grpc_service, test_generate_registration_input) {
  configureWallet();
  auto context = grpc::ClientContext{};
  PrivacyWalletRequest request;
  auto registration_req = request.mutable_user_registration_request();
  (void)registration_req;
  auto response = ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse{};
  grpc::Status status = stub_->PrivacyWalletService(&context, request, &response);
  ASSERT_TRUE(response.has_user_registration_response());
  ASSERT_TRUE(!response.user_registration_response().rcm1().empty());
  ASSERT_TRUE(!response.user_registration_response().rcm1_sig().empty());
  ASSERT_TRUE(status.ok());
}

TEST_F(test_privacy_wallet_grpc_service, test_generate_registration_input_wallet_not_configured) {
  auto context = grpc::ClientContext{};
  PrivacyWalletRequest request;
  auto registration_req = request.mutable_user_registration_request();
  (void)registration_req;
  auto response = ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse{};
  grpc::Status status = stub_->PrivacyWalletService(&context, request, &response);
  ASSERT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
  ASSERT_EQ(status.error_message(), "wallet is not configured");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}