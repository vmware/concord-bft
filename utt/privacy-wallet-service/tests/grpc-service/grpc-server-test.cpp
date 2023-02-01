
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

#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
#error "Missing filesystem support"
#endif

#include "testUtils/testUtils.hpp"
#include "gtest/gtest.h"
#include "wallet-api.grpc.pb.h"  // Generated from privacy-wallet-library/proto/api

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include "PrivacyService.hpp"
#include "Wallet.hpp"
#include "types.hpp"
#include <thread>
#include "serialization.hpp"
#include "utils/crypto.hpp"
#include <utt/Address.h>
namespace {
using namespace libutt::api;
using namespace libutt::api::operations;
using namespace utt::walletservice;
using namespace ::vmware::concord::privacy::wallet::api::v1;
class test_privacy_wallet_grpc_service : public libutt::api::testing::test_utt_instance {
 protected:
  void SetUp() override {
    libutt::api::testing::test_utt_instance::setUp(false, false, false);
    server_.StartServer(grpc_uri_);
  }
  void TearDown() override {
    fs::remove_all("wallet-db");
    server_.Shutdown();
  }

  std::pair<grpc::Status, ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse> configureWallet(
      size_t index) {
    auto context = grpc::ClientContext{};
    utt::client::ConfigInputParams params;
    params.threshold = 2;
    params.useBudget = false;
    for (auto i = 0; i < 4; i++) {
      params.validatorPublicKeys.push_back("placeholderPublicKey");
    }
    auto public_config = libutt::api::serialize<libutt::api::PublicConfig>(config->getPublicConfig());
    PrivacyWalletRequest request;
    auto wallet_conf = request.mutable_privacy_wallet_config_request();
    wallet_conf->set_public_key(pkeys[index]);
    wallet_conf->set_private_key(pr_keys[index]);
    wallet_conf->set_public_application_config({public_config.begin(), public_config.end()});
    auto response = ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse{};
    grpc::Status status = stub_->PrivacyWalletService(&context, request, &response);
    if (status.ok()) {
      user_id_ = utt::client::utils::crypto::sha256(
          {pkeys[index].begin(), pkeys[index].end()});  // same user id as the one the wallet creates
    }
    return {status, response};
  }

  std::pair<types::CurvePoint, types::Signature> registerUser(const std::vector<uint8_t>& rcm1) {
    size_t n = registrators.size();
    std::vector<std::vector<uint8_t>> shares;
    std::vector<uint16_t> shares_signers;
    Fr fr_s2 = Fr::random_element();
    types::CurvePoint s2;

    auto deserialized_rcm1 = libutt::api::deserialize<libutt::api::Commitment>(rcm1);

    auto pidHash = AddrSK::pidHash(user_id_).to_words();
    for (auto& r : registrators) {
      auto [ret_s2, sig] = r->signRCM(pidHash, fr_s2.to_words(), deserialized_rcm1);
      shares.push_back(sig);
      shares_signers.push_back(r->getId());
      if (s2.empty()) {
        s2 = ret_s2;
      }
    }
    for (auto& r : registrators) {
      for (size_t i = 0; i < shares.size(); i++) {
        auto& sig = shares[i];
        auto& signer = shares_signers[i];
        assertTrue(r->validatePartialRCMSig(signer, pidHash, s2, deserialized_rcm1, sig));
      }
    }
    auto sids = getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> rsigs;
    for (auto i : sids) {
      rsigs[i] = shares[i];
    }
    auto sig = Utils::unblindSignature(d,
                                       Commitment::Type::REGISTRATION,
                                       {Fr::zero().to_words(), Fr::zero().to_words()},
                                       Utils::aggregateSigShares((uint32_t)n, rsigs));
    return {s2, sig};
  }

  const std::string grpc_uri_{"127.0.0.1:50051"};
  utt::walletservice::PrivacyWalletService server_;
  std::shared_ptr<grpc::Channel> channel_ = grpc::CreateChannel(grpc_uri_, grpc::InsecureChannelCredentials());
  std::unique_ptr<::vmware::concord::privacy::wallet::api::v1::PrivacyWalletService::Stub> stub_ =
      ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletService::NewStub(channel_);
  std::string user_id_;
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

TEST_F(test_privacy_wallet_grpc_service, test_wallet_config_request) {
  auto [status1, response1] = configureWallet(0);
  ASSERT_TRUE(response1.has_privacy_wallet_config_response());
  ASSERT_TRUE(response1.privacy_wallet_config_response().succ());
  ASSERT_TRUE(status1.ok());

  auto [status2, _] = configureWallet(0);
  ASSERT_EQ(status2.error_code(), grpc::StatusCode::ALREADY_EXISTS);
  ASSERT_EQ(status2.error_message(), "wallet is already configured");
  (void)_;
}

TEST_F(test_privacy_wallet_grpc_service, test_generate_registration_input) {
  configureWallet(0);
  auto context = grpc::ClientContext{};
  PrivacyWalletRequest request;
  auto registration_req = request.mutable_user_registration_request();
  (void)registration_req;
  auto response = ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse{};
  grpc::Status status = stub_->PrivacyWalletService(&context, request, &response);
  ASSERT_TRUE(response.has_user_registration_response());
  ASSERT_TRUE(!response.user_registration_response().rcm1().empty());
  ASSERT_TRUE(!response.user_registration_response().rcm1_sig().empty());
  ASSERT_TRUE(!response.user_registration_response().pid().empty());
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

TEST_F(test_privacy_wallet_grpc_service, test_privacy_service_wallet_registration_update) {
  configureWallet(0);
  auto response = ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse{};
  auto context = grpc::ClientContext{};
  PrivacyWalletRequest request;
  auto registration_req = request.mutable_user_registration_request();
  (void)registration_req;
  grpc::Status status = stub_->PrivacyWalletService(&context, request, &response);

  std::vector<uint8_t> rcm1 = {response.user_registration_response().rcm1().begin(),
                               response.user_registration_response().rcm1().end()};

  auto [s2, rcm_sig] = registerUser(rcm1);

  auto response2 = ::vmware::concord::privacy::wallet::api::v1::PrivacyWalletResponse{};
  PrivacyWalletRequest request2;
  auto registration_update_req = request2.mutable_user_registration_update_request();
  registration_update_req->set_rcm_sig({rcm_sig.begin(), rcm_sig.end()});
  *(registration_update_req->mutable_s2()) = {s2.begin(), s2.end()};
  auto context2 = grpc::ClientContext{};
  grpc::Status status2 = stub_->PrivacyWalletService(&context2, request2, &response2);
  std::cout << status.error_message() << std::endl;
  ASSERT_TRUE(status2.ok());
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}