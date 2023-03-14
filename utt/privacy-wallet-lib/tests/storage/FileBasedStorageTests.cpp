
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
#include "mint.hpp"
#include <storage/FileBasedUserStorage.hpp>
#include "gtest/gtest.h"
namespace {
using namespace utt::client;
using namespace libutt::api;
using namespace libutt::api::operations;
class test_utt_storage : public libutt::api::testing::test_utt_instance {
 protected:
  void SetUp() override {
    libutt::api::testing::test_utt_instance::setUp(false, true);
    restartStorage();
  }
  void TearDown() override { ASSERT_TRUE(fs::remove_all(storage_path)); }

  void restartStorage() { storage_ = std::make_unique<utt::client::FileBasedUserStorage>(storage_path); }

  std::string storage_path = "./test_storage";
  std::unique_ptr<utt::client::IStorage> storage_;
};
TEST_F(test_utt_storage, test_isNewStorage) {
  ASSERT_TRUE(storage_->isNewStorage());
  { IStorage::tx_guard g(*storage_); }
  ASSERT_FALSE(storage_->isNewStorage());
}

TEST_F(test_utt_storage, test_isNewStorage_negative) {
  ASSERT_TRUE(storage_->isNewStorage());
  storage_->setKeyPair({"private_key", "public_key"});
  ASSERT_TRUE(storage_->isNewStorage());
}

TEST_F(test_utt_storage, test_setKeyPair) {
  std::pair<std::string, std::string> keypair = {pr_keys.front(), pkeys.front()};
  {
    IStorage::tx_guard g(*storage_);
    storage_->setKeyPair(keypair);
  }
  restartStorage();
  ASSERT_EQ(storage_->getKeyPair(), keypair);
}

TEST_F(test_utt_storage, test_setSystemSideSecret) {
  std::vector<uint64_t> s2 = {1, 2, 3, 4};
  {
    IStorage::tx_guard g(*storage_);
    storage_->setSystemSideSecret(s2);
  }
  restartStorage();
  ASSERT_EQ(storage_->getSystemSideSecret(), s2);
}

TEST_F(test_utt_storage, test_setClientSideSecret) {
  std::vector<uint64_t> s1 = {1, 2, 3, 4};
  {
    IStorage::tx_guard g(*storage_);
    storage_->setClientSideSecret(s1);
  }
  restartStorage();
  ASSERT_EQ(storage_->getClientSideSecret(), s1);
}

TEST_F(test_utt_storage, test_setRcmSignature) {
  std::vector<uint8_t> sig = {0x0, 0x1, 0x2, 0x3};
  {
    IStorage::tx_guard g(*storage_);
    storage_->setRcmSignature(sig);
  }
  restartStorage();
  ASSERT_EQ(storage_->getRcmSignature(), sig);
}

TEST_F(test_utt_storage, test_setCoin) {
  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    std::vector<uint16_t> shares_signers;
    std::string simulatonOfUniqueTxHash = std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
    auto mint = Mint(simulatonOfUniqueTxHash, 100, c.getPid());
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(mint).front());
      shares_signers.push_back(banks[i]->getId());
    }
    for (auto& b : banks) {
      for (size_t i = 0; i < rsigs.size(); i++) {
        auto& sig = rsigs[i];
        auto& signer = shares_signers[i];
        ASSERT_TRUE(b->validatePartialSignature<Mint>(signer, sig, 0, mint));
      }
    }
    auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(mint, d, {blinded_sig}).front();
    {
      IStorage::tx_guard g(*storage_);
      storage_->setCoin(coin);
    }
  }
  restartStorage();
  ASSERT_EQ(storage_->getCoins().size(), clients.size());
}

TEST_F(test_utt_storage, test_removeCoin) {
  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    std::vector<uint16_t> shares_signers;
    std::string simulatonOfUniqueTxHash = std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
    auto mint = Mint(simulatonOfUniqueTxHash, 100, c.getPid());
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(mint).front());
      shares_signers.push_back(banks[i]->getId());
    }
    for (auto& b : banks) {
      for (size_t i = 0; i < rsigs.size(); i++) {
        auto& sig = rsigs[i];
        auto& signer = shares_signers[i];
        ASSERT_TRUE(b->validatePartialSignature<Mint>(signer, sig, 0, mint));
      }
    }
    auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(mint, d, {blinded_sig}).front();
    {
      IStorage::tx_guard g(*storage_);
      storage_->setCoin(coin);
      storage_->removeCoin(coin);
    }
  }
  restartStorage();
  ASSERT_TRUE(storage_->getCoins().empty());
}

TEST_F(test_utt_storage, test_getCoins) {
  std::vector<std::string> coins_nulls;
  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    std::vector<uint16_t> shares_signers;
    std::string simulatonOfUniqueTxHash = std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
    auto mint = Mint(simulatonOfUniqueTxHash, 100, c.getPid());
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(mint).front());
      shares_signers.push_back(banks[i]->getId());
    }
    for (auto& b : banks) {
      for (size_t i = 0; i < rsigs.size(); i++) {
        auto& sig = rsigs[i];
        auto& signer = shares_signers[i];
        ASSERT_TRUE(b->validatePartialSignature<Mint>(signer, sig, 0, mint));
      }
    }
    auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(mint, d, {blinded_sig}).front();
    coins_nulls.push_back(coin.getNullifier());
    {
      IStorage::tx_guard g(*storage_);
      storage_->setCoin(coin);
    }
  }
  restartStorage();
  for (const auto& coin : storage_->getCoins()) {
    ASSERT_TRUE(std::find(coins_nulls.begin(), coins_nulls.end(), coin.getNullifier()) != coins_nulls.end());
  }
}

}  // namespace
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}