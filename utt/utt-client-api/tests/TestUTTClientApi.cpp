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

#include <utt-client-api/ClientApi.hpp>

#include <xassert/XAssert.h>

#include <memory>

// libutt
#include <utt/RegAuth.h>
#include <utt/RandSig.h>
#include <utt/Utils.h>

// libutt new interface
#include <registrator.hpp>
#include <coinsSigner.hpp>
#include <common.hpp>
#include <config.hpp>
#include <budget.hpp>
#include <mint.hpp>
#include <burn.hpp>
#include <transaction.hpp>
#include <serialization.hpp>
#include "testUtils/testKeys.hpp"
using namespace libutt;
using namespace libutt::api::testing;
struct RegisterUserResponse {
  utt::RegistrationSig sig;
  utt::S2 s2;
};

struct PrivacyBudgetResponse {
  utt::PrivacyBudget budget;
  utt::PrivacyBudgetSig sig;
};

struct ExecutedTx {
  utt::Transaction tx_;
  utt::TxOutputSigs sigs_;
};

struct ServerMock {
  // UTT Configuration
  std::unique_ptr<libutt::api::Configuration> config_;
  std::vector<libutt::api::Registrator> registrars_;
  std::vector<libutt::api::CoinsSigner> coinsSigners_;

  // UTT State
  uint32_t lastTokenId_ = 0;
  std::set<std::string> nullifiers_;
  std::vector<ExecutedTx> ledger_;

  static ServerMock createFromConfig(const utt::Configuration& config) {
    ServerMock mock;
    mock.config_ =
        std::make_unique<libutt::api::Configuration>(libutt::api::deserialize<libutt::api::Configuration>(config));
    assertTrue(mock.config_->isValid());

    auto registrationVerificationKey = mock.config_->getPublicConfig().getRegistrationVerificationKey();
    auto commitVerificationKey = mock.config_->getPublicConfig().getCommitVerificationKey();

    std::map<uint16_t, std::string> commitVerificationKeyShares;
    std::map<uint16_t, std::string> registrationVerificationKeyShares;
    for (uint16_t i = 0; i < mock.config_->getNumValidators(); ++i) {
      commitVerificationKeyShares.emplace(i, mock.config_->getCommitVerificationKeyShare(i));
      registrationVerificationKeyShares.emplace(i, mock.config_->getRegistrationVerificationKeyShare(i));
    }

    // Create registrars and coins signers
    for (uint16_t i = 0; i < mock.config_->getNumValidators(); ++i) {
      mock.registrars_.emplace_back(
          i, mock.config_->getRegistrationSecret(i), registrationVerificationKeyShares, registrationVerificationKey);
      mock.coinsSigners_.emplace_back(i,
                                      mock.config_->getCommitSecret(i),
                                      commitVerificationKey,
                                      commitVerificationKeyShares,
                                      registrationVerificationKey);
    }

    return mock;
  }

  uint64_t getLastExecutedTxNum() const { return ledger_.size(); }
  const ExecutedTx& getExecutedTx(uint64_t txNum) const {
    if (txNum == 0) throw std::runtime_error("Zero is not a valid transaction number!");
    return ledger_.at(txNum - 1);
  }

  RegisterUserResponse registerUser(const std::string& userId, const utt::UserRegistrationInput& userRegInput) {
    assertFalse(userId.empty());
    assertFalse(userRegInput.empty());

    auto pidHash = libutt::api::Utils::curvePointFromHash(userId);
    assertFalse(pidHash.empty());

    auto rcm1 = libutt::api::deserialize<libutt::api::Commitment>(userRegInput);

    // [TODO-UTT] Each validator in computing the registration signature
    // must pick the same s2 based on some "seed". Here we simply generate
    // a single s2 which is a big simplification.
    libutt::api::types::CurvePoint s2 = libutt::Fr::random_element().to_words();

    std::vector<std::vector<uint8_t>> shares;
    for (const auto& registrar : registrars_) {
      // We ignore s2 from the result because it's simply returned back to us from signRCM
      // without anything useful happening to it.
      auto [_, share] = registrar.signRCM(pidHash, s2, rcm1);
      shares.emplace_back(std::move(share));
    }

    const uint16_t n = config_->getNumValidators();
    const uint16_t t = config_->getThreshold();

    std::map<uint32_t, std::vector<uint8_t>> shareSubset;
    auto idxSubset = libutt::random_subset(t, n);
    for (size_t idx : idxSubset) {
      shareSubset[(uint32_t)idx] = shares[(uint32_t)idx];
    }

    RegisterUserResponse resp;
    resp.s2 = s2;
    resp.sig = libutt::api::Utils::aggregateSigShares(n, shareSubset);
    return resp;
  }

  PrivacyBudgetResponse createPrivacyBudget(const std::string& userId, uint64_t amount, uint64_t expireTime) {
    assertFalse(userId.empty());
    assertTrue(amount > 0);

    auto pidHash = libutt::api::Utils::curvePointFromHash(userId);
    auto snHash = libutt::api::Utils::curvePointFromHash("budget|" + std::to_string(++lastTokenId_));

    auto budget =
        libutt::api::operations::Budget(config_->getPublicConfig().getParams(), snHash, pidHash, amount, expireTime);

    std::vector<std::vector<uint8_t>> shares;
    for (const auto& signer : coinsSigners_) {
      shares.emplace_back(signer.sign(budget).front());
    }

    const uint16_t n = config_->getNumValidators();
    const uint16_t t = config_->getThreshold();

    std::map<uint32_t, std::vector<uint8_t>> shareSubset;
    auto idxSubset = libutt::random_subset(t, n);
    for (size_t idx : idxSubset) {
      shareSubset[(uint32_t)idx] = shares[(uint32_t)idx];
    }

    PrivacyBudgetResponse resp;
    resp.budget = libutt::api::serialize<libutt::api::operations::Budget>(budget);
    resp.sig = libutt::api::Utils::aggregateSigShares(n, shareSubset);
    return resp;
  }

  uint64_t mint(const std::string& userId, uint64_t amount, const utt::Transaction& tx) {
    assertFalse(userId.empty());
    assertTrue(amount > 0);
    assertTrue(tx.type_ == utt::Transaction::Type::Mint);
    assertTrue(!tx.data_.empty());

    auto mintTx = libutt::api::deserialize<libutt::api::operations::Mint>(tx.data_);
    assertTrue(mintTx.getRecipentID() == userId);
    assertTrue(mintTx.getVal() == amount);

    std::vector<std::vector<uint8_t>> shares;
    for (const auto& signer : coinsSigners_) {
      shares.emplace_back(signer.sign(mintTx).front());
    }

    const uint16_t n = config_->getNumValidators();
    const uint16_t t = config_->getThreshold();

    std::map<uint32_t, std::vector<uint8_t>> shareSubset;
    auto idxSubset = libutt::random_subset(t, n);
    for (size_t idx : idxSubset) {
      shareSubset[(uint32_t)idx] = shares[(uint32_t)idx];
    }

    ExecutedTx executedTx;
    executedTx.tx_ = tx;
    executedTx.sigs_.emplace_back(libutt::api::Utils::aggregateSigShares(n, shareSubset));
    ledger_.emplace_back(std::move(executedTx));

    return ledger_.size();
  }

  uint64_t burn(const utt::Transaction& tx) {
    assertTrue(tx.type_ == utt::Transaction::Type::Burn);

    auto burn = libutt::api::deserialize<libutt::api::operations::Burn>(tx.data_);

    for (const auto& signer : coinsSigners_) {
      assertTrue(signer.validate(config_->getPublicConfig().getParams(), burn));
    }

    auto null = burn.getNullifier();
    assertTrue(nullifiers_.count(null) == 0);
    nullifiers_.emplace(std::move(null));

    ExecutedTx executedTx;
    executedTx.tx_.type_ = utt::Transaction::Type::Burn;
    executedTx.tx_.data_ = libutt::api::serialize<libutt::api::operations::Burn>(burn);
    ledger_.emplace_back(std::move(executedTx));

    return ledger_.size();
  }

  uint64_t transfer(const utt::Transaction& tx) {
    assertTrue(tx.type_ == utt::Transaction::Type::Transfer);

    auto uttTx = libutt::api::deserialize<libutt::api::operations::Transaction>(tx.data_);

    for (const auto& signer : coinsSigners_) {
      assertTrue(signer.validate(config_->getPublicConfig().getParams(), uttTx));
    }

    for (auto&& null : uttTx.getNullifiers()) {
      assertTrue(nullifiers_.count(null) == 0);
      nullifiers_.emplace(std::move(null));
    }

    std::vector<std::vector<std::vector<uint8_t>>> shares;
    for (const auto& signer : coinsSigners_) {
      shares.emplace_back(signer.sign(uttTx));
    }

    const uint16_t n = config_->getNumValidators();
    const uint16_t t = config_->getThreshold();

    size_t numOutCoins = shares[0].size();
    assertTrue(numOutCoins > 0);

    ExecutedTx executedTx;
    executedTx.tx_.type_ = utt::Transaction::Type::Transfer;
    executedTx.tx_.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);

    // Aggregate a signature for each output coin
    for (size_t i = 0; i < numOutCoins; ++i) {
      std::map<uint32_t, std::vector<uint8_t>> shareSubset;
      auto idxSubset = libutt::random_subset(t, n);
      for (size_t idx : idxSubset) {
        shareSubset[(uint32_t)idx] = shares[(uint32_t)idx][i];
      }
      executedTx.sigs_.emplace_back(libutt::api::Utils::aggregateSigShares(n, shareSubset));
    }

    ledger_.emplace_back(std::move(executedTx));

    return ledger_.size();
  }
};
class InMemoryUserStorage : public utt::client::IStorage {
 public:
  bool isNewStorage() override { return true; };
  void setKeyPair(const std::pair<std::string, std::string>& keyPair) override { keyPair_ = keyPair; }
  void setClientSideSecret(const libutt::api::types::CurvePoint& s1) override { s1_ = s1; }
  void setSystemSideSecret(const libutt::api::types::CurvePoint& s2) override { s2_ = s2; };
  void setRcmSignature(const libutt::api::types::Signature& rcm_sig) override { rcm_sig_ = rcm_sig; }
  void setCoin(const libutt::api::Coin& coin) override {
    if (coins_.find(coin.getNullifier()) != coins_.end())
      throw std::runtime_error("trying to add an already exist coin to storage");
    coins_[coin.getNullifier()] = coin;
  }
  void removeCoin(const libutt::api::Coin& coin) override {
    if (coins_.find(coin.getNullifier()) == coins_.end())
      throw std::runtime_error("trying to remove an non existed coin to storage");
    coins_.erase(coin.getNullifier());
  };

  libutt::api::types::CurvePoint getClientSideSecret() override { return s1_; }
  libutt::api::types::CurvePoint getSystemSideSecret() override { return s2_; }
  libutt::api::types::Signature getRcmSignature() override { return rcm_sig_; }
  std::vector<libutt::api::Coin> getCoins() override {
    std::vector<libutt::api::Coin> coins;
    for (const auto& [_, c] : coins_) coins.push_back(c);
    return coins;
  }
  std::pair<std::string, std::string> getKeyPair() override { return keyPair_; }
  void startTransaction() override {}
  void commit() override {}

 private:
  libutt::api::types::CurvePoint s1_;
  libutt::api::types::CurvePoint s2_;
  libutt::api::types::Signature rcm_sig_;
  std::unordered_map<std::string, libutt::api::Coin> coins_;
  std::pair<std::string, std::string> keyPair_;
};

const std::vector<std::string> user_ids({"user-1", "user-2", "user-3"});
std::vector<uint64_t> users_last_sn{0, 0, 0};
int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  // Note that this test assumes the client and server-side parts of the code work under the same initialization of
  // libutt.
  utt::client::Initialize();

  utt::client::ConfigInputParams cfgInputParams;

  // Create a UTT system tolerating F faulty validators
  const uint16_t F = 1;
  cfgInputParams.validatorPublicKeys = std::vector<std::string>(3 * F + 1, "placeholderForPublicKey");
  cfgInputParams.threshold = F + 1;

  // Create a new UTT instance config
  auto config = utt::client::generateConfig(cfgInputParams);

  // Create a valid server-side mock based on the config
  auto serverMock = ServerMock::createFromConfig(config);

  const size_t C = user_ids.size();
  loginfo << "Test users: " << C << '\n';
  assertTrue(C >= 3);  // At least 3 test users expected

  std::vector<std::unique_ptr<utt::client::User>> users;

  auto syncUsersWithServer = [&]() {
    loginfo << "Synchronizing users with server" << endl;
    for (size_t i = 0; i < C; ++i) {
      for (uint64_t txNum = users_last_sn[i] + 1; txNum <= serverMock.getLastExecutedTxNum(); ++txNum) {
        const auto& executedTx = serverMock.getExecutedTx(txNum);
        switch (executedTx.tx_.type_) {
          case utt::Transaction::Type::Mint: {
            assertTrue(executedTx.sigs_.size() == 1);
            users[i]->updateMintTx(executedTx.tx_, executedTx.sigs_.front());
          } break;
          case utt::Transaction::Type::Burn: {
            assertTrue(executedTx.sigs_.empty());
            users[i]->updateBurnTx(executedTx.tx_);
          } break;
          case utt::Transaction::Type::Transfer: {
            assertFalse(executedTx.sigs_.empty());
            users[i]->updateTransferTx(executedTx.tx_, executedTx.sigs_);
          } break;
          default:
            assertFail("Unknown tx type!");
        }
        users_last_sn[i] = txNum;
      }
    }
  };

  // Create new users by using the public config
  auto publicConfig = libutt::api::serialize<libutt::api::PublicConfig>(serverMock.config_->getPublicConfig());
  std::vector<uint64_t> initialBalance;
  std::vector<uint64_t> initialBudget;

  for (size_t i = 0; i < C; ++i) {
    std::unique_ptr<utt::client::IStorage> storage = std::make_unique<InMemoryUserStorage>();
    const auto& private_key = k_TestKeys.at(user_ids[i]).first;
    const auto& public_key = k_TestKeys.at(user_ids[i]).second;
    users.emplace_back(utt::client::createUser(user_ids[i], publicConfig, private_key, public_key, std::move(storage)));
    initialBalance.emplace_back((i + 1) * 100);
    initialBudget.emplace_back((i + 1) * 100);
  }

  // Register users
  loginfo << "Registering users" << endl;
  for (size_t i = 0; i < C; ++i) {
    auto resp = serverMock.registerUser(users[i]->getUserId(), users[i]->getRegistrationInput());
    assertFalse(resp.s2.empty());
    assertFalse(resp.sig.empty());

    // Note: the user's pk is usually recorded by the system and returned as part of the registration
    users[i]->updateRegistration(users[i]->getPK(), resp.sig, resp.s2);
  }

  // Create budgets
  loginfo << "Creating user budgets" << endl;
  for (size_t i = 0; i < C; ++i) {
    auto resp = serverMock.createPrivacyBudget(users[i]->getUserId(), initialBudget[i], 0);
    assertFalse(resp.budget.empty());
    assertFalse(resp.sig.empty());

    users[i]->updatePrivacyBudget(resp.budget, resp.sig);
    assertTrue(users[i]->getPrivacyBudget() == initialBudget[i]);
  }

  // Mint test
  {
    loginfo << "Minting tokens" << endl;
    for (size_t i = 0; i < C; ++i) {
      auto tx = users[i]->mint(initialBalance[i]);
      serverMock.mint(users[i]->getUserId(), initialBalance[i], tx);
    }

    syncUsersWithServer();

    for (size_t i = 0; i < C; ++i) {
      assertTrue(users[i]->getBalance() == initialBalance[i]);
      assertTrue(users[i]->getPrivacyBudget() == initialBudget[i]);  // Unchanged
    }
  }

  // Each user sends to the next one (wrapping around to the first) some amount
  {
    const uint64_t amount = 50;
    for (size_t i = 0; i < C; ++i) {
      size_t nextUserIdx = (i + 1) % C;
      std::string nextUserId = "user-" + std::to_string(nextUserIdx + 1);
      loginfo << "Sending " << amount << " from " << users[i]->getUserId() << " to " << nextUserId << endl;
      assertTrue(amount <= users[i]->getBalance());
      auto result = users[i]->transfer(nextUserId, k_TestKeys.at(nextUserId).second, amount);
      assertTrue(result.isFinal_);
      serverMock.transfer(result.requiredTx_);
    }
    syncUsersWithServer();

    for (size_t i = 0; i < C; ++i) {
      assertTrue(users[i]->getBalance() ==
                 initialBalance[i]);  // Unchanged - each user sent X and received X from another user
      assertTrue(users[i]->getPrivacyBudget() ==
                 initialBudget[i] - amount);  // Each transfer costs the same amount of privacy budget
    }
  }

  // All users burn their private funds
  loginfo << "Burning user's tokens" << endl;
  for (size_t i = 0; i < C; ++i) {
    const uint64_t balance = users[i]->getBalance();
    assertTrue(balance > 0);

    while (true) {
      auto result = users[i]->burn(balance);
      if (result.requiredTx_.type_ == utt::Transaction::Type::Burn) {
        assertTrue(result.isFinal_);
        serverMock.burn(result.requiredTx_);
        syncUsersWithServer();
        break;  // We can stop processing after burning the coin
      } else if (result.requiredTx_.type_ == utt::Transaction::Type::Transfer) {
        assertFalse(result.isFinal_);
        // We need to process a self transaction (split/merge)
        serverMock.transfer(result.requiredTx_);
        syncUsersWithServer();
      }
    }
  }

  for (size_t i = 0; i < C; ++i) {
    assertTrue(users[i]->getBalance() == 0);
  }

  return 0;
}