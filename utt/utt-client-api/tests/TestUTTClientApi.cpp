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
#include <utt/Coin.h>
#include <utt/MintOp.h>
#include <utt/BurnOp.h>

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

std::map<std::string, std::pair<std::string, std::string>> k_TestKeys{
    {"user-1",
     {"-----BEGIN RSA PRIVATE KEY-----\n"
      "MIICXQIBAAKBgQDAyUwjK+m4EXyFKEha2NfQUY8eIqurRujNMqI1z7B3bF/8Bdg0\n"
      "wffIYJUjiXo13hB6yOq+gD62BGAPRjmgReBniT3d2rLU0Z72zQ64Gof66jCGQt0W\n"
      "0sfwDUv0XsfXXW9p1EGBYwIkgW6UGiABnkZcIUW4dzP2URoRCN/VIypkRwIDAQAB\n"
      "AoGAa3VIvSoTAoisscQ8YHcSBIoRjiihK71AsnAQvpHfuRFthxry4qVjqgs71i0h\n"
      "M7lt0iL/xePSEL7rlFf+cvnAFL4/j1R04ImBjRzWGnaNE8I7nNGGzJo9rL5I1oi3\n"
      "zN2yUucTSGm7qR0MCNVy26zNmCuS/FdBPsfdZ017OTsHtPECQQDlWXAJG6nHyw2o\n"
      "2cLYHzlyrrYgnWJkgFSKzr7VFNlHxfQSWXJ4zuDwhqkm3d176bVm4eHhDDv6f413\n"
      "iQGraKvTAkEA1zAzpxfI7LAqd3sObWYstQb03IXE7yddMgbhoMDCT3gXhNaHKfjT\n"
      "Z/GIk49jh8kyitN2FeYXXi9TiwrXStfhPQJBAMNea6ymjvstwoYKcgsOli5WG7ku\n"
      "uEkqdFoGAdObvfeA7gfPgE7e1AiwfVkpd+l9TVTFqFe/xzv8+fJQmEZ+lJcCQQDN\n"
      "5I7nh7h1zzEy1Qk+345TP262OT/u26kuHqtv1j+VLgDC10jIfg443D+jgITo/Tdg\n"
      "4WeRGHCva3TyCtNoBxq5AkA9KZpKof4ripad4oIuCJpR/ZhQATgQwR9f+FlAxgP0\n"
      "ABmBPCoxy4uGMtSBMqiiGpImbDuivYkhlBl7D8u8vn26\n"
      "-----END RSA PRIVATE KEY-----\n",
      "-----BEGIN PUBLIC KEY-----\n"
      "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDAyUwjK+m4EXyFKEha2NfQUY8e\n"
      "IqurRujNMqI1z7B3bF/8Bdg0wffIYJUjiXo13hB6yOq+gD62BGAPRjmgReBniT3d\n"
      "2rLU0Z72zQ64Gof66jCGQt0W0sfwDUv0XsfXXW9p1EGBYwIkgW6UGiABnkZcIUW4\n"
      "dzP2URoRCN/VIypkRwIDAQAB\n"
      "-----END PUBLIC KEY-----\n"}},
    {"user-2",
     {"-----BEGIN RSA PRIVATE KEY-----\n"
      "MIICXAIBAAKBgQDMw+qeJXQ/ZxrqSqjRXoN2wFYNwhljh7RTBJuIzaqK2pDL+zaK\n"
      "aREzgufG/7r2uk8njWW7EJbriLcmj1oJA913LPl4YWUEORKl7Cb6wLoPO/E5gAt1\n"
      "JJ0dhDeCRU+E7vtNQ4pLyy2dYS2GHO1Tm88yuFegS3gkINPYgzNggGJ2cQIDAQAB\n"
      "AoGAScyCjqTpFMDQTojB91OdBfukCClghSKvtwv+EnwtbwX/EcVkjtX3QR1485vP\n"
      "goT7akHn3FfKTPFlMRyRUpZ2Bov1whQ1ztuboIonFQ7ohbDTLE3QzUv4L3e8cEbY\n"
      "51MSe8tEUVRxKu53nD3asWxAi/CEyqWvRCzf4s3Q6Xw3h5ECQQD8mBT6ervLr1Qk\n"
      "oKmaAuPTDyZDaSjipD0/d1p1vG8Wb8go14tq89Ts+UIWzH5aGlidxTK9j/luQqlR\n"
      "YVVGNkC3AkEAz4a8jtg2++fvWT0PDz6OBfw/iHJQzSmynlzKQzpRac02UBCPo4an\n"
      "o7wl9uEnucXuVpCSo0JdSf+x4r9dwmCKFwJBAPWlGNG2xicBbPzp2cZTBSheVUG9\n"
      "ZOtz+bRc5/YTuJzDPI6rf4QVeH60sNbnLAGIGaHlAsFi4Jmf7nWcCIftfuUCQEbx\n"
      "hJxAhetvyn7zRKatd9fL99wpWD4Ktyk0B2EcGqDUqnCMeM4qRjzPIRtYtT/oziWB\n"
      "nt943HNjmeguC1tbrVkCQBMd+kbpcoFHKKrC577FM24maWRTfXJeu2/o6pxUFIUY\n"
      "kzkDZ2k+FfvXWaY+N5q5bJCayor8W1QeruHzewrQmgY=\n"
      "-----END RSA PRIVATE KEY-----\n",
      "-----BEGIN PUBLIC KEY-----\n"
      "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDMw+qeJXQ/ZxrqSqjRXoN2wFYN\n"
      "whljh7RTBJuIzaqK2pDL+zaKaREzgufG/7r2uk8njWW7EJbriLcmj1oJA913LPl4\n"
      "YWUEORKl7Cb6wLoPO/E5gAt1JJ0dhDeCRU+E7vtNQ4pLyy2dYS2GHO1Tm88yuFeg\n"
      "S3gkINPYgzNggGJ2cQIDAQAB\n"
      "-----END PUBLIC KEY-----\n"}},
    {"user-3",
     {"-----BEGIN RSA PRIVATE KEY-----\n"
      "MIICXAIBAAKBgQCnmdqW7f/rg8CiUzLugxc0jPMqzphhtl40IqINAk3pasCQsA3T\n"
      "eHSa1fcKocFMY9bfQRvqiKpnK74d0V0fujFUaPIMlKMLWXEVefwe1fOYrDXOoz7N\n"
      "3Go7x9u/LXwCA6HehXtIavtTPQs1yHldb/bDocEhjfGvU3TLXkAuGWsnmQIDAQAB\n"
      "AoGBAJ6fRHyYICB8f7Kh35BRTYMU64fWI+5GtX3OUWTSi36g5EOL/GnqlSF94+OS\n"
      "F+n+i/ycGJmuYuhmQ/bgkaxXghsDYb7fsdMJ8DEqUJAKbxeOosn8fxwmJkNAJ07J\n"
      "+oAg/xkJ+ukyYnPf0P3UTuTZl0EFEpwu/vnX09QJGtuXgmQhAkEA0c0Co9MdP62r\n"
      "/ybXXeqgaol2YVGzFr/bMz3hhlviV9IOGPRZmeQ8v+f1/lSsqZ8wSP5P8dkBo4UB\n"
      "NSLaHAUL/QJBAMyB72EyHZUEFy3o241myqamfVtN+Dzo6qdPn/PfF/BLjwsEApCO\n"
      "oUObmDDo/yiSSb00XSnn23bGYH1VJJDNJs0CQE1aG+YQ+VC4FJkfVfpvfjOpePcK\n"
      "q0/w7r2mzBbAm+QrMz1qIfsGVoue12itCXgElEXlVc5iZyNF75sKvYXlKnUCQHHC\n"
      "tc5zelEyfVJkff0ieQhLBOCNdtErH50Chg+6wi5BWcje6i5PqRVasEZE1etTtQEy\n"
      "58Av4b0ojPQrMLP76uECQEz0c1RPDwMvznwT3BJxl8t4tixPML0nyBMD8ttnZswG\n"
      "K/1CYV1uMNbchmuVQb2Kd2JyE1gQF8s3ShsbteMc5og=\n"
      "-----END RSA PRIVATE KEY-----\n",
      "-----BEGIN PUBLIC KEY-----\n"
      "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCnmdqW7f/rg8CiUzLugxc0jPMq\n"
      "zphhtl40IqINAk3pasCQsA3TeHSa1fcKocFMY9bfQRvqiKpnK74d0V0fujFUaPIM\n"
      "lKMLWXEVefwe1fOYrDXOoz7N3Go7x9u/LXwCA6HehXtIavtTPQs1yHldb/bDocEh\n"
      "jfGvU3TLXkAuGWsnmQIDAQAB\n"
      "-----END PUBLIC KEY-----\n"}},
};

using namespace libutt;

struct RegisterUserResponse {
  utt::RegistrationSig sig;
  utt::S2 s2;
};

struct PrivacyBudgetResponse {
  utt::PrivacyBudget budget;
  utt::PrivacyBudgetSig sig;
};

struct MintResponse {
  uint64_t txNum = 0;
  utt::Transaction tx;
  utt::TxOutputSig sig;
};

struct TransferResponse {
  uint64_t txNum = 0;
  utt::Transaction tx;
  utt::TxOutputSigs sigs;
};

struct BurnResponse {
  uint64_t txNum = 0;
  utt::Transaction tx;
};

struct ExecutedTx {
  utt::Transaction tx_;
  utt::TxOutputSigs sigs_;
  std::string publicUserId_;  // Burns and mints are public transactions that expose the user id, we save it here for
                              // convenience
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
    for (uint16_t i = 0; i < mock.config_->getNumParticipants(); ++i) {
      commitVerificationKeyShares.emplace(i, mock.config_->getCommitVerificationKeyShare(i));
      registrationVerificationKeyShares.emplace(i, mock.config_->getRegistrationVerificationKeyShare(i));
    }

    // Create registrars and coins signers
    for (uint16_t i = 0; i < mock.config_->getNumParticipants(); ++i) {
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

    // [TODO-UTT] Each participant in computing the registration signature
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

    const uint16_t n = config_->getNumParticipants();
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

  PrivacyBudgetResponse createBudget(const std::string& userId, uint64_t amount, uint64_t expireTime) {
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

    const uint16_t n = config_->getNumParticipants();
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

  uint64_t mint(const std::string& userId, uint64_t amount) {
    assertFalse(userId.empty());
    assertTrue(amount > 0);

    auto pidHash = libutt::api::Utils::curvePointFromHash(userId);
    auto uniqueHash = "mint|" + std::to_string(++lastTokenId_);

    auto mintTx = libutt::api::operations::Mint(uniqueHash, amount, userId);

    std::vector<std::vector<uint8_t>> shares;
    for (const auto& signer : coinsSigners_) {
      shares.emplace_back(signer.sign(mintTx).front());
    }

    const uint16_t n = config_->getNumParticipants();
    const uint16_t t = config_->getThreshold();

    std::map<uint32_t, std::vector<uint8_t>> shareSubset;
    auto idxSubset = libutt::random_subset(t, n);
    for (size_t idx : idxSubset) {
      shareSubset[(uint32_t)idx] = shares[(uint32_t)idx];
    }

    ExecutedTx executedTx;
    executedTx.tx_.type_ = utt::Transaction::Type::Mint;
    executedTx.tx_.data_ = libutt::api::serialize<libutt::api::operations::Mint>(mintTx);
    executedTx.sigs_.emplace_back(libutt::api::Utils::aggregateSigShares(n, shareSubset));
    executedTx.publicUserId_ = userId;
    ledger_.emplace_back(std::move(executedTx));

    return ledger_.size();
  }

  uint64_t burn(const std::string& userId, const utt::Transaction& tx) {
    assertTrue(tx.type_ == utt::Transaction::Type::Burn);

    auto burn = libutt::api::deserialize<libutt::api::operations::Burn>(tx.data_);
    auto null = burn.getNullifier();
    assertTrue(nullifiers_.count(null) == 0);
    nullifiers_.emplace(std::move(null));

    ExecutedTx executedTx;
    executedTx.tx_.type_ = utt::Transaction::Type::Burn;
    executedTx.tx_.data_ = libutt::api::serialize<libutt::api::operations::Burn>(burn);
    executedTx.publicUserId_ = userId;
    ledger_.emplace_back(std::move(executedTx));

    return ledger_.size();
  }

  uint64_t transfer(const utt::Transaction& tx) {
    assertTrue(tx.type_ == utt::Transaction::Type::Transfer);

    auto uttTx = libutt::api::deserialize<libutt::api::operations::Transaction>(tx.data_);
    for (auto&& null : uttTx.getNullifiers()) {
      assertTrue(nullifiers_.count(null) == 0);
      nullifiers_.emplace(std::move(null));
    }

    std::vector<std::vector<std::vector<uint8_t>>> shares;
    for (const auto& signer : coinsSigners_) {
      shares.emplace_back(signer.sign(uttTx));
    }

    const uint16_t n = config_->getNumParticipants();
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

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  // Note that this test assumes the client and server-side parts of the code work under the same initialization of
  // libutt.
  utt::client::Initialize();

  utt::client::ConfigInputParams cfgInputParams;

  // Create a UTT system tolerating F faulty participants
  const uint16_t F = 1;
  cfgInputParams.participantsPublicKeys = std::vector<std::string>(3 * F + 1, "placeholderForPublicKey");
  cfgInputParams.threshold = F + 1;

  // Create a new UTT instance config
  auto config = utt::client::generateConfig(cfgInputParams);

  // Create a valid server-side mock based on the config
  auto serverMock = ServerMock::createFromConfig(config);
  (void)serverMock;

  // Create new users by using the public config
  const int C = 3;
  std::vector<std::unique_ptr<utt::client::User>> users;
  users.reserve(C);

  auto publicConfig = libutt::api::serialize<libutt::api::PublicConfig>(serverMock.config_->getPublicConfig());

  struct DummyUserPKInfrastructure : public utt::client::IUserPKInfrastructure {
    utt::client::IUserPKInfrastructure::KeyPair generateKeys(const std::string& userId) override {
      auto it = k_TestKeys.find(userId);
      if (it == k_TestKeys.end()) throw std::runtime_error("No test keys for " + userId);
      return utt::client::IUserPKInfrastructure::KeyPair{it->second.first, it->second.second};
    }
  };

  auto syncUsersWithServer = [&]() {
    loginfo << "Synchronizing users with server" << endl;
    for (size_t i = 0; i < C; ++i) {
      for (uint64_t txNum = users[i]->getLastExecutedTxNum() + 1; txNum <= serverMock.getLastExecutedTxNum(); ++txNum) {
        const auto& executedTx = serverMock.getExecutedTx(txNum);
        switch (executedTx.tx_.type_) {
          case utt::Transaction::Type::Mint: {
            if (executedTx.publicUserId_ == users[i]->getUserId()) {
              assertTrue(executedTx.sigs_.size() == 1);
              auto result = users[i]->updateMintTx(txNum, executedTx.tx_, executedTx.sigs_.front());
              assertTrue(result);
            } else {
              auto result = users[i]->updateNoOp(txNum);
              assertTrue(result);
            }
          } break;
          case utt::Transaction::Type::Burn: {
            if (executedTx.publicUserId_ == users[i]->getUserId()) {
              assertTrue(executedTx.sigs_.empty());
              auto result = users[i]->updateBurnTx(txNum, executedTx.tx_);
              assertTrue(result);
            } else {
              auto result = users[i]->updateNoOp(txNum);
              assertTrue(result);
            }
          } break;
          case utt::Transaction::Type::Transfer: {
            assertFalse(executedTx.sigs_.empty());
            auto result = users[i]->updateTransferTx(txNum, executedTx.tx_, executedTx.sigs_);
            assertTrue(result);
          } break;
          default:
            assertFail("Unknown tx type!");
        }
      }
    }
  };

  // Create users
  utt::client::IUserStorage storage;
  DummyUserPKInfrastructure pki;
  std::vector<uint64_t> initialBalance;
  std::vector<uint64_t> initialBudget;
  for (int i = 0; i < C; ++i) {
    users.emplace_back(utt::client::createUser("user-" + std::to_string(i + 1), publicConfig, pki, storage));
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
    auto result = users[i]->updateRegistration(users[i]->getPK(), resp.sig, resp.s2);
    assertTrue(result);
  }

  // Create budgets
  loginfo << "Creating user budgets" << endl;
  for (size_t i = 0; i < C; ++i) {
    auto resp = serverMock.createBudget(users[i]->getUserId(), initialBudget[i], 0);
    assertFalse(resp.budget.empty());
    assertFalse(resp.sig.empty());

    auto result = users[i]->updatePrivacyBudget(resp.budget, resp.sig);
    assertTrue(result);
    assertTrue(users[i]->getPrivacyBudget() == initialBudget[i]);
  }

  // Mint test
  {
    loginfo << "Minting tokens" << endl;
    for (size_t i = 0; i < C; ++i) {
      auto txNum = serverMock.mint(users[i]->getUserId(), initialBalance[i]);
      assertTrue(txNum == serverMock.getLastExecutedTxNum());
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
      auto result = users[i]->transfer(nextUserId, k_TestKeys[nextUserId].second, amount);
      assertTrue(result.isFinal_);
      auto txNum = serverMock.transfer(result.requiredTx_);
      assertTrue(txNum == serverMock.getLastExecutedTxNum());
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
        auto txNum = serverMock.burn(users[i]->getUserId(), result.requiredTx_);
        assertTrue(txNum == serverMock.getLastExecutedTxNum());
        syncUsersWithServer();
        break;  // We can stop processing after burning the coin
      } else if (result.requiredTx_.type_ == utt::Transaction::Type::Transfer) {
        assertFalse(result.isFinal_);
        // We need to process a self transaction (split/merge)
        auto txNum = serverMock.transfer(result.requiredTx_);
        assertTrue(txNum == serverMock.getLastExecutedTxNum());
        syncUsersWithServer();
      }
    }
  }

  for (size_t i = 0; i < C; ++i) {
    assertTrue(users[i]->getBalance() == 0);
  }

  return 0;
}