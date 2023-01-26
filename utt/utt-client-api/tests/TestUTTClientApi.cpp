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

using namespace libutt;

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
  void setLastExecutedSn(uint64_t sn) override {
    if (sn < lastExecutedSn_) throw std::runtime_error("trying to write an incorrect sequence number");
    lastExecutedSn_ = sn;
  }
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

  uint64_t getLastExecutedSn() override { return lastExecutedSn_; }
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
  uint64_t lastExecutedSn_;
  libutt::api::types::CurvePoint s1_;
  libutt::api::types::CurvePoint s2_;
  libutt::api::types::Signature rcm_sig_;
  std::unordered_map<std::string, libutt::api::Coin> coins_;
  std::pair<std::string, std::string> keyPair_;
};

const std::vector<std::string> user_ids({"user-1", "user-2", "user-3"});
const std::map<std::string, std::pair<std::string, std::string>> k_TestKeys{
    {"user-1",
     {"-----BEGIN PRIVATE KEY-----\n"
      "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDRMymAGtHdMtc2\n"
      "J6pDnk0fSt9ZUDT62yXkNF+jBY4zxLuBMq/qhp+nXaa2rB2haqtJxMpAQH/xy5D8\n"
      "l7smiMjgxsLre9oeV5dH9hjVu+HzqulEbFNGtWOBqQg8mFO6orwxLoZXsyUeumF8\n"
      "wFFfY0QDZ9IZDZPP7gdXy8iKFe2z4A3KKiG1Xgp+DQWU2qM1AppekluNWYXRVyZg\n"
      "buHNmTMdbZTfat5rzPfNOPSTRPJTSsbKSufidxe1iS+QS1OW4UzKfDx8r2Z0AgDJ\n"
      "0CWvOmArMM5YZjkr+4C4iurhHd20pnAb29QuYWqOcUIIfR31QM3zSwi3qmCs2ECw\n"
      "d28tN4zfAgMBAAECggEAf3xQNBMkBUqwP/5YEjDsCr/T8FeikaTvKGyKQ4xlJkMj\n"
      "iQ5cie1Uaef2aqfkvrOEgsX8Ar/LuIw3ZNcKY+dDk0dNDbhCKe9y75WXeNiwT4+9\n"
      "68af0R1E8IUT0el6TOhTCx2xHMy9OEaYli+U5y5VCkZAkKfnhfUmYGh9YzkNzGkJ\n"
      "dhGepKcmSfV68qzwptT8KqFfyBVRDNsVPT2FQ228DaJGYiMSz1H72KTOvq8oNtLa\n"
      "5itjY9By3PUrM3Zl7I/0uSmbYkq1izKlN9aoVVA8D5otSnoArsbjRjNzT1NsMEDj\n"
      "SO41ADjmrPQFoYfK6neKbWJ637XsWJNbF11SOJ74oQKBgQDw2RGHtOpPzAHR7kcN\n"
      "6I1ycDrxliwHoiGkt/qiRbvbvZtvg3nM1QDruyrT4MV8IaEGth6qxw/LDZl/PKi/\n"
      "dUV9S+r0gV7Z2Eja1j8oETZsEGgfsqpgtxgqjee0c4n4ijks5i5ByIam6ezPk0CX\n"
      "drPHmKTa3wv25yvsqUDqA4yneQKBgQDeXGQ4rzN4VS2XdxeoZtfgGo9lSi3ooG7p\n"
      "yrDr3/6tIWYZln5WbeI+Ym58XNLFs/UYLuaAXX2xAD1GC8GyRwfV2IxNtnvyELqe\n"
      "r5VZLUyr/nAKNy4DCgmDirsMQi2BiCIW+3dbaSMWk0hyRL4XywgDT1LIv1iBjME9\n"
      "VRdOcqtJFwKBgQCOnJnqt09/DJePTP452BfZSWc9oeRUaMZvGJmJ+KyyAuXE9B7t\n"
      "ELtI2j0T1KZbSDZnGPOzv8c0PstDMhHhvHj3JjrrqKcXayBIpFAlU3vcJSLikhKc\n"
      "zg27NOecTEXIK2CLm4iMX0aMEzur9c9rYFg3ucTz7NrSyjOSIO5VIuDoUQKBgGq/\n"
      "ZAWqZkUwbxC1xY+8v4oAdjKkJ+Hzkwt9mO3DvNmUnRVPoBsR6XkVfAEL9suelt0j\n"
      "NtkNCNg+SywjXLufSe2pZcGxB1OwIEcp98K18obnQRZGYzpmSSbzJNS/uGTk26i4\n"
      "1BX4JTYjQrZIthFqENC19gIVigG3dtVg0i2A8yXFAoGBALJAAyZe0CviVpR7uBeS\n"
      "fhvN1+0T4mVo1p00Cn5I9eUPMaHkuFoXSgy96PVvfbI+s5vZOEqqrkr3ERfl9Lo7\n"
      "FXEtnQuUZ+zrURHHYxwzYj10MFYaUV408ywQ0KpKqpA1KJpnxLZEidFbZSKUliOa\n"
      "1BNh6wL6XelbX7hpe53RpXSU\n"
      "-----END PRIVATE KEY-----",
      "-----BEGIN PUBLIC KEY-----\n"
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0TMpgBrR3TLXNieqQ55N\n"
      "H0rfWVA0+tsl5DRfowWOM8S7gTKv6oafp12mtqwdoWqrScTKQEB/8cuQ/Je7JojI\n"
      "4MbC63vaHleXR/YY1bvh86rpRGxTRrVjgakIPJhTuqK8MS6GV7MlHrphfMBRX2NE\n"
      "A2fSGQ2Tz+4HV8vIihXts+ANyiohtV4Kfg0FlNqjNQKaXpJbjVmF0VcmYG7hzZkz\n"
      "HW2U32rea8z3zTj0k0TyU0rGykrn4ncXtYkvkEtTluFMynw8fK9mdAIAydAlrzpg\n"
      "KzDOWGY5K/uAuIrq4R3dtKZwG9vULmFqjnFCCH0d9UDN80sIt6pgrNhAsHdvLTeM\n"
      "3wIDAQAB\n"
      "-----END PUBLIC KEY-----"}},
    {"user-2",
     {"-----BEGIN PRIVATE KEY-----\n"
      "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDmtzssQWb6iagv\n"
      "gmn8WGqeyvl1jVPEjtuD+OC/i29NByi/SzuEBHmnEMbThXyy2hE9wbxL9fcwv9P7\n"
      "DGhOiHPfWryUksoiJtkAxhJeAofcPH8h40cn35CZC/z52MBf6jxhlsr8YzaPXALO\n"
      "KWuVxHXWm4XRQuYasHc17FlC+QOB/piwT1pJzJQL82XMVPQ3BiiPVBYmBIIGZ4UV\n"
      "Elbv0G8oiuX8sspJxK5u0QlKbXe4WaCpwn5+9eW32xMd5IwMGKsz2GMU+TWovJk6\n"
      "V4QairTtv7KYNo8noGVEPP8WiieHc6K/esqaqOMEOjSjbHIkz8x4x2a3EcQ8ZqqE\n"
      "5ECJCHyNAgMBAAECggEAAj4TsCluk87UuKl+2DoPxz1X0WGvR8DPt2eq35G+YjRI\n"
      "nBtiF7VnbU1HcXThsmdWNZB1gROB9JJYwB7twJ2o6qnaHWOT3WMdRtmmAg3qPiTE\n"
      "Y5Lu9R9CK4qnSdJmhEglkBlEHdHlDN2rFU/nwMvpDU3zrgYFcIr4jaifcXw0IYZQ\n"
      "QcohiItC72Nz9/xYVOYkzzj4CbltAvH7QEniVPEkiHuKcMFocb0Qr1xUdw7cYCpH\n"
      "wgLoUk+7mv7QNo1gDVtIfFJcig1w/YMe6wLZV0cs2jCzQqJL/H24cDnSobqFld+F\n"
      "W0HFazNX8dw50213oSlINHH7Ys9abPV7UXeolHWTbQKBgQD5MJnGt6HZPTPu6aWc\n"
      "HN9EVR0iYU5J4lxYFdvFnTC1uWUxwQGa9h3ohvt7J7RgOe1zfoDHQBzjTmTz2emt\n"
      "Z3K4xVVxwatyZO460AAWvJDJRpHx6QMBpVS1rek0LifaR8sac4NIX3Km7UcmWywR\n"
      "Byt5T5gHL8otwvmVn4BmWIn+IwKBgQDtBWFz8+g58Y5KgH/LWEC6r6df/5oxj8BY\n"
      "sZhgw8Oz9VjGMuleSc0vByzVg9kRdIOEQrNQrVgJwU5AeZiRL/eXw8W2FQUBLHPF\n"
      "pYMXUBrSluGdlg+JLGkNP2pa9mNhlgVGeiHWPINWiDZL6OMejWawge0hUe8UI3XP\n"
      "yuAD8CFNjwKBgF+NHUovm+YbK9DO0uwbvhkLshI+0bBPFi3Io+8QqV7lakI7ygvL\n"
      "mAhTyhadUPMdA0ooFeVRVkJrCxbeVlZhtoHXWT43jzBcN2Vh4MbXI4Wqg8gJG1gE\n"
      "N4k62JNjp3Bx2xWXeZ3Ey8fqcD/q8ejNoQPxW1BWKOaHPwD6mlekV7WVAoGAJ8+/\n"
      "jrppR1JLlDgBPyaTuMfIVVeZjrmWlU8/SQGY3aFYR/JFQJEk5cFOxo5e54+qh4ys\n"
      "keCL6RePDUVfWwOzkspPa0YckRXmXExwuHm8B6NQQifydBgBjTgZpS29g6avPCdH\n"
      "h7SiWuaGODnl7DvUA9HPAsnAXqGWKFDrT74F2gECgYB8s752mNPfKerMuTH7cRmZ\n"
      "cX1GdwshZVnA745mlEgwNgbeF3ir9ZGPsK5j0GuYgy1+pwjp8eT4OT8gWcJJjU7t\n"
      "9i2pQzrqCArD4WMQ+XhADM26fNL3utI+kEhK205KHy42yE0lCCqoTtPxdwZAru3Q\n"
      "5apYHTHKXTFGxzEZBGn3lg==\n"
      "-----END PRIVATE KEY-----",
      "-----BEGIN PUBLIC KEY-----\n"
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5rc7LEFm+omoL4Jp/Fhq\n"
      "nsr5dY1TxI7bg/jgv4tvTQcov0s7hAR5pxDG04V8stoRPcG8S/X3ML/T+wxoTohz\n"
      "31q8lJLKIibZAMYSXgKH3Dx/IeNHJ9+QmQv8+djAX+o8YZbK/GM2j1wCzilrlcR1\n"
      "1puF0ULmGrB3NexZQvkDgf6YsE9aScyUC/NlzFT0NwYoj1QWJgSCBmeFFRJW79Bv\n"
      "KIrl/LLKScSubtEJSm13uFmgqcJ+fvXlt9sTHeSMDBirM9hjFPk1qLyZOleEGoq0\n"
      "7b+ymDaPJ6BlRDz/Foonh3Oiv3rKmqjjBDo0o2xyJM/MeMdmtxHEPGaqhORAiQh8\n"
      "jQIDAQAB\n"
      "-----END PUBLIC KEY-----"}},
    {"user-3",
     {"-----BEGIN PRIVATE KEY-----\n"
      "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDKI22rxWJQWA0M\n"
      "zt0QA91dv6u9ZRXN97ojkEU3byqf6VPhpzoFrEUo0pUj/8kApCoBphJnlqUc2qJT\n"
      "t8rtonD8SscVAboTHLHHwC4RWR4hVMml+GWPdrucpIdA8vGvnAoSLtcZltyrdyGO\n"
      "OM4G7TOI+At6wWRZSJq9+3F748It5g5tWeDPZQ/HsS8h5rYHgItvQOM/QV43XJRA\n"
      "X0b9S2S8zAfwcg8Veb8xxGr9+OCSGYmUMnztT+6cTHcWrmf04iR4Xe6sdvp8/3JZ\n"
      "hG0aDHXnAhv6hrqDbR1/qe+wA8wWklT/5i2YKhQy/zAsZP6+SR6uXcb4RmFezFCn\n"
      "1QPHahdHAgMBAAECggEBAJ10n3dQF9cR34h8kwJooEToivTLCwMX1yzgsqoNtBxA\n"
      "epCp3K6SlITKFFaoZoBTYE1MecWrOQ6S2CNyZWZaRLyh2xXn9HPyxCEe+EOXMbsw\n"
      "+qqIJURtbpMELj9PfygV2lpzliZet6Hw+Hh6kzIeDyjAXuDOSEasIa2fcbTZo2Zw\n"
      "DUQNimJ8zm2vqzTXaDQxUk8vWTvYjaTU7HpjKK0Y6VL/MSb36a8db3iCdtWMhfQr\n"
      "/sEDIc9PXwE2dXEs7quz79PjrX9WyAW+lI0B4jw6s4O+3jgSwZ5/n1pE3Q1QDLic\n"
      "DspyOkWI9bAeQkANQNXZsJtBpBWAwfVEcD6K+mNHGWECgYEA7iS9fxiUSjWJeKDS\n"
      "vHPvqIUB72KinnDt5WnpN+ac/uvg8tUUal9Ka2VpfOvt7DUnfu/8fxMOW5sEwWrD\n"
      "c34Nx7aweKDzNNBX13IV30rmrQWfXKIHF0zl2iCe1n4ZbRxtJ+XzS5eI56ofKb46\n"
      "bVy8mWIffF8VT8tX4Ps01mmUXQ8CgYEA2UuOMq0Z4aFzDybAFXrtGBF2ZydR7TYY\n"
      "vqCgERK4wYZ1Its7ikyUc178L4g+BtLcdraJAC1ai7sTvn/OgytJAaP2hzrxOdy4\n"
      "9ufXQ7YmUUmt6vgC0svPPBm6rRuBHMo4jlLcQ1GZxCIniGO32OzVl61L4nABviGj\n"
      "r+XG6oJnkkkCgYAom10wmdlXWg+p4QpuFfrMqnls/02pZKo8DzY0UP9+PGxsG8dS\n"
      "aBNBgtZc9cHpIBsu+u78IBy3pBRIgtL3E4x3/H/U4eT2oXwJAawXGieBY08MNZit\n"
      "8W/UJPHQs0nUoB9AyWYWAq3WityreoNt+H00TlX+GByDMbI3a75TvUcOIwKBgQCF\n"
      "LnHxTXdXe4Tx/GYuxDEdV1ai0s67/TQdYoW48SicNLUGsChGE7nJHKak23Ro6kSF\n"
      "3ksJ+MJOclMfp2YDrzwH7V7kc5P2SvpzGpYtwi6qE+as5WWVnaVeyMZJ41m3M5qG\n"
      "YPLvr8v3Epf4WGYOZtpjJKwmFVB8IFIbNVI92b9xQQKBgE77nz2wIPVM6Diwe1ro\n"
      "jgRURprOPDj5eT9JkMgyH8Qe9iKI5qCk8ps/s+P1hrZnjNkerdYilMuid20V9Lib\n"
      "/MKAhAV4UCIIVSMzP4bHpP+ZLhVHee4InLLA9EShYJGTdL1V5YYbeTTWiym4TetH\n"
      "oeRZA5cnrpr7PMvfRP0+sx7W\n"
      "-----END PRIVATE KEY-----",
      "-----BEGIN PUBLIC KEY-----\n"
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyiNtq8ViUFgNDM7dEAPd\n"
      "Xb+rvWUVzfe6I5BFN28qn+lT4ac6BaxFKNKVI//JAKQqAaYSZ5alHNqiU7fK7aJw\n"
      "/ErHFQG6Exyxx8AuEVkeIVTJpfhlj3a7nKSHQPLxr5wKEi7XGZbcq3chjjjOBu0z\n"
      "iPgLesFkWUiavftxe+PCLeYObVngz2UPx7EvIea2B4CLb0DjP0FeN1yUQF9G/Utk\n"
      "vMwH8HIPFXm/McRq/fjgkhmJlDJ87U/unEx3Fq5n9OIkeF3urHb6fP9yWYRtGgx1\n"
      "5wIb+oa6g20df6nvsAPMFpJU/+YtmCoUMv8wLGT+vkkerl3G+EZhXsxQp9UDx2oX\n"
      "RwIDAQAB\n"
      "-----END PUBLIC KEY-----"}}};

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
      for (uint64_t txNum = users[i]->getLastExecutedTxNum() + 1; txNum <= serverMock.getLastExecutedTxNum(); ++txNum) {
        const auto& executedTx = serverMock.getExecutedTx(txNum);
        switch (executedTx.tx_.type_) {
          case utt::Transaction::Type::Mint: {
            assertTrue(executedTx.sigs_.size() == 1);
            users[i]->updateMintTx(txNum, executedTx.tx_, executedTx.sigs_.front());
          } break;
          case utt::Transaction::Type::Burn: {
            assertTrue(executedTx.sigs_.empty());
            users[i]->updateBurnTx(txNum, executedTx.tx_);
          } break;
          case utt::Transaction::Type::Transfer: {
            assertFalse(executedTx.sigs_.empty());
            users[i]->updateTransferTx(txNum, executedTx.tx_, executedTx.sigs_);
          } break;
          default:
            assertFail("Unknown tx type!");
        }
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
      auto txNum = serverMock.mint(users[i]->getUserId(), initialBalance[i], tx);
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
      auto result = users[i]->transfer(nextUserId, k_TestKeys.at(nextUserId).second, amount);
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
        auto txNum = serverMock.burn(result.requiredTx_);
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