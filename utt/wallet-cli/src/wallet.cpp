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

#include "wallet.hpp"

#include <iostream>

using namespace vmware::concord::utt::wallet::api::v1;

Wallet::Wallet(std::string userId, utt::client::TestUserPKInfrastructure& pki, const utt::PublicConfig& config)
    : userId_{std::move(userId)}, pki_{pki} {
  user_ = utt::client::createUser(userId_, config, pki_, storage_);
  if (!user_) throw std::runtime_error("Failed to create user!");
}

Wallet::Connection Wallet::newConnection() {
  std::string grpcServerAddr = "127.0.0.1:49000";

  std::cout << "Connecting to gRPC server at " << grpcServerAddr << " ...\n";

  auto chan = grpc::CreateChannel(grpcServerAddr, grpc::InsecureChannelCredentials());

  if (!chan) {
    throw std::runtime_error("Failed to create gRPC channel.");
  }
  auto timeoutSec = std::chrono::seconds(5);
  if (chan->WaitForConnected(std::chrono::system_clock::now() + timeoutSec)) {
    std::cout << "Connected.\n";
  } else {
    throw std::runtime_error("Failed to connect to gRPC server after " + std::to_string(timeoutSec.count()) +
                             " seconds.");
  }

  return WalletService::NewStub(chan);
}

void Wallet::showInfo(Connection& conn) {
  syncState(conn);

  std::cout << "User Id: " << userId_ << '\n';
  std::cout << "Private balance: " << user_->getBalance() << '\n';
  std::cout << "Privacy budget: " << user_->getPrivacyBudget() << '\n';
  std::cout << "Last executed transaction number: " << user_->getLastExecutedTxNum() << '\n';
}

std::pair<utt::Configuration, utt::PublicConfig> Wallet::deployApp(Connection& conn) {
  // Generate a privacy config for a N=4 replica system tolerating F=1 failures
  utt::client::ConfigInputParams params;
  params.validatorPublicKeys = std::vector<std::string>{4, "placeholderPublicKey"};  // N = 3 * F + 1
  params.threshold = 2;                                                              // F + 1
  auto config = utt::client::generateConfig(params);
  if (config.empty()) throw std::runtime_error("Failed to generate a privacy app configuration!");

  grpc::ClientContext ctx;

  DeployPrivacyAppRequest req;
  req.set_config(config.data(), config.size());

  DeployPrivacyAppResponse resp;
  conn->deployPrivacyApp(&ctx, req, &resp);

  // Note that keeping the config around in memory is just a temp solution and should not happen in real system
  if (resp.has_err()) throw std::runtime_error("Failed to deploy privacy app: " + resp.err());
  if (resp.app_id().empty()) throw std::runtime_error("Failed to deploy privacy app: empty app id!");

  // We need the public config part which can typically be obtained from the service, but we derive it for simplicity
  std::cout << "Successfully deployed privacy app with id: " << resp.app_id() << '\n';

  return std::pair<utt::Configuration, utt::PublicConfig>{config, utt::client::getPublicConfig(config)};
}

void Wallet::preCreatePrivacyBudget(const utt::Configuration& config, uint64_t amount) {
  user_->preCreatePrivacyBudget(config, amount);
}

void Wallet::registerUser(Connection& conn) {
  auto userRegInput = user_->getRegistrationInput();
  if (userRegInput.empty()) throw std::runtime_error("Failed to create user registration input!");

  grpc::ClientContext ctx;

  RegisterUserRequest req;
  req.set_user_id(userId_);
  req.set_input_rcm(userRegInput.data(), userRegInput.size());
  req.set_user_pk(user_->getPK());

  RegisterUserResponse resp;
  conn->registerUser(&ctx, req, &resp);

  if (resp.has_err()) {
    std::cout << "Failed to register user: " << resp.err() << '\n';
  } else {
    utt::RegistrationSig sig = std::vector<uint8_t>(resp.signature().begin(), resp.signature().end());
    std::cout << "Got sig for registration with size: " << sig.size() << '\n';

    utt::S2 s2 = std::vector<uint64_t>(resp.s2().begin(), resp.s2().end());
    std::cout << "Got S2 for registration: [";
    for (const auto& val : s2) std::cout << val << ' ';
    std::cout << "]\n";

    user_->updateRegistration(user_->getPK(), sig, s2);
  }
}

void Wallet::createPrivacyBudget(Connection& conn) {
  grpc::ClientContext ctx;

  CreatePrivacyBudgetRequest req;
  req.set_user_id(userId_);

  CreatePrivacyBudgetResponse resp;
  conn->createPrivacyBudget(&ctx, req, &resp);

  if (resp.has_err()) {
    std::cout << "Failed to create privacy budget:" << resp.err() << '\n';
  } else {
    utt::PrivacyBudget budget = std::vector<uint8_t>(resp.budget().begin(), resp.budget().end());
    utt::RegistrationSig sig = std::vector<uint8_t>(resp.signature().begin(), resp.signature().end());

    std::cout << "Got budget " << budget.size() << " bytes.\n";
    std::cout << "Got budget sig " << sig.size() << " bytes.\n";

    user_->updatePrivacyBudget(budget, sig);
  }
}

void Wallet::mint(Connection& conn, uint64_t amount) {
  auto mintTx = user_->mint(amount);

  grpc::ClientContext ctx;

  MintRequest req;
  req.set_user_id(userId_);
  req.set_value(amount);
  req.set_tx_data(mintTx.data_.data(), mintTx.data_.size());

  MintResponse resp;
  conn->mint(&ctx, req, &resp);

  if (resp.has_err()) {
    std::cout << "Failed to mint:" << resp.err() << '\n';
  } else {
    std::cout << "Successfully sent mint tx. Last added tx number:" << resp.last_added_tx_number() << '\n';

    syncState(conn, resp.last_added_tx_number());
  }
}

void Wallet::transfer(Connection& conn, uint64_t amount, const std::string& recipient) {
  if (userId_ == recipient) {
    std::cout << "Cannot transfer to self directly!\n";
    return;
  }

  if (user_->getBalance() < amount) {
    std::cout << "Insufficient private balance!\n";
    return;
  }

  if (user_->getPrivacyBudget() < amount) {
    std::cout << "Insufficient privacy budget!\n";
    return;
  }

  std::cout << "Started processing " << amount << " anonymous transfer to " << recipient << "...\n";

  // Process the transfer until we get the final transaction
  // On each iteration we also sync up to the tx number of our request
  while (true) {
    auto result = user_->transfer(recipient, pki_.getPublicKey(recipient), amount);

    grpc::ClientContext ctx;

    TransferRequest req;
    req.set_tx_data(result.requiredTx_.data_.data(), result.requiredTx_.data_.size());
    req.set_num_outputs(result.requiredTx_.numOutputs_);

    TransferResponse resp;
    conn->transfer(&ctx, req, &resp);

    if (resp.has_err()) {
      std::cout << "Failed to transfer:" << resp.err() << '\n';
    } else {
      std::cout << "Successfully sent transfer tx. Last added tx number:" << resp.last_added_tx_number() << '\n';

      syncState(conn, resp.last_added_tx_number());
    }

    if (result.isFinal_) break;  // Done
  }
}

void Wallet::burn(Connection& conn, uint64_t amount) {
  if (user_->getBalance() < amount) {
    std::cout << "Insufficient private balance!\n";
    return;
  }

  std::cout << "Started processing " << amount << " burn...\n";

  // Process the transfer until we get the final transaction
  // On each iteration we also sync up to the tx number of our request
  while (true) {
    auto result = user_->burn(amount);

    grpc::ClientContext ctx;

    if (result.isFinal_) {
      BurnRequest req;
      req.set_user_id(user_->getUserId());
      req.set_value(amount);
      req.set_tx_data(result.requiredTx_.data_.data(), result.requiredTx_.data_.size());

      BurnResponse resp;
      conn->burn(&ctx, req, &resp);

      if (resp.has_err()) {
        std::cout << "Failed to do burn:" << resp.err() << '\n';
      } else {
        std::cout << "Successfully sent burn tx. Last added tx number:" << resp.last_added_tx_number() << '\n';

        syncState(conn, resp.last_added_tx_number());
      }

      break; // Done
    } else {
      TransferRequest req;
      req.set_tx_data(result.requiredTx_.data_.data(), result.requiredTx_.data_.size());
      req.set_num_outputs(result.requiredTx_.numOutputs_);
      TransferResponse resp;
      conn->transfer(&ctx, req, &resp);

      if (resp.has_err()) {
        std::cout << "Failed to do self-transfer as part of burn:" << resp.err() << '\n';
        return;
      } else {
        std::cout << "Successfully sent self-transfer tx as part of burn. Last added tx number:" << resp.last_added_tx_number() << '\n';

        syncState(conn, resp.last_added_tx_number());
      }
      // Continue with the next transaction in the burn process
    }
  }
}

void Wallet::syncState(Connection& conn, uint64_t lastKnownTxNum) {
  std::cout << "Synchronizing state...\n";

  // Sync to latest state
  if (lastKnownTxNum == 0) {
    std::cout << "Last known tx number is zero (or not provided) - fetching last added tx number...\n";

    grpc::ClientContext ctx;
    GetLastAddedTxNumberRequest req;
    GetLastAddedTxNumberResponse resp;
    conn->getLastAddedTxNumber(&ctx, req, &resp);

    if (resp.has_err()) {
      std::cout << "Failed to get last added tx number:" << resp.err() << '\n';
    } else {
      std::cout << "Got last added tx number:" << resp.tx_number() << '\n';
      lastKnownTxNum = resp.tx_number();
    }
  }

  for (uint64_t txNum = user_->getLastExecutedTxNum() + 1; txNum <= lastKnownTxNum; ++txNum) {
    grpc::ClientContext ctx;

    GetSignedTransactionRequest req;
    req.set_tx_number(txNum);

    GetSignedTransactionResponse resp;
    conn->getSignedTransaction(&ctx, req, &resp);

    if (resp.has_err()) {
      std::cout << "Failed to get signed tx with number " << txNum << ':' << resp.err() << '\n';
      return;
    }

    if (!resp.has_tx_number()) {
      std::cout << "Incomplete response from wallet service!\n";
      return;
    }

    std::cout << "Got signed " << TxType_Name(resp.tx_type()) << " transaction.\n";
    std::cout << "Tx data: " << resp.tx_data().size() << " bytes\n";
    std::cout << "Num Sigs: " << resp.sigs_size() << '\n';

    utt::Transaction tx;
    tx.data_ = std::vector<uint8_t>(resp.tx_data().begin(), resp.tx_data().end());

    utt::TxOutputSigs sigs;
    sigs.reserve((size_t)resp.sigs_size());
    for (const auto& sig : resp.sigs()) {
      sigs.emplace_back(std::vector<uint8_t>(sig.begin(), sig.end()));
    }

    // Apply transaction
    switch (resp.tx_type()) {
      case TxType::MINT: {
        tx.type_ = utt::Transaction::Type::Mint;
        if (sigs.size() != 1) throw std::runtime_error("Expected single signature in mint tx!");
        user_->updateMintTx(resp.tx_number(), tx, sigs[0]);
      } break;
      case TxType::TRANSFER: {
        tx.type_ = utt::Transaction::Type::Transfer;
        user_->updateTransferTx(resp.tx_number(), tx, sigs);
      } break;
      case TxType::BURN: {
        tx.type_ = utt::Transaction::Type::Burn;
        if (!sigs.empty()) throw std::runtime_error("Expected no signatures for burn tx!");
        user_->updateBurnTx(resp.tx_number(), tx);
      } break;
      default:
        throw std::runtime_error("Unexpected tx type!");
    }
  }
}