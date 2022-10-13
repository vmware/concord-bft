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

Wallet::Wallet(std::string userId, utt::client::TestUserPKInfrastructure& pki) : userId_{std::move(userId)}, pki_{pki} {
  connect();
}

void Wallet::connect() {
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

  grpc_ = WalletService::NewStub(chan);
}

void Wallet::showInfo() const {
  std::cout << "User Id: " << userId_ << '\n';

  if (!checkOperational()) return;
  std::cout << "Private balance: " << user_->getBalance() << '\n';
  std::cout << "Privacy budget: " << user_->getPrivacyBudget() << '\n';
  std::cout << "Last executed transaction number: " << user_->getLastExecutedTxNum() << '\n';
}

void Wallet::deployApp() {
  if (isOperational_) {
    std::cout << "The wallet has already deployed an app and is operational.\n";
    return;
  }

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
  grpc_->deployPrivacyApp(&ctx, req, &resp);

  // Note that keeping the config around in memory is just a temp solution and should not happen in real system
  if (resp.has_err()) {
    std::cout << "Failed to deploy privacy app: " << resp.err() << '\n';
  } else if (resp.app_id().empty()) {
    std::cout << "Failed to deploy privacy app: empty app id!\n";
  } else {
    // We need the public config part which can typically be obtained from the service, but we derive it for simplicity
    auto publicConfig = utt::client::getPublicConfig(config);
    std::cout << "Successfully deployed privacy app with id: " << resp.app_id() << '\n';

    user_ = utt::client::createUser(userId_, publicConfig, pki_, storage_);
    if (!user_) throw std::runtime_error("Failed to create user!");

    isOperational_ = true;
  }
}

void Wallet::registerUser() {
  if (!checkOperational()) return;

  auto userRegInput = user_->getRegistrationInput();
  if (userRegInput.empty()) throw std::runtime_error("Failed to create user registration input!");

  grpc::ClientContext ctx;

  RegisterUserRequest req;
  req.set_user_id(userId_);
  req.set_input_rcm(userRegInput.data(), userRegInput.size());
  req.set_user_pk(user_->getPK());

  RegisterUserResponse resp;
  grpc_->registerUser(&ctx, req, &resp);

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

void Wallet::createPrivacyBudget() {
  if (!checkOperational()) return;

  grpc::ClientContext ctx;

  CreatePrivacyBudgetRequest req;
  req.set_user_id(userId_);

  CreatePrivacyBudgetResponse resp;
  grpc_->createPrivacyBudget(&ctx, req, &resp);

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

void Wallet::mint(uint64_t amount) {
  if (!checkOperational()) return;

  grpc::ClientContext ctx;

  MintRequest req;
  req.set_user_id(userId_);
  req.set_value(amount);

  MintResponse resp;
  grpc_->mint(&ctx, req, &resp);

  if (resp.has_err()) {
    std::cout << "Failed to mint:" << resp.err() << '\n';
  } else {
    std::cout << "Successfully sent mint tx with number:" << resp.tx_number() << '\n';

    syncState(resp.tx_number());
  }
}

void Wallet::transfer(uint64_t amount, const std::string recipient) {
  if (!checkOperational()) return;

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

  std::cout << "Started processing " << amount << "anonymous transfer to " << recipient << "...\n";

  // Process the transfer until we get the final transaction
  // On each iteration we also sync up to the tx number of our request
  while (true) {
    auto result = user_->transfer(recipient, pki_.getPublicKey(recipient), amount);

    grpc::ClientContext ctx;

    TransferRequest req;
    req.set_tx_data(result.requiredTx_.data_.data(), result.requiredTx_.data_.size());

    TransferResponse resp;
    grpc_->transfer(&ctx, req, &resp);

    if (resp.has_err()) {
      std::cout << "Failed to transfer:" << resp.err() << '\n';
    } else {
      std::cout << "Successfully sent transfer tx with number:" << resp.tx_number() << '\n';

      syncState(resp.tx_number());
    }

    if (result.isFinal_) break;  // Done
  }
}

void Wallet::burn(uint64_t amount) {
  if (!checkOperational()) return;

  if (user_->getBalance() < amount) {
    std::cout << "Insufficient private balance!\n";
    return;
  }

  if (user_->getPrivacyBudget() < amount) {
    std::cout << "Insufficient privacy budget!\n";
    return;
  }

  std::cout << "Started processing " << amount << " burn...\n";

  // Process the transfer until we get the final transaction
  // On each iteration we also sync up to the tx number of our request
  while (true) {
    auto result = user_->burn(amount);

    grpc::ClientContext ctx;

    BurnRequest req;
    req.set_tx_data(result.requiredTx_.data_.data(), result.requiredTx_.data_.size());

    BurnResponse resp;
    grpc_->burn(&ctx, req, &resp);

    if (resp.has_err()) {
      std::cout << "Failed to burn:" << resp.err() << '\n';
    } else {
      std::cout << "Successfully sent burn tx with number:" << resp.tx_number() << '\n';

      syncState(resp.tx_number());
    }

    if (result.isFinal_) break;  // Done
  }
}

bool Wallet::checkOperational() const {
  if (!isOperational_) {
    std::cout << "You must first deploy a privacy app. Use command 'deploy app'\n";
    return false;
  }
  return true;
}

void Wallet::syncState(uint64_t lastKnownTxNum) {
  if (!checkOperational()) return;

  std::cout << "Synchronizing state...\n";

  // Sync to latest state
  if (lastKnownTxNum == 0) {
    std::cout << "Last known tx number is zero (or not provided) - fetching last signed tx number...\n";

    grpc::ClientContext ctx;
    GetLastSignedTxNumberRequest req;
    GetLastSignedTxNumberResponse resp;
    grpc_->getLastSignedTxNumber(&ctx, req, &resp);

    if (resp.has_err()) {
      std::cout << "Failed to get last signed tx number:" << resp.err() << '\n';
    } else {
      std::cout << "Got last signed tx number:" << resp.tx_number() << '\n';
      lastKnownTxNum = resp.tx_number();
    }
  }

  for (uint64_t txNum = user_->getLastExecutedTxNum() + 1; txNum <= lastKnownTxNum; ++txNum) {
    grpc::ClientContext ctx;

    GetSignedTransactionRequest req;
    req.set_tx_number(txNum);

    GetSignedTransactionResponse resp;
    grpc_->getSignedTransaction(&ctx, req, &resp);

    if (resp.has_err()) {
      std::cout << "Failed to get signed tx with number " << txNum << ':' << resp.err() << '\n';
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
        tx.type_ = utt::Transaction::Type::Transfer;
        if (!sigs.empty()) throw std::runtime_error("Expected no signatures for burn tx!");
        user_->updateBurnTx(resp.tx_number(), tx);
      } break;
      default:
        throw std::runtime_error("Unexpected tx type!");
    }
  }
}