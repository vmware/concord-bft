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

namespace {
void printStatus(const grpc::Status& status, const char* rpcName) {
  if (status.ok())
    std::cout << rpcName << " grpc status ok\n";
  else
    std::cout << rpcName << " grpc status error " << status.error_code() << ": " << status.error_message() << '\n';
}
}  // namespace

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
  std::cout << "\n--------- " << userId_ << " ---------\n";
  std::cout << "Public balance: " << publicBalance_ << '\n';
  std::cout << "Private balance: " << user_->getBalance() << '\n';
  std::cout << "Privacy budget: " << user_->getPrivacyBudget() << '\n';
  std::cout << "Last executed tx number: " << user_->getLastExecutedTxNum() << '\n';
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
  auto status = conn->deployPrivacyApp(&ctx, req, &resp);
  printStatus(status, "deployPrivacyApp");

  // Note that keeping the config around in memory is just a temp solution and should not happen in real system
  if (resp.has_err()) throw std::runtime_error("Failed to deploy privacy app: " + resp.err());

  std::cout << "\nDeployed privacy application\n";
  std::cout << "-----------------------------------\n";
  std::cout << "Privacy contract: " << resp.privacy_contract_addr() << '\n';
  std::cout << "Token contract: " << resp.token_contract_addr() << '\n';

  return std::pair<utt::Configuration, utt::PublicConfig>{config, utt::client::getPublicConfig(config)};
}

void Wallet::createPrivacyBudgetLocal(const utt::Configuration& config, uint64_t amount) {
  user_->createPrivacyBudgetLocal(config, amount);
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
  auto status = conn->registerUser(&ctx, req, &resp);
  printStatus(status, "registerUser");

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

      break;  // Done
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
        std::cout << "Successfully sent self-transfer tx as part of burn. Last added tx number:"
                  << resp.last_added_tx_number() << '\n';

        syncState(conn, resp.last_added_tx_number());
      }
      // Continue with the next transaction in the burn process
    }
  }
}

void Wallet::syncState(Connection& conn, uint64_t lastKnownTxNum) {
  std::cout << "Synchronizing state...\n";

  // Update public balance
  while (true) {
    grpc::ClientContext ctx;
    GetPublicBalanceRequest req;
    req.set_user_id(userId_);
    GetPublicBalanceResponse resp;
    auto status = conn->getPublicBalance(&ctx, req, &resp);
    printStatus(status, "getPublicBalance");

    if (resp.has_err()) {
      std::cout << "Failed to get public balance:" << resp.err() << '\n';
      break;
    } else if (!resp.has_public_balance()) {
      std::cout << "retry getPublicBalance\n";
    } else {
      publicBalance_ = resp.public_balance();
      break;  // Done
    }
  }

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
    auto status = conn->getSignedTransaction(&ctx, req, &resp);
    printStatus(status, "getSignedTransaction");

    if (resp.has_err()) {
      std::cout << "Failed to get signed tx with number " << txNum << ':' << resp.err() << '\n';
      return;
    }

    if (!resp.has_tx_number()) {
      std::cout << "Missing tx number in GetSignedTransactionResponse!\n";
      return;
    }

    std::cout << "Got " << TxType_Name(resp.tx_type()) << " transaction.\n";
    std::cout << "Tx num: " << resp.tx_number() << '\n';
    std::cout << "Tx data size: " << resp.tx_data().size() << " bytes\n";
    std::cout << "Tx data actual size: " << resp.tx_data_size() << " bytes\n";
    std::cout << "Num Sigs: " << resp.sigs_size() << '\n';

    utt::Transaction tx;
    std::copy(resp.tx_data().begin(), resp.tx_data().end(), std::back_inserter(tx.data_));

    auto getTxData = [](Connection& conn, uint64_t txNumber, std::vector<uint8_t>& buff) {
      grpc::ClientContext ctx;

      GetTxDataRequest req;
      req.set_tx_number(txNumber);
      req.set_byte_offset((uint32_t)buff.size());

      GetTxDataResponse resp;
      conn->getTxData(&ctx, req, &resp);

      if (resp.has_err()) {
        throw std::runtime_error("getTxData: " + resp.err());
      }
      std::cout << "got extra data:" << resp.tx_data().size() << '\n';

      std::copy(resp.tx_data().begin(), resp.tx_data().end(), std::back_inserter(buff));
    };

    // [TODO-UTT] As it seems the wallet grpc server written in node.js using grpc-js occasionally
    // sends empty message (with missing fields) with no error either on the client or server.
    // I have observed this mostly when fetching transactions which are a bit larger than other messages
    // but not much - 20-50k. My initial suspicion was that the size of the proto message was at fault
    // so I added a way to fetch the data in chunks. Each tx is cached in the wallet service and the client
    // will fetch it sequentially in the next loop with 'getTxData'. This behavior could be a bug on part
    // of the proto serialization in node.js or some misuse of gRPC in this client or the server. In any
    // occasion this issue will plague the demo if not fixed properly, because syncing will fail and it
    // needs to be retried.
    while (true) {
      if (tx.data_.size() < resp.tx_data_size()) {
        std::cout << "Tx data was incomplete - fetch additional tx data...\n";
        getTxData(conn, resp.tx_number(), tx.data_);
      } else if (tx.data_.size() > resp.tx_data_size()) {
        throw std::runtime_error("Got more bytes than the actual tx size!");
      } else {  // tx size == actual size
        break;  // Done
      }
    }

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

void Wallet::debugOutput() const { user_->debugOutput(); }