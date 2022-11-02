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
  std::string grpcServerAddr = "127.0.0.1:49001";

  std::cout << "Connecting to gRPC server at " << grpcServerAddr << "... ";

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

void Wallet::showInfo(Channel& chan) {
  std::cout << '\n';
  syncState(chan);
  std::cout << "--------- " << userId_ << " ---------\n";
  std::cout << "Public balance: " << publicBalance_ << '\n';
  std::cout << "Private balance: " << user_->getBalance() << '\n';
  std::cout << "Privacy budget: " << user_->getPrivacyBudget() << '\n';
  std::cout << "Last executed tx number: " << user_->getLastExecutedTxNum() << '\n';
}

std::pair<utt::Configuration, utt::PublicConfig> Wallet::getConfigs(Channel& chan) {
  WalletRequest req;
  req.mutable_configure();
  chan->Write(req);

  WalletResponse resp;
  chan->Read(&resp);
  if (!resp.has_configure()) throw std::runtime_error("Expected configure response from wallet service!");
  const auto& configureResp = resp.configure();

  // Note that keeping the config around in memory is just a temp solution and should not happen in real system
  if (configureResp.has_err()) throw std::runtime_error("Failed to configure: " + resp.err());

  std::cout << "\nSuccessfully configured privacy application\n";
  std::cout << "---------------------------------------------------\n";
  std::cout << "Privacy contract: " << configureResp.privacy_contract_addr() << '\n';
  std::cout << "Token contract: " << configureResp.token_contract_addr() << '\n';

  if (configureResp.config().empty()) throw std::runtime_error("The full config is empty!");
  if (configureResp.public_config().empty()) throw std::runtime_error("The public config is empty!");

  utt::Configuration config(configureResp.config().begin(), configureResp.config().end());
  utt::PublicConfig publicConfig(configureResp.public_config().begin(), configureResp.public_config().end());

  auto otherPublicConfig = utt::client::getPublicConfig(config);
  if (publicConfig != otherPublicConfig) {
    throw std::runtime_error("The public config doesn't correspond to the one in the full config!");
  }

  return std::pair<utt::Configuration, utt::PublicConfig>{std::move(config), std::move(publicConfig)};
}

void Wallet::createPrivacyBudgetLocal(const utt::Configuration& config, uint64_t amount) {
  user_->createPrivacyBudgetLocal(config, amount);
  std::cout << "Successfully created budget with value " << amount << '\n';
}

bool Wallet::isRegistered() const { return registered_; }

void Wallet::registerUser(Channel& chan) {
  if (registered_) throw std::runtime_error("Wallet is already registered!");

  auto userRegInput = user_->getRegistrationInput();
  if (userRegInput.empty()) throw std::runtime_error("Failed to create user registration input!");

  WalletRequest req;
  auto& registerReq = *req.mutable_register_user();
  registerReq.set_user_id(userId_);
  registerReq.set_input_rcm(userRegInput.data(), userRegInput.size());
  registerReq.set_user_pk(user_->getPK());
  chan->Write(req);

  WalletResponse resp;
  chan->Read(&resp);
  if (!resp.has_register_user()) throw std::runtime_error("Expected register response from wallet service!");
  const auto& regUserResp = resp.register_user();

  if (regUserResp.has_err()) {
    std::cout << "Failed to register user: " << regUserResp.err() << '\n';
  } else {
    utt::RegistrationSig sig = std::vector<uint8_t>(regUserResp.signature().begin(), regUserResp.signature().end());
    if (sig.empty()) throw std::runtime_error("Registration signature is empty!");

    utt::S2 s2 = std::vector<uint64_t>(regUserResp.s2().begin(), regUserResp.s2().end());
    if (s2.empty()) throw std::runtime_error("Registration param s2 is empty!");

    user_->updateRegistration(user_->getPK(), sig, s2);

    registered_ = true;

    std::cout << "Successfully registered user.\n";
  }
}

void Wallet::createPrivacyBudget(Channel& chan) {
  (void)chan;
  // [TODO-UTT] Create budget is done locally, should be done by the system
  // grpc::ClientContext ctx;

  // CreatePrivacyBudgetRequest req;
  // req.set_user_id(userId_);

  // CreatePrivacyBudgetResponse resp;
  // conn->createPrivacyBudget(&ctx, req, &resp);

  // if (resp.has_err()) {
  //   std::cout << "Failed to create privacy budget:" << resp.err() << '\n';
  // } else {
  //   utt::PrivacyBudget budget = std::vector<uint8_t>(resp.budget().begin(), resp.budget().end());
  //   utt::RegistrationSig sig = std::vector<uint8_t>(resp.signature().begin(), resp.signature().end());

  //   std::cout << "Got budget " << budget.size() << " bytes.\n";
  //   std::cout << "Got budget sig " << sig.size() << " bytes.\n";

  //   user_->updatePrivacyBudget(budget, sig);
  // }
}

void Wallet::mint(Channel& chan, uint64_t amount) {
  auto mintTx = user_->mint(amount);

  grpc::ClientContext ctx;

  WalletRequest req;
  auto& mintReq = *req.mutable_mint();
  mintReq.set_user_id(userId_);
  mintReq.set_value(amount);
  mintReq.set_tx_data(mintTx.data_.data(), mintTx.data_.size());
  chan->Write(req);

  WalletResponse resp;
  chan->Read(&resp);
  if (!resp.has_mint()) throw std::runtime_error("Expected mint response from wallet service!");
  const auto& mintResp = resp.mint();

  if (mintResp.has_err()) {
    std::cout << "Failed to mint:" << mintResp.err() << '\n';
  } else {
    std::cout << "Successfully sent mint tx. Last added tx number:" << mintResp.last_added_tx_number() << '\n';

    syncState(chan, mintResp.last_added_tx_number());
  }
}

void Wallet::transfer(Channel& chan, uint64_t amount, const std::string& recipient) {
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

  std::cout << "Processing an anonymous transfer of " << amount << " to " << recipient << "...\n";

  // Process the transfer until we get the final transaction
  // On each iteration we also sync up to the tx number of our request
  while (true) {
    auto result = user_->transfer(recipient, pki_.getPublicKey(recipient), amount);

    WalletRequest req;
    auto& transferReq = *req.mutable_transfer();
    transferReq.set_tx_data(result.requiredTx_.data_.data(), result.requiredTx_.data_.size());
    transferReq.set_num_outputs(result.requiredTx_.numOutputs_);
    chan->Write(req);

    WalletResponse resp;
    chan->Read(&resp);
    if (!resp.has_transfer()) throw std::runtime_error("Expected transfer response from wallet service!");
    const auto& transferResp = resp.transfer();

    if (transferResp.has_err()) {
      std::cout << "Failed to transfer:" << resp.err() << '\n';
    } else {
      std::cout << "Successfully sent transfer tx. Last added tx number:" << transferResp.last_added_tx_number()
                << '\n';

      syncState(chan, transferResp.last_added_tx_number());
    }

    if (result.isFinal_) break;  // Done
  }

  std::cout << "Anonymous transfer done.\n";
}

void Wallet::burn(Channel& chan, uint64_t amount) {
  if (user_->getBalance() < amount) {
    std::cout << "Insufficient private balance!\n";
    return;
  }

  std::cout << "Processing a burn operation for " << amount << "...\n";

  // Process the transfer until we get the final transaction
  // On each iteration we also sync up to the tx number of our request
  while (true) {
    auto result = user_->burn(amount);

    if (result.isFinal_) {
      WalletRequest req;
      auto& burnReq = *req.mutable_burn();
      burnReq.set_user_id(user_->getUserId());
      burnReq.set_value(amount);
      burnReq.set_tx_data(result.requiredTx_.data_.data(), result.requiredTx_.data_.size());
      chan->Write(req);

      WalletResponse resp;
      chan->Read(&resp);
      if (!resp.has_burn()) throw std::runtime_error("Expected burn response from wallet service!");
      const auto& burnResp = resp.burn();

      if (burnResp.has_err()) {
        std::cout << "Failed to do burn:" << resp.err() << '\n';
      } else {
        std::cout << "Successfully sent burn tx. Last added tx number:" << burnResp.last_added_tx_number() << '\n';

        syncState(chan, burnResp.last_added_tx_number());
      }

      break;  // Done
    } else {
      WalletRequest req;
      auto& transferReq = *req.mutable_transfer();
      transferReq.set_tx_data(result.requiredTx_.data_.data(), result.requiredTx_.data_.size());
      transferReq.set_num_outputs(result.requiredTx_.numOutputs_);
      chan->Write(req);

      WalletResponse resp;
      chan->Read(&resp);
      if (!resp.has_transfer()) throw std::runtime_error("Expected transfer response from wallet service!");
      const auto& transferResp = resp.transfer();

      if (transferResp.has_err()) {
        std::cout << "Failed to do self-transfer as part of burn:" << resp.err() << '\n';
        return;
      } else {
        std::cout << "Successfully sent self-transfer tx as part of burn. Last added tx number:"
                  << transferResp.last_added_tx_number() << '\n';

        syncState(chan, transferResp.last_added_tx_number());
      }
      // Continue with the next transaction in the burn process
    }
  }

  std::cout << "Burn operation done.\n";
}

void Wallet::syncState(Channel& chan, uint64_t lastKnownTxNum) {
  std::cout << "Synchronizing state... ";

  // Update public balance
  {
    WalletRequest req;
    req.mutable_get_public_balance()->set_user_id(userId_);
    chan->Write(req);

    WalletResponse resp;
    chan->Read(&resp);
    if (!resp.has_get_public_balance())
      throw std::runtime_error("Expected get public balance response from wallet service!");
    const auto& getPubBalanceResp = resp.get_public_balance();

    if (getPubBalanceResp.has_err()) {
      std::cout << "Failed to get public balance:" << resp.err() << '\n';
    } else {
      publicBalance_ = getPubBalanceResp.public_balance();
    }
  }

  // Sync to latest state
  if (lastKnownTxNum == 0) {
    WalletRequest req;
    req.mutable_get_last_added_tx_number();
    chan->Write(req);

    WalletResponse resp;
    chan->Read(&resp);
    if (!resp.has_get_last_added_tx_number())
      throw std::runtime_error("Expected get last added tx number response from wallet service!");
    const auto& getLastAddedTxNumResp = resp.get_last_added_tx_number();

    if (getLastAddedTxNumResp.has_err()) {
      std::cout << "Failed to get last added tx number:" << resp.err() << '\n';
    } else {
      lastKnownTxNum = getLastAddedTxNumResp.tx_number();
    }
  }

  for (uint64_t txNum = user_->getLastExecutedTxNum() + 1; txNum <= lastKnownTxNum; ++txNum) {
    WalletRequest req;
    req.mutable_get_signed_tx()->set_tx_number(txNum);
    chan->Write(req);

    WalletResponse resp;
    chan->Read(&resp);
    if (!resp.has_get_signed_tx()) throw std::runtime_error("Expected get signed tx response from wallet service!");
    const auto& getSignedTxResp = resp.get_signed_tx();

    if (getSignedTxResp.has_err()) {
      std::cout << "Failed to get signed tx with number " << txNum << ':' << resp.err() << '\n';
      return;
    }

    if (!getSignedTxResp.has_tx_number()) {
      std::cout << "Missing tx number in GetSignedTransactionResponse!\n";
      return;
    }

    utt::Transaction tx;
    std::copy(getSignedTxResp.tx_data().begin(), getSignedTxResp.tx_data().end(), std::back_inserter(tx.data_));

    utt::TxOutputSigs sigs;
    sigs.reserve((size_t)getSignedTxResp.sigs_size());
    for (const auto& sig : getSignedTxResp.sigs()) {
      sigs.emplace_back(std::vector<uint8_t>(sig.begin(), sig.end()));
    }

    // Apply transaction
    switch (getSignedTxResp.tx_type()) {
      case TxType::MINT: {
        tx.type_ = utt::Transaction::Type::Mint;
        if (sigs.size() != 1) throw std::runtime_error("Expected single signature in mint tx!");
        user_->updateMintTx(getSignedTxResp.tx_number(), tx, sigs[0]);
      } break;
      case TxType::TRANSFER: {
        tx.type_ = utt::Transaction::Type::Transfer;
        user_->updateTransferTx(getSignedTxResp.tx_number(), tx, sigs);
      } break;
      case TxType::BURN: {
        tx.type_ = utt::Transaction::Type::Burn;
        if (!sigs.empty()) throw std::runtime_error("Expected no signatures for burn tx!");
        user_->updateBurnTx(getSignedTxResp.tx_number(), tx);
      } break;
      default:
        throw std::runtime_error("Unexpected tx type!");
    }
  }

  std::cout << "Ok. (Last known tx number: " << lastKnownTxNum << ")\n";
}

void Wallet::debugOutput() const { user_->debugOutput(); }