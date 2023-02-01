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

#include <iostream>
#include <string>
#include <sstream>
#include "testUtils/testKeys.hpp"
#include "wallet.hpp"

// [TODO-UTT] The wallet should use RocksDB to store UTT assets.
// [TODO-UTT] The wallet should use some RSA cryptography to generate public/private
// keys (used by the UTT Client Library)

// [TODO-UTT] Initial registration
// Upon first launch (no records in RocksDB) the wallet asks the user to register
// > Choose a unique user identifier:
// After this prompt the wallet sends a registration request and waits for the response
// Upon successful registration the user can use any of the following commands.

// [TODO-UTT] Startup Sequence
// A. Confirm registration -- check that the user is registered, otherwise go to "Initial registration".
// B. Synchronize
// 1) Ask about the latest signed transaction number and compare with
// the latest executed transaction in the wallet. Determine the range of tx numbers to be retrieved.
// 2) For each tx number to execute request the transaction and signature (can be combined)
// 3) Apply the transaction to the wallet state
//  a. If it's a burn or a mint transaction matching our user-id
//  b. IF it's an anonymous tx that we can claim outputs from or slash spent coins (check the nullifiers)
// [TODO-UTT] Synchronization can be optimized to require fewer requests by batching tx requests and/or filtering by
// user-id for burns and mints

// [TODO-UTT] Periodic synchronization
// We need to periodically sync with the wallet service - we can either detect this when we send requests
// (we see that there are multiple transactions that happened before ours) or we do it periodically or before
// attempt an operation.

// Note: Limited recovery from liveness issues
// In a single machine demo setting liveness issues will not be created due to the network,
// so we don't need to implement the full range of precautions to handle liveness issues
// such as timeouts.
using namespace libutt::api::testing;
void printHelp() {
  std::cout << "\nCommands:\n";
  std::cout << "config                    -- configures wallets with the privacy application.\n";
  std::cout << "show                      -- prints information about the user managed by this wallet.\n";
  std::cout << "register <user-id>        -- requests user registration required for spending coins.\n";
  std::cout << "convertPublicToPrivate <amount>             -- converts the specified amount of public funds to "
               "private funds.\n";
  std::cout << "transfer <amount> <to-user-id> -- transfers the specified amount between users.\n";
  std::cout
      << "public-transfer <amount> <to-user-id> -- transfers the specified amount of public funds between users.\n";
  std::cout << "convertPrivateToPublic <amount>             -- converts the specified amount of private funds to "
               "public funds.\n";
  std::cout << '\n';
}

struct CLIApp {
  // For this demo we assume all the private and public key are pre known

  std::string userId;
  grpc::ClientContext ctx;
  Wallet::Connection conn;
  Wallet::Channel chan;
  utt::Configuration config;
  std::unique_ptr<Wallet> wallet;

  CLIApp() {
    conn = Wallet::newConnection();
    if (!conn) throw std::runtime_error("Failed to create wallet connection!");

    chan = conn->walletChannel(&ctx);
    if (!chan) throw std::runtime_error("Failed to create wallet streaming channel!");
  }

  ~CLIApp() {
    std::cout << "Closing wallet streaming channel... ";
    chan->WritesDone();
    auto status = chan->Finish();
    std::cout << " Done.\n";
    // std::cout << "gRPC error code: " << status.error_code() << '\n';
    // std::cout << "gRPC error msg: " << status.error_message() << '\n';
    // std::cout << "gRPC error details: " << status.error_details() << '\n';
  }

  void configure() {
    if (wallet) {
      std::cout << "The wallet is already configured.\n";
      return;
    }

    auto publicConfig = Wallet::getPublicConfig(chan);

    wallet = std::make_unique<Wallet>(userId, k_TestKeys.at(userId).first, k_TestKeys.at(userId).second, publicConfig);
  }

  void registerUserCmd() {
    if (!wallet->isRegistered()) {
      wallet->registerUser(chan);
      wallet->showInfo(chan);
    } else {
      std::cout << "Wallet is already registered.\n";
    }
  }

  void showCmd() { wallet->showInfo(chan); }

  void mintCmd(const std::vector<std::string>& cmdTokens) {
    if (cmdTokens.size() != 2) {
      std::cout << "Usage: convertPublicToPrivate <amount>\n";
      return;
    }
    int amount = std::atoi(cmdTokens[1].c_str());
    if (amount <= 0) {
      std::cout << "Expected a positive public funds amount!\n";
      return;
    }
    wallet->mint(chan, (uint64_t)amount);
    wallet->showInfo(chan);
  }

  void transferCmd(const std::vector<std::string>& cmdTokens) {
    if (cmdTokens.size() != 3) {
      std::cout << "Usage: transfer <amount> <to-user-id>\n";
      return;
    }
    int amount = std::atoi(cmdTokens[1].c_str());
    if (amount <= 0) {
      std::cout << "Expected a positive transfer amount!\n";
      return;
    }
    const auto& userId = cmdTokens[2];
    wallet->transfer(chan, (uint64_t)amount, userId, k_TestKeys.at(userId).second);
    wallet->showInfo(chan);
  }

  void publicTransferCmd(const std::vector<std::string>& cmdTokens) {
    if (cmdTokens.size() != 3) {
      std::cout << "Usage: transfer <amount> <recipient>\n";
      return;
    }
    int amount = std::atoi(cmdTokens[1].c_str());
    if (amount <= 0) {
      std::cout << "Expected a positive transfer amount!\n";
      return;
    }
    wallet->publicTransfer(chan, (uint64_t)amount, cmdTokens[2]);
    wallet->showInfo(chan);
  }

  void burnCmd(const std::vector<std::string>& cmdTokens) {
    if (cmdTokens.size() != 2) {
      std::cout << "Usage: convertPrivateToPublic <amount>\n";
      return;
    }
    int amount = std::atoi(cmdTokens[1].c_str());
    if (amount <= 0) {
      std::cout << "Expected a positive private funds amount!\n";
      return;
    }
    wallet->burn(chan, (uint64_t)amount);
    wallet->showInfo(chan);
  }

  void debugCmd() { wallet->debugOutput(); }
};

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  if (argc != 2) {
    std::cout << "Provide a user-id.\n";
    return 0;
  }

  CLIApp app;
  app.userId = argv[1];

  if (k_TestKeys.find(app.userId) == k_TestKeys.end()) {
    std::cout << "Selected user id has no pre-generated keys!\n";
    std::cout << "Valid user ids: [";
    for (const auto& [userId, _] : k_TestKeys) std::cout << userId << ", ";
    std::cout << "]\n";
    return 0;
  }

  std::cout << "\nSample Privacy Wallet CLI Application.\n";
  std::cout << "UserId: " << app.userId << '\n';

  try {
    utt::client::Initialize();

    while (true) {
      std::cout << "\nEnter command (type 'h' for commands 'Ctr-D' to quit):\n > ";
      std::string cmd;
      std::getline(std::cin, cmd);

      if (std::cin.eof()) {
        std::cout << "Quitting...\n";
        break;
      }

      if (cmd == "h") {
        printHelp();
      } else if (cmd == "config") {
        app.configure();
      } else if (!app.wallet) {
        std::cout << "You must first configure the wallet. Use the 'config' command.\n";
      } else {
        // Tokenize params
        std::vector<std::string> cmdTokens;
        std::string token;
        std::stringstream ss(cmd);
        while (std::getline(ss, token, ' ')) cmdTokens.emplace_back(token);
        if (cmdTokens.empty()) continue;

        if (cmdTokens[0] == "register") {
          app.registerUserCmd();
        } else if (!app.wallet->isRegistered()) {
          std::cout << "You must first register the user. Use the 'register' command.\n";
        } else if (cmdTokens[0] == "show") {
          app.showCmd();
        } else if (cmdTokens[0] == "convertPublicToPrivate") {
          app.mintCmd(cmdTokens);
        } else if (cmdTokens[0] == "transfer") {
          app.transferCmd(cmdTokens);
        } else if (cmdTokens[0] == "public-transfer") {
          app.publicTransferCmd(cmdTokens);
        } else if (cmdTokens[0] == "convertPrivateToPublic") {
          app.burnCmd(cmdTokens);
        } else if (cmdTokens[0] == "debug") {
          app.debugCmd();
        } else {
          std::cout << "Unknown command '" << cmd << "'\n";
        }
      }
    }
  } catch (const std::runtime_error& e) {
    std::cout << "Error (exception): " << e.what() << '\n';
    return 1;
  }

  return 0;
}