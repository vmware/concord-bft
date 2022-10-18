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

// [TODO-UTT] Commands:
// mint <amount> -- convert some amount of public funds (ERC20 tokens) to private funds (UTT tokens)
// burn <amount> -- convert some amount of private funds (UTT tokens) to public funds (ERC20 tokens)
// transfer <amount> <user-id> -- transfers anonymously some amount of private funds (UTT tokens) to another user
// public balance -- print the number of ERC20 tokens the user has (needs a gRPC method to retrieve this value from
// the wallet service) private balance -- print the number of UTT tokens the user has currently (compute locally)
// budget -- print the currently available anonymity budget (a budget is created in advance for each user)

void printHelp() {
  std::cout << "\nCommands:\n";
  std::cout << "deploy                    -- generates a privacy app config, deploys it on the blockchain and creates "
               "test users.\n";
  std::cout << "show <user-id>            -- prints information about the user managed by this wallet\n";
  std::cout << "register <user-id>        -- requests user registration required for spending coins\n";
  std::cout
      << "create-budget <user-id>   -- requests creation of a privacy budget, the amount is decided by the system.\n";
  std::cout << "mint <user-id> <amount>   -- mint the requested amount of public funds.\n";
  std::cout << "transfer <amount> <from-user-id> <to-user-id> -- transfers the specified amount between users.\n";
  std::cout << "burn <user-id> <amount>   -- burns the specified amount of private funds to public funds.\n";
  std::cout << '\n';
}

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;
  std::cout << "Sample Privacy Wallet CLI Application.\n";

  bool deployed = false;

  try {
    utt::client::Initialize();

    std::map<std::string, Wallet> wallets;

    auto conn = Wallet::newConnection();

    utt::Configuration config;

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
      } else if (cmd == "deploy") {
        if (deployed) {
          std::cout << "The privacy app is already deployed.\n";
        } else {
          auto configs = Wallet::deployApp(conn);
          config = std::move(configs.first);  // Save the full config for creating budgets locally later

          utt::client::TestUserPKInfrastructure pki;
          auto testUserIds = pki.getUserIds();
          for (const auto& userId : testUserIds) {
            std::cout << "Creating test user with id '" << userId << "'\n";
            wallets.emplace(userId, Wallet(userId, pki, configs.second));
          }

          deployed = true;
        }
      } else if (!deployed) {
        std::cout << "You must first deploy the privacy application. Use the 'deploy' command.\n";
      } else {
        // Tokenize params
        std::vector<std::string> cmdTokens;
        std::string token;
        std::stringstream ss(cmd);
        while (std::getline(ss, token, ' ')) cmdTokens.emplace_back(token);

        if (!cmdTokens.empty()) {
          if (cmdTokens[0] == "register") {
            if (cmdTokens.size() != 2) {
              std::cout << "Usage: register <user-id>\n";
            } else {
              wallets.at(cmdTokens[1]).registerUser(conn);
            }
          } else if (cmdTokens[0] == "create-budget") {
            if (cmdTokens.size() != 2) {
              std::cout << "Usage: create-budget <user-id>\n";
            } else {
              wallets.at(cmdTokens[1]).createPrivacyBudgetLocal(config, 10000);
            }
          } else if (cmdTokens[0] == "show") {
            if (cmdTokens.size() != 2) {
              std::cout << "Usage: show <user-id>\n";
            } else {
              wallets.at(cmdTokens[1]).showInfo(conn);
            }
          } else if (cmdTokens[0] == "mint") {
            if (cmdTokens.size() != 3) {
              std::cout << "Usage: mint <user-id> <amount>\n";
            } else {
              int amount = std::atoi(cmdTokens[2].c_str());
              if (amount <= 0) {
                std::cout << "Expected a positive mint amount!\n";
              } else {
                wallets.at(cmdTokens[1]).mint(conn, (uint64_t)amount);
              }
            }
          } else if (cmdTokens[0] == "transfer") {
            if (cmdTokens.size() != 4) {
              std::cout << "Usage: transfer <amount> <from-user-id> <to-user-id>\n";
            } else {
              int amount = std::atoi(cmdTokens[1].c_str());
              if (amount <= 0) {
                std::cout << "Expected a positive transfer amount!\n";
              } else {
                wallets.at(cmdTokens[2]).transfer(conn, (uint64_t)amount, cmdTokens[3]);
              }
            }
          } else if (cmdTokens[0] == "burn") {
            if (cmdTokens.size() != 3) {
              std::cout << "Usage: burn <user-id> <amount>\n";
            } else {
              int amount = std::atoi(cmdTokens[2].c_str());
              if (amount <= 0) {
                std::cout << "Expected a positive burn amount!\n";
              } else {
                wallets.at(cmdTokens[1]).burn(conn, (uint64_t)amount);
              }
            }
          } else {
            std::cout << "Unknown command '" << cmd << "'\n";
          }
        }
      }
    }

  } catch (const std::runtime_error& e) {
    std::cout << "Error (exception): " << e.what() << '\n';
    return 1;
  }

  return 0;
}