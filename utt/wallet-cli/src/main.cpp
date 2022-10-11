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

void printHelp() {
  std::cout << "\nCommands:\n";
  std::cout << "deploy app -- generates a privacy app config and deploys it to the blockchain.\n";
  std::cout << "register -- creates a new user and registers it in a previously deployed privacy app instance\n";
  std::cout << "info -- prints information about the user managed by this wallet\n";

  // mint <amount> -- convert some amount of public funds (ERC20 tokens) to private funds (UTT tokens)
  // burn <amount> -- convert some amount of private funds (UTT tokens) to public funds (ERC20 tokens)
  // transfer <amount> <user-id> -- transfers anonymously some amount of private funds (UTT tokens) to another user
  // public balance -- print the number of ERC20 tokens the user has (needs a gRPC method to retrieve this value from
  // the wallet service) private balance -- print the number of UTT tokens the user has currently (compute locally)
  // budget -- print the currently available anonymity budget (a budget is created in advance for each user)
}

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  std::cout << "Sample Privacy Wallet CLI Application.\n";
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

  if (argc != 2) {
    std::cout << "Usage: specify the user id\n";
    return 0;
  }

  auto userId = std::string(argv[1]);

  // Check that we have test RSA keys for this user id
  // If not - print the avaialble user ids
  utt::client::TestUserPKInfrastructure pki;

  // Print out the list of valid user ids with pre-generated keys if we don't have a match.
  // This is a temp code until we can generate keys on demand for every user id.
  auto userIds = pki.getUserIds();
  auto it = std::find(userIds.begin(), userIds.end(), userId);
  if (it == userIds.end()) {
    std::cout << "Use one of the following user ids: [";
    for (const auto& userId : userIds) std::cout << userId << ' ';
    std::cout << "]\n";
    return 0;
  }

  try {
    utt::client::Initialize();

    auto wallet = Wallet(userId, pki);

    std::cout << "WARNING: The wallet will not be operational until you perform the 'deploy app' command.\nThis is a "
                 "temporary solution and will be changed.\n";

    while (true) {
      std::cout << "Enter command (type 'h' for commands 'Ctr-D' to quit):\n > ";
      std::string cmd;
      std::getline(std::cin, cmd);

      if (std::cin.eof()) {
        std::cout << "Quitting...\n";
        break;
      }

      if (cmd == "h") {
        printHelp();
      } else if (cmd == "deploy app") {
        wallet.deployApp();
      } else if (cmd == "register") {
        wallet.registerUser();
      } else if (cmd == "info") {
        wallet.showInfo();
      } else {
        std::cout << "Unknown command '" << cmd << "'\n";
      }
    }

  } catch (const std::runtime_error& e) {
    std::cout << "Error (exception): " << e.what() << '\n';
    return 1;
  }

  return 0;
}