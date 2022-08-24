#include <iostream>

#include <grpcpp/grpcpp.h>
#include "api.grpc.pb.h"  // Generated from utt/wallet/proto/api

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  std::cout << "UTT Wallet CLI Application.\n";
  // [TODO-UTT] The wallet should use the UTT Client Library for UTT requests.
  // [TODO-UTT] The wallet should use RocksDB to store UTT assets.
  // [TODO-UTT] The wallet should use some RSA cryptography to generate public/private
  // keys (used by the UTT Client Library)

  // [TODO-UTT] Initial registration
  // Upon first launch (no records in RocksDB) the wallet asks the user to register
  // > Choose a unique user identifier:
  // After this prompt the wallet sends a registration request and waits for the response
  // Upon successfull registration the user can use any of the following commands.

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

  // gRPC
  try {
    std::string grpcServerAddr = "127.0.0.1:49000";

    std::cout << "Connecting to gRPC server at " << grpcServerAddr << " ...\n";

    using namespace vmware::concord::utt::wallet::api::v1;

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

    auto stub = TestService::NewStub(chan);
    if (!stub) {
      throw std::runtime_error("Failed to create gRPC client stub.");
    }

    while (true) {
      std::cout << "Enter ASCII string to be reversed (Ctr-D to quit):\n > ";
      std::string input;
      std::cin >> input;

      if (std::cin.eof()) {
        std::cout << "Quitting...\n";
        break;
      }

      if (!input.empty()) {
        grpc::ClientContext ctx;

        ReverseStringRequest req;
        req.set_input(input);

        ReverseStringResponse resp;
        stub->reverseString(&ctx, req, &resp);

        std::cout << " > Response: " << resp.output() << '\n';
      }
    }

  } catch (const std::runtime_error& e) {
    std::cout << "Error (exception): " << e.what() << '\n';
    return 1;
  }

  return 0;
}