#include <iostream>

#include <grpcpp/grpcpp.h>
#include "api.grpc.pb.h"  // Generated from utt/wallet/proto/api

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  std::cout << "UTT Wallet CLI Application.\n";

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