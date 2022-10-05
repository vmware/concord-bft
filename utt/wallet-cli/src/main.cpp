#include <iostream>

#include <grpcpp/grpcpp.h>
#include "api.grpc.pb.h"  // Generated from utt/wallet/proto/api

#include "utt-client-api/ClientApi.hpp"
#include "utt-client-api/TestKeys.hpp"

#include <map>
#include <string>
#include <sstream>

using namespace vmware::concord::utt::wallet::api::v1;

using GrpcWalletService = std::unique_ptr<WalletService::Stub>;

GrpcWalletService connectToGrpcWalletService() {
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

void printHelp() {
  std::cout << "\nCommands:\n";
  std::cout << "deploy app -- generates a privacy app config and deploys it to the blockchain.\n";
  std::cout
      << "register <userId> -- creates a new user and registers it in a previously deployed privacy app instance\n";
  std::cout << "show <userId> -- prints information about a user created by this wallet\n";

  // mint <amount> -- convert some amount of public funds (ERC20 tokens) to private funds (UTT tokens)
  // burn <amount> -- convert some amount of private funds (UTT tokens) to public funds (ERC20 tokens)
  // transfer <amount> <user-id> -- transfers anonymously some amount of private funds (UTT tokens) to another user
  // public balance -- print the number of ERC20 tokens the user has (needs a gRPC method to retrieve this value from
  // the wallet service) private balance -- print the number of UTT tokens the user has currently (compute locally)
  // budget -- print the currently available anonymity budget (a budget is created in advance for each user)
}

class Wallet {
 public:
  void showUser(const std::string& userId) {
    auto it = users_.find(userId);
    if (it == users_.end()) {
      std::cout << "User '" << userId << "' does not exist.\n";
      return;
    }

    std::cout << "User Id: " << it->first << '\n';
    std::cout << "Private balance: " << it->second->getBalance() << '\n';
    std::cout << "Privacy budget: " << it->second->getPrivacyBudget() << '\n';
    std::cout << "Last executed transaction number: " << it->second->getLastExecutedTxNum() << '\n';
  }

  void deployApp(GrpcWalletService& grpc) {
    if (!deployedAppId_.empty()) {
      std::cout << "Privacy app '" << deployedAppId_ << "' already deployed\n";
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
    grpc->deployPrivacyApp(&ctx, req, &resp);

    // Note that keeping the config around in memory is just for a demo purpose and should not happen in real system
    if (resp.has_err()) {
      std::cout << "Failed to deploy privacy app: " << resp.err() << '\n';
    } else if (resp.app_id().empty()) {
      std::cout << "Failed to deploy privacy app: empty app id!\n";
    } else {
      deployedAppId_ = resp.app_id();
      // We need the public config part which can typically be obtained from the service, but we keep it for simplicity
      deployedPublicConfig_ = std::make_unique<utt::PublicConfig>(utt::client::getPublicConfig(config));
      std::cout << "Successfully deployed privacy app with id: " << deployedAppId_ << '\n';
    }
  }

  void registerUser(const std::string& userId, GrpcWalletService& grpc) {
    if (userId.empty()) throw std::runtime_error("Cannot register user with empty user id!");
    if (deployedAppId_.empty()) {
      std::cout << "You must first deploy a privacy app to register a user. Use command 'deploy app'\n";
      return;
    }

    if (users_.find(userId) != users_.end()) {
      std::cout << "User '" << userId << " is already registered!\n";
      return;
    }

    auto user = utt::client::createUser(userId, *deployedPublicConfig_, pki_, storage_);
    if (!user) throw std::runtime_error("Failed to create user!");

    auto userRegInput = user->getRegistrationInput();
    if (userRegInput.empty()) throw std::runtime_error("Failed to create user registration input!");

    grpc::ClientContext ctx;

    RegisterUserRequest req;
    req.set_user_id(userId);
    req.set_input_rcm(userRegInput.data(), userRegInput.size());
    req.set_user_pk(user->getPK());

    RegisterUserResponse resp;
    grpc->registerUser(&ctx, req, &resp);

    if (resp.has_err()) {
      std::cout << "Failed to register user: " << resp.err() << '\n';
    } else {
      utt::RegistrationSig sig = std::vector<uint8_t>(resp.signature().begin(), resp.signature().end());
      std::cout << "Got sig for registration with size: " << sig.size() << '\n';

      utt::S2 s2 = std::vector<uint64_t>(resp.s2().begin(), resp.s2().end());
      std::cout << "Got S2 for registration: [";
      for (const auto& val : s2) std::cout << val << ' ';
      std::cout << "]\n";

      if (!user->updateRegistration(user->getPK(), sig, s2)) {
        std::cout << "Failed to update user's registration!\n";
      } else {
        std::cout << "Successfully registered user with id '" << user->getUserId() << "'\n";
        users_.emplace(userId, std::move(user));
      }
    }
  }

 private:
  struct DummyUserPKInfrastructure : public utt::client::IUserPKInfrastructure {
    utt::client::IUserPKInfrastructure::KeyPair generateKeys(const std::string& userId) override {
      const auto& keys = utt::client::test::getKeysForUser(userId);
      if (keys.first.empty()) throw std::runtime_error("No test private key for " + userId);
      if (keys.second.empty()) throw std::runtime_error("No test public key for " + userId);
      return utt::client::IUserPKInfrastructure::KeyPair{keys.first, keys.second};
    }
  };

  struct DummyUserStorage : public utt::client::IUserStorage {};

  std::unique_ptr<utt::PublicConfig> deployedPublicConfig_;
  std::string deployedAppId_;
  DummyUserPKInfrastructure pki_;
  DummyUserStorage storage_;
  std::map<std::string, std::unique_ptr<utt::client::User>> users_;
};

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

  try {
    auto grpc = connectToGrpcWalletService();
    if (!grpc) throw std::runtime_error("Failed to create gRPC client stub.");

    utt::client::Initialize();

    auto wallet = Wallet();

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
        wallet.deployApp(grpc);
      } else {
        // Tokenize command
        std::vector<std::string> cmdTokens;
        {
          std::stringstream ss(cmd);
          std::string t;
          while (std::getline(ss, t, ' ')) cmdTokens.emplace_back(std::move(t));
        }
        if (cmdTokens.empty()) continue;

        if (cmdTokens[0] == "register") {
          if (cmdTokens.size() != 2) {
            std::cout << "Usage: specify the user id to register.\n";
          } else {
            wallet.registerUser(cmdTokens[1], grpc);
          }
        } else if (cmdTokens[0] == "show") {
          if (cmdTokens.size() != 2) {
            std::cout << "Usage: specify the user id to show,\n";
          } else {
            wallet.showUser(cmdTokens[1]);
          }
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