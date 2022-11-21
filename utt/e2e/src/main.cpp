#include "admin.hpp"
#include "wallet.hpp"
#include "test_base_scenario.cpp"
#include "test_scenario_burn_above_balance.cpp"
#include "test_scenario_transfer_above_budget.cpp"
#include "test_scenario_transfer_above_balance.cpp"
#include "test_scenario_mint_above_balance.cpp"
#include <iostream>
#include <unistd.h>
#include <xutils/Log.h>

using namespace libutt;

class E2eTestSuite {
 private:
  std::string userId;
  grpc::ClientContext ctxWallet;
  grpc::ClientContext ctxAdmin;
  utt::Configuration config;
  utt::client::TestUserPKInfrastructure pki;
  E2eTestContext context;

  std::list<std::unique_ptr<E2eTestScenario>> testScenarios;

 public:
  E2eTestSuite() {
    utt::client::Initialize();
    context.chanAdmin = createAdminChannel(ctxAdmin);
    std::cout << "Deploy app\n";
    Admin::deployApp(context.chanAdmin);

    context.chanWallet = createWalletChannel(ctxWallet);

    config = Wallet::getPublicConfig(context.chanWallet);

    configureWallet(context.wallet1, "user-1");
    configureWallet(context.wallet2, "user-2");
    configureWallet(context.wallet3, "user-3");

    testScenarios.push_back(
        std::make_unique<E2eTestBaseScenario>(context, "Mint transfer and burn should result in balance change"));
    testScenarios.push_back(std::make_unique<E2eTestScenarioBurnAboveBalance>(
        context, "Burn above private balance should not change balance"));
    testScenarios.push_back(std::make_unique<E2eTestScenarioTransferAboveBudget>(
        context, "Transfer above privacy budget should not change balance"));
    testScenarios.push_back(std::make_unique<E2eTestScenarioTransferAboveBalance>(
        context, "Transfer above private balance should not change balance"));
    testScenarios.push_back(std::make_unique<E2eTestScenarioMintAboveBalance>(
        context, "Mint above public balance should not change balance"));
  }

  bool run() {
    bool failed = false;
    for (auto &test : testScenarios) {
      int result = test->execute();
      if (0 != result) {
        logdbg << "Test failed, status: " << result << ", test description: " << test->getDescription() << std::endl;
        failed = true;
      }
    }
    return failed;
  }

 private:
  void configureWallet(std::unique_ptr<Wallet> &wallet, std::string userId) {
    wallet = std::make_unique<Wallet>(userId, pki, config);
    wallet->registerUser(context.chanWallet);
  }

  Admin::Channel createAdminChannel(grpc::ClientContext &ctxAdmin) {
    Admin::Connection connAdmin;
    Admin::Channel chanAdmin;

    connAdmin = Admin::newConnection();
    if (!connAdmin) throw std::runtime_error("Failed to create admin connection!");

    chanAdmin = connAdmin->adminChannel(&ctxAdmin);
    if (!chanAdmin) throw std::runtime_error("Failed to create admin streaming channel!");

    return chanAdmin;
  }

  Wallet::Channel createWalletChannel(grpc::ClientContext &ctxWallet) {
    Wallet::Connection connWallet;
    Wallet::Channel chanWallet;
    connWallet = Wallet::newConnection();
    if (!connWallet) throw std::runtime_error("Failed to create wallet connection!");

    chanWallet = connWallet->walletChannel(&ctxWallet);
    if (!chanWallet) throw std::runtime_error("Failed to create wallet streaming channel!");

    return chanWallet;
  }
};

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  E2eTestSuite testSuite = E2eTestSuite();
  testSuite.run();
  return 0;
}