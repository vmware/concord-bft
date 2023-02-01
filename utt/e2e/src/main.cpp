#include "admin.hpp"
#include "wallet.hpp"
#include "testUtils/testKeys.hpp"
#include "test_base_scenario.cpp"
#include "test_scenario_convertPrivateToPublic_above_balance.cpp"
#include "test_scenario_transfer_above_budget.cpp"
#include "test_scenario_transfer_above_balance.cpp"
#include "test_scenario_convertPublicToPrivate_above_balance.cpp"
#include "assertUtils.hpp"
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
        std::make_unique<E2eTestBaseScenario>("Mint transfer and burn should result in balance change"));
    testScenarios.push_back(std::make_unique<E2eTestScenarioConvertPrivateToPublicAboveBalance>(
        "Burn above private balance should not change balance"));
    testScenarios.push_back(std::make_unique<E2eTestScenarioTransferAboveBalance>(
        "Transfer above private balance should not change balance"));
    testScenarios.push_back(std::make_unique<E2eTestScenarioTransferAboveBudget>(
        "Transfer above privacy budget should not change balance"));
    testScenarios.push_back(std::make_unique<E2eTestScenarioConvertPublicToPrivateAboveBalance>(
        "Mint above public balance should not change balance"));
  }

  int run() {
    int failedCount = 0, passedCount = 0;
    Admin::createPrivacyBudget(context.chanAdmin, "user-1", 5000);
    for (auto &test : testScenarios) {
      assertWalletsPresent();
      int result = test->execute(context);
      if (E2eTestResult::PASSED != result) {
        std::cout << "Test\033[31m FAILED\033[0m, status: " << result
                  << ", test description: " << test->getDescription() << std::endl;
        ++failedCount;
      } else {
        ++passedCount;
        std::cout << "Test\033[32m PASSED\033[0m, test description: " << test->getDescription() << std::endl;
      }
    }
    std::cout << "Privacy feature E2E tests completed, \033[32m PASSED \033[0m: " << passedCount
              << ", \033[31m FAILED \033[0m: " << failedCount << std::endl;
    return failedCount;
  }

 private:
  void configureWallet(std::unique_ptr<Wallet> &wallet, const std::string &userId) {
    wallet = std::make_unique<Wallet>(userId,
                                      libutt::api::testing::k_TestKeys.at(userId).first,
                                      libutt::api::testing::k_TestKeys.at(userId).second,
                                      config);
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

  void assertWalletsPresent() {
    ConcordAssert(nullptr != context.wallet1);
    ConcordAssert(nullptr != context.wallet2);
    ConcordAssert(nullptr != context.wallet3);
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
  int failedCount = testSuite.run();
  return failedCount;
}