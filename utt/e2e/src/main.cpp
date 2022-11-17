#include "admin.hpp"
#include "wallet.hpp"
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

 public:
  std::unique_ptr<Wallet> wallet1, wallet2, wallet3;
  Admin::Channel chanAdmin;
  Wallet::Channel chanWallet;

  E2eTestSuite() {
    utt::client::Initialize();
    chanAdmin = createAdminChannel(ctxAdmin);
    std::cout << "Deploy app\n";
    Admin::deployApp(chanAdmin);

    chanWallet = createWalletChannel(ctxWallet);

    config = Wallet::getPublicConfig(chanWallet);

    configureWallet(wallet1, "user-1");
    configureWallet(wallet2, "user-2");
    configureWallet(wallet3, "user-3");
  }

 private:
  void configureWallet(std::unique_ptr<Wallet> &wallet, std::string userId) {
    wallet = std::make_unique<Wallet>(userId, pki, config);
    wallet->registerUser(chanWallet);
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

  std::cout << "UTT Privacy demo E2E test scenario\n";

  // test scenario begins here
  testSuite.wallet1->mint(testSuite.chanWallet, 2000);
  Admin::createPrivacyBudget(testSuite.chanAdmin, "user-1", 10000);
  sleep(5);
  uint64_t publicBalance, privateBalance;
  const uint64_t EXPECTED_PUBLIC_BALANCE_1_BEFORE = 8000;
  const uint64_t EXPECTED_PRIVATE_BALANCE_1_BEFORE = 2000;
  const uint64_t EXPECTED_PUBLIC_BALANCE_2_BEFORE = 10000;
  const uint64_t EXPECTED_PRIVATE_BALANCE_2_BEFORE = 0;

  std::tie(publicBalance, privateBalance) = testSuite.wallet1->getPublicAndPrivateBalance(testSuite.chanWallet);
  logdbg << "publicBalance1 before: " << publicBalance << ", privateBalance1 before: " << privateBalance << std::endl;
  if (publicBalance != EXPECTED_PUBLIC_BALANCE_1_BEFORE or privateBalance != EXPECTED_PRIVATE_BALANCE_1_BEFORE)
    return 1;

  std::tie(publicBalance, privateBalance) = testSuite.wallet2->getPublicAndPrivateBalance(testSuite.chanWallet);
  logdbg << "publicBalance2 before: " << publicBalance << ", privateBalance2 before: " << privateBalance << std::endl;
  if (publicBalance != EXPECTED_PUBLIC_BALANCE_2_BEFORE or privateBalance != EXPECTED_PRIVATE_BALANCE_2_BEFORE)
    return 1;

  testSuite.wallet1->transfer(testSuite.chanWallet, 800, "user-2");
  testSuite.wallet1->burn(testSuite.chanWallet, 700);

  const uint64_t EXPECTED_PUBLIC_BALANCE_1_AFTER = 8700;
  const uint64_t EXPECTED_PRIVATE_BALANCE_1_AFTER = 500;
  const uint64_t EXPECTED_PUBLIC_BALANCE_2_AFTER = 10000;
  const uint64_t EXPECTED_PRIVATE_BALANCE_2_AFTER = 800;

  std::tie(publicBalance, privateBalance) = testSuite.wallet1->getPublicAndPrivateBalance(testSuite.chanWallet);
  logdbg << "publicBalance1 after: " << publicBalance << ", privateBalance1 after: " << privateBalance << std::endl;
  if (publicBalance != EXPECTED_PUBLIC_BALANCE_1_AFTER or privateBalance != EXPECTED_PRIVATE_BALANCE_1_AFTER) return 1;

  std::tie(publicBalance, privateBalance) = testSuite.wallet2->getPublicAndPrivateBalance(testSuite.chanWallet);
  logdbg << "publicBalance2 after: " << publicBalance << ", privateBalance2 after: " << privateBalance << std::endl;
  if (publicBalance != EXPECTED_PUBLIC_BALANCE_2_AFTER or privateBalance != EXPECTED_PRIVATE_BALANCE_2_AFTER) return 1;

  logdbg << "TEST PASSED\n";
  return 0;
}