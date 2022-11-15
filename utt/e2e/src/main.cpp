#include "admin.hpp"
#include "wallet.hpp"
#include <iostream>
#include <unistd.h>
#include <xutils/Log.h>

using namespace libutt;

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

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  utt::client::Initialize();

  std::cout << "UTT Privacy demo E2E test scenario\n";
  grpc::ClientContext ctxAdmin;
  Admin::Channel chanAdmin = createAdminChannel(ctxAdmin);
  std::cout << "Deploy app\n";
  Admin::deployApp(chanAdmin);

  std::string userId;
  grpc::ClientContext ctxWallet;
  utt::Configuration config;
  utt::client::TestUserPKInfrastructure pki;
  std::unique_ptr<Wallet> wallet1, wallet2, wallet3;

  Wallet::Channel chanWallet = createWalletChannel(ctxWallet);

  config = Wallet::getPublicConfig(chanWallet);

  logdbg << "Configuring wallets 1 - 3" << std::endl;
  wallet1 = std::make_unique<Wallet>("user-1", pki, config);
  wallet1->registerUser(chanWallet);

  logdbg << "Configuring wallet3" << std::endl;
  wallet3 = std::make_unique<Wallet>("user-3", pki, config);
  wallet3->registerUser(chanWallet);

  logdbg << "Configuring wallet2" << std::endl;
  wallet2 = std::make_unique<Wallet>("user-2", pki, config);
  wallet2->registerUser(chanWallet);

  logdbg << "Wallets configured. Minting and creating privacy budget..." << std::endl;
  wallet1->mint(chanWallet, 2000);
  Admin::createPrivacyBudget(chanAdmin, "user-1", 10000);
  sleep(5);
  uint64_t publicBalance, privateBalance;
  const uint64_t EXPECTED_PUBLIC_BALANCE_1_BEFORE = 8000;
  const uint64_t EXPECTED_PRIVATE_BALANCE_1_BEFORE = 2000;
  const uint64_t EXPECTED_PUBLIC_BALANCE_2_BEFORE = 10000;
  const uint64_t EXPECTED_PRIVATE_BALANCE_2_BEFORE = 0;

  std::tie(publicBalance, privateBalance) = wallet1->getPublicAndPrivateBalance(chanWallet);
  logdbg << "publicBalance1 before: " << publicBalance << ", privateBalance1 before: " << privateBalance << std::endl;
  if (publicBalance != EXPECTED_PUBLIC_BALANCE_1_BEFORE or privateBalance != EXPECTED_PRIVATE_BALANCE_1_BEFORE)
    return 1;

  std::tie(publicBalance, privateBalance) = wallet2->getPublicAndPrivateBalance(chanWallet);
  logdbg << "publicBalance2 before: " << publicBalance << ", privateBalance2 before: " << privateBalance << std::endl;
  if (publicBalance != EXPECTED_PUBLIC_BALANCE_2_BEFORE or privateBalance != EXPECTED_PRIVATE_BALANCE_2_BEFORE)
    return 1;

  logdbg << "Performing transfer and burn" << std::endl;
  wallet1->transfer(chanWallet, 800, "user-2");
  wallet1->burn(chanWallet, 700);

  const uint64_t EXPECTED_PUBLIC_BALANCE_1_AFTER = 8700;
  const uint64_t EXPECTED_PRIVATE_BALANCE_1_AFTER = 500;
  const uint64_t EXPECTED_PUBLIC_BALANCE_2_AFTER = 10000;
  const uint64_t EXPECTED_PRIVATE_BALANCE_2_AFTER = 800;

  std::tie(publicBalance, privateBalance) = wallet1->getPublicAndPrivateBalance(chanWallet);
  logdbg << "publicBalance1 after: " << publicBalance << ", privateBalance1 after: " << privateBalance << "\n";
  if (publicBalance != EXPECTED_PUBLIC_BALANCE_1_AFTER or privateBalance != EXPECTED_PRIVATE_BALANCE_1_AFTER) return 1;

  std::tie(publicBalance, privateBalance) = wallet2->getPublicAndPrivateBalance(chanWallet);
  logdbg << "publicBalance2 after: " << publicBalance << ", privateBalance2 after: " << privateBalance << "\n";
  if (publicBalance != EXPECTED_PUBLIC_BALANCE_2_AFTER or privateBalance != EXPECTED_PRIVATE_BALANCE_2_AFTER) return 1;

  logdbg << "TEST PASSED\n";
  return 0;
}