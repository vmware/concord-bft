#include "test_scenario.hpp"
#include <xutils/Log.h>
#include <optional>

using namespace libutt;

void E2eTestScenario::checkExpectedBalances(Wallet::Channel &chanWallet,
                                            std::unique_ptr<Wallet> &wallet,
                                            const E2eTestExpectedUserBalances &expectedBalances,
                                            E2eTestResult &result) {
  uint64_t publicBalance = 0, privateBalance = 0, privacyBudget = 0;
  std::tie(publicBalance, privateBalance, privacyBudget) = wallet->getBalanceInfo(chanWallet);

  logdbg << "User:" << wallet->getUserId() << " public balance: " << publicBalance
         << ", private balance: " << privateBalance << ", privacy budget: " << privacyBudget << std::endl;

  if (expectedBalances.expectedPublicBalance != std::nullopt and
      publicBalance != expectedBalances.expectedPublicBalance.value()) {
    std::cout << "expectedPublicBalance MISMATCH, expected: " << expectedBalances.expectedPublicBalance.value()
              << ", actual: " << publicBalance << std::endl;
    result = E2eTestResult::FAILED;
  } else if (expectedBalances.expectedPrivateBalance != std::nullopt and
             privateBalance != expectedBalances.expectedPrivateBalance.value()) {
    std::cout << "expectedPrivateBalance MISMATCH, expected: " << expectedBalances.expectedPrivateBalance.value()
              << ", actual: " << privateBalance << std::endl;
    result = E2eTestResult::FAILED;
  } else if (expectedBalances.expectedPrivacyBudget != std::nullopt and
             privacyBudget != expectedBalances.expectedPrivacyBudget.value()) {
    std::cout << "expectedPrivacyBudget MISMATCH, expected: " << expectedBalances.expectedPrivacyBudget.value()
              << ", actual: " << privacyBudget << std::endl;
    result = E2eTestResult::FAILED;
  }
}