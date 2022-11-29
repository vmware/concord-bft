#include "test_scenario.hpp"
#include <xutils/Log.h>

using namespace libutt;

class E2eTestScenarioConvertPrivateToPublicAboveBalance : public E2eTestScenario {
 public:
  E2eTestScenarioConvertPrivateToPublicAboveBalance(std::string description) : E2eTestScenario(description) {}
  int execute(E2eTestContext &context) override {
    const int MINTED_AMOUNT = 500;
    const int BURN_OVERFLOW = 200;

    E2eTestResult testResult = E2eTestResult::PASSED;
    E2eTestExpectedUserBalances expectedBalances;

    context.wallet1->mint(context.chanWallet, MINTED_AMOUNT);
    Admin::createPrivacyBudget(context.chanAdmin, "user-1", 10000);

    uint64_t publicBalance1Before = 0, privateBalance1Before = 0, publicBalance2Before = 0, privateBalance2Before = 0;

    std::tie(publicBalance1Before, privateBalance1Before, std::ignore) =
        context.wallet1->getBalanceInfo(context.chanWallet);

    logdbg << "publicBalance1 before: " << publicBalance1Before << ", privateBalance1 before: " << privateBalance1Before
           << std::endl;

    std::tie(publicBalance2Before, privateBalance2Before, std::ignore) =
        context.wallet2->getBalanceInfo(context.chanWallet);

    logdbg << "publicBalance2 before: " << publicBalance2Before << ", privateBalance2 before: " << privateBalance2Before
           << std::endl;

    context.wallet1->burn(context.chanWallet, privateBalance1Before + MINTED_AMOUNT + BURN_OVERFLOW);

    const uint64_t EXPECTED_PUBLIC_BALANCE_1_AFTER = publicBalance1Before;
    const uint64_t EXPECTED_PRIVATE_BALANCE_1_AFTER = privateBalance1Before;
    const uint64_t EXPECTED_PUBLIC_BALANCE_2_AFTER = publicBalance2Before;
    const uint64_t EXPECTED_PRIVATE_BALANCE_2_AFTER = privateBalance2Before;

    expectedBalances.expectedPublicBalance = std::make_optional(EXPECTED_PUBLIC_BALANCE_1_AFTER);
    expectedBalances.expectedPrivateBalance = std::make_optional(EXPECTED_PRIVATE_BALANCE_1_AFTER);
    expectedBalances.expectedPrivacyBudget = std::nullopt;
    checkExpectedBalances(context.chanWallet, context.wallet1, expectedBalances, testResult);

    expectedBalances.expectedPublicBalance = std::make_optional(EXPECTED_PUBLIC_BALANCE_2_AFTER);
    expectedBalances.expectedPrivateBalance = std::make_optional(EXPECTED_PRIVATE_BALANCE_2_AFTER);
    expectedBalances.expectedPrivacyBudget = std::nullopt;
    checkExpectedBalances(context.chanWallet, context.wallet2, expectedBalances, testResult);

    return testResult;
  }
};