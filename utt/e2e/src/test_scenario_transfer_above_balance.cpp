#include "test_scenario.hpp"
#include <xutils/Log.h>

using namespace libutt;

class E2eTestScenarioTransferAboveBalance : public E2eTestScenario {
 public:
  E2eTestScenarioTransferAboveBalance(std::string description) : E2eTestScenario(description) {}
  int execute(E2eTestContext &context) override {
    const int TRANSFER_OVERFLOW = 800;

    E2eTestResult testResult = E2eTestResult::PASSED;
    E2eTestExpectedUserBalances expectedBalances;

    uint64_t publicBalance1Before = 0, privateBalance1Before = 0, publicBalance2Before = 0, privateBalance2Before = 0,
             privacyBudget1Before = 0, privacyBudget2Before = 0;

    std::tie(publicBalance1Before, privateBalance1Before, privacyBudget1Before) =
        context.wallet1->getBalanceInfo(context.chanWallet);

    logdbg << "publicBalance1 before: " << publicBalance1Before << ", privateBalance1 before: " << privateBalance1Before
           << ", privacyBudget1 before: " << privacyBudget1Before << std::endl;

    std::tie(publicBalance2Before, privateBalance2Before, privacyBudget2Before) =
        context.wallet2->getBalanceInfo(context.chanWallet);

    logdbg << "publicBalance2 before: " << publicBalance2Before << ", privateBalance2 before: " << privateBalance2Before
           << ", privacyBudget2 before: " << privacyBudget2Before << std::endl;

    if (privateBalance1Before > privacyBudget1Before) return E2eTestResult::PREREQUISITES_NOT_MET;
    context.wallet1->transfer(context.chanWallet,
                              privateBalance1Before + TRANSFER_OVERFLOW,
                              "user-2",
                              E2eTestKeys::k_TestKeys.at("user-2").second);

    const uint64_t EXPECTED_PUBLIC_BALANCE_1_AFTER = publicBalance1Before;
    const uint64_t EXPECTED_PRIVATE_BALANCE_1_AFTER = privateBalance1Before;
    const uint64_t EXPECTED_PRIVACY_BUDGET_1_AFTER = privacyBudget1Before;
    const uint64_t EXPECTED_PUBLIC_BALANCE_2_AFTER = publicBalance2Before;
    const uint64_t EXPECTED_PRIVATE_BALANCE_2_AFTER = privateBalance2Before;
    const uint64_t EXPECTED_PRIVACY_BUDGET_2_AFTER = privacyBudget2Before;

    expectedBalances.expectedPublicBalance = std::make_optional(EXPECTED_PUBLIC_BALANCE_1_AFTER);
    expectedBalances.expectedPrivateBalance = std::make_optional(EXPECTED_PRIVATE_BALANCE_1_AFTER);
    expectedBalances.expectedPrivacyBudget = std::make_optional(EXPECTED_PRIVACY_BUDGET_1_AFTER);
    checkExpectedBalances(context.chanWallet, context.wallet1, expectedBalances, testResult);

    expectedBalances.expectedPublicBalance = std::make_optional(EXPECTED_PUBLIC_BALANCE_2_AFTER);
    expectedBalances.expectedPrivateBalance = std::make_optional(EXPECTED_PRIVATE_BALANCE_2_AFTER);
    expectedBalances.expectedPrivacyBudget = std::make_optional(EXPECTED_PRIVACY_BUDGET_2_AFTER);
    checkExpectedBalances(context.chanWallet, context.wallet2, expectedBalances, testResult);

    return testResult;
  }
};