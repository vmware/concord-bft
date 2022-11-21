#include "test_scenario.hpp"
#include <xutils/Log.h>

using namespace libutt;

class E2eTestScenarioTransferAboveBalance : public E2eTestScenario {
 public:
  E2eTestScenarioTransferAboveBalance(E2eTestContext &context, std::string description)
      : E2eTestScenario(context, description) {}
  int execute() override {
    const int TRANSFER_OVERFLOW = 800;

    uint64_t publicBalance1Before, privateBalance1Before, publicBalance2Before, privateBalance2Before,
        privacyBudget1Before, privacyBudget2Before;
    uint64_t publicBalance1After, privateBalance1After, publicBalance2After, privateBalance2After, privacyBudget1After,
        privacyBudget2After;

    std::tie(publicBalance1Before, privateBalance1Before, privacyBudget1Before) =
        context.wallet1->getBalanceInfo(context.chanWallet);
    logdbg << "publicBalance1 before: " << publicBalance1Before << ", privateBalance1 before: " << privateBalance1Before
           << ", privacyBudget1 before: " << privacyBudget1Before << std::endl;

    std::tie(publicBalance2Before, privateBalance2Before, privacyBudget2Before) =
        context.wallet2->getBalanceInfo(context.chanWallet);
    logdbg << "publicBalance2 before: " << publicBalance2Before << ", privateBalance2 before: " << privateBalance2Before
           << ", privacyBudget2 before: " << privacyBudget2Before << std::endl;

    if (privateBalance1Before > privacyBudget1Before) return E2eTestResult::PREREQUISITES_NOT_MET;
    context.wallet1->transfer(context.chanWallet, privateBalance1Before + TRANSFER_OVERFLOW, "user-2");

    const uint64_t EXPECTED_PUBLIC_BALANCE_1_AFTER = publicBalance1Before;
    const uint64_t EXPECTED_PRIVATE_BALANCE_1_AFTER = privateBalance1Before;
    const uint64_t EXPECTED_PRIVACY_BUDGET_1_AFTER = privacyBudget1Before;
    const uint64_t EXPECTED_PUBLIC_BALANCE_2_AFTER = publicBalance2Before;
    const uint64_t EXPECTED_PRIVATE_BALANCE_2_AFTER = privateBalance2Before;
    const uint64_t EXPECTED_PRIVACY_BUDGET_2_AFTER = privacyBudget2Before;

    std::tie(publicBalance1After, privateBalance1After, privacyBudget1After) =
        context.wallet1->getBalanceInfo(context.chanWallet);
    logdbg << "publicBalance1 after: " << publicBalance1After << ", privateBalance1 after: " << privateBalance1After
           << ", privacyBudget1 after: " << privacyBudget1After << std::endl;
    if (publicBalance1After != EXPECTED_PUBLIC_BALANCE_1_AFTER or
        privateBalance1After != EXPECTED_PRIVATE_BALANCE_1_AFTER or
        privacyBudget1After != EXPECTED_PRIVACY_BUDGET_1_AFTER)
      return E2eTestResult::FAILED;

    std::tie(publicBalance2After, privateBalance2After, privacyBudget2After) =
        context.wallet2->getBalanceInfo(context.chanWallet);
    logdbg << "publicBalance2 after: " << publicBalance2After << ", privateBalance2 after: " << privateBalance2After
           << ", privacyBudget2 after: " << privacyBudget2After << std::endl;
    if (publicBalance2After != EXPECTED_PUBLIC_BALANCE_2_AFTER or
        privateBalance2After != EXPECTED_PRIVATE_BALANCE_2_AFTER or
        privacyBudget2After != EXPECTED_PRIVACY_BUDGET_2_AFTER)
      return E2eTestResult::FAILED;

    return E2eTestResult::PASSED;
  }
};