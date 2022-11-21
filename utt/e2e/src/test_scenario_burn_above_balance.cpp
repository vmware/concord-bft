#include "test_scenario.hpp"
#include <xutils/Log.h>

using namespace libutt;

class E2eTestScenarioBurnAboveBalance : public E2eTestScenario {
 public:
  E2eTestScenarioBurnAboveBalance(E2eTestContext &context, std::string description)
      : E2eTestScenario(context, description) {}
  int execute() override {
    const int MINTED_AMOUNT = 500;
    const int BURN_OVERFLOW = 200;

    context.wallet1->mint(context.chanWallet, MINTED_AMOUNT);
    Admin::createPrivacyBudget(context.chanAdmin, "user-1", 10000);

    uint64_t publicBalance1Before, privateBalance1Before, publicBalance2Before, privateBalance2Before;
    uint64_t publicBalance1After, privateBalance1After, publicBalance2After, privateBalance2After;

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

    std::tie(publicBalance1After, privateBalance1After, std::ignore) =
        context.wallet1->getBalanceInfo(context.chanWallet);
    logdbg << "publicBalance1 after: " << publicBalance1After << ", privateBalance1 after: " << privateBalance1After
           << std::endl;
    if (publicBalance1After != EXPECTED_PUBLIC_BALANCE_1_AFTER or
        privateBalance1After != EXPECTED_PRIVATE_BALANCE_1_AFTER)
      return 1;

    std::tie(publicBalance2After, privateBalance2After, std::ignore) =
        context.wallet2->getBalanceInfo(context.chanWallet);
    logdbg << "publicBalance2 after: " << publicBalance2After << ", privateBalance2 after: " << privateBalance2After
           << std::endl;
    if (publicBalance2After != EXPECTED_PUBLIC_BALANCE_2_AFTER or
        privateBalance2After != EXPECTED_PRIVATE_BALANCE_2_AFTER)
      return 1;

    logdbg << "TEST PASSED\n";
    return 0;
  }
};