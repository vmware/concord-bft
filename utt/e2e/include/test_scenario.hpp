#pragma once

#include "wallet.hpp"
#include "admin.hpp"
#include <string>
#include <optional>
#include <memory>

enum E2eTestResult { PASSED = 0, FAILED = 1, PREREQUISITES_NOT_MET = 2 };

struct E2eTestContext {
  std::unique_ptr<Wallet> wallet1, wallet2, wallet3;
  Admin::Channel chanAdmin;
  Wallet::Channel chanWallet;
};

struct E2eTestExpectedUserBalances {
  std::optional<uint64_t> expectedPublicBalance;
  std::optional<uint64_t> expectedPrivateBalance;
  std::optional<uint64_t> expectedPrivacyBudget;
};

class E2eTestScenario {
 protected:
  std::string description;
  void checkExpectedBalances(Wallet::Channel &chanWallet,
                             std::unique_ptr<Wallet> &wallet,
                             const E2eTestExpectedUserBalances expectedBalances,
                             E2eTestResult &result);

 public:
  E2eTestScenario(std::string description) : description(description) {}

  virtual int execute(E2eTestContext &context) = 0;

  virtual ~E2eTestScenario() {}

  std::string &getDescription() { return description; }
};