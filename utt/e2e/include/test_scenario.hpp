#pragma once

#include <string>

struct E2eTestContext {
  std::unique_ptr<Wallet> wallet1, wallet2, wallet3;
  Admin::Channel chanAdmin;
  Wallet::Channel chanWallet;
};

class E2eTestScenario {
 protected:
  E2eTestContext &context;
  std::string description;

 public:
  E2eTestScenario(E2eTestContext &context, std::string description) : context(context), description(description) {}

  virtual int execute() = 0;

  virtual ~E2eTestScenario() {}

  std::string getDescription() { return description; }
};