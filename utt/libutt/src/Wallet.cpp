#include <utt/Configuration.h>

#include <utt/Coin.h>
#include <utt/RegAuth.h>
#include <utt/Tx.h>
#include <utt/Wallet.h>

#include <utt/Serialization.h>  // WARNING: Must include last

std::ostream& operator<<(std::ostream& out, const libutt::Wallet& w) {
  out << w.p;
  out << w.ask;
  out << w.rpk;
  out << w.bpk;
  libutt::serializeVector(out, w.coins);
  out << w.budgetCoin;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::Wallet& w) {
  in >> w.p;
  in >> w.ask;
  in >> w.rpk;
  in >> w.bpk;
  libutt::deserializeVector(in, w.coins);
  in >> w.budgetCoin;
  return in;
}

namespace libutt {

void Wallet::__print() const {
  loginfo << "pid:         " << getUserPid() << endl;
  loginfo << "# coins:     " << numCoins() << endl;
  loginfo << "total:       " << totalValue() << endl;
  loginfo << "budget coin? " << hasBudgetCoin() << endl;
}

void Wallet::addNormalCoin(const Coin& c) {
  if (c.pid_hash != ask.getPidHash()) {
    throw std::runtime_error("You are adding another person's coin to your wallet");
  }

  // logdbg << "Adding coin..." << endl;
  assertTrue(c.hasValidSig(bpk));

  testAssertTrue(c.isNormal());

  coins.push_back(c);
}

void Wallet::setBudgetCoin(const Coin& c) {
  if (c.pid_hash != ask.getPidHash()) {
    throw std::runtime_error("You are adding another person's coin to your wallet");
  }

  assertTrue(c.hasValidSig(bpk));

  testAssertTrue(!budgetCoin.has_value());
  testAssertTrue(!c.isNormal());

  budgetCoin = c;
}

Tx Wallet::spendTwoRandomCoins(const std::string& pid, bool removeFromWallet) {
  testAssertGreaterThanOrEqual(coins.size(), 2);

  // Input coins
  std::vector<Coin> c;

  if (removeFromWallet) {
    c.push_back(coins.back());  // remove one coin from the wallet
    coins.pop_back();
    c.push_back(coins.back());  // remove one coin from the wallet
    coins.pop_back();
  } else {
    c.push_back(coins.at(coins.size() - 1));
    c.push_back(coins.at(coins.size() - 2));
  }

  // the recipients and their amounts received
  std::vector<std::tuple<std::string, Fr>> recip;

  // split these two input coins randomly amongst sender and recipient
  Fr totalVal = c.at(0).val + c.at(1).val;
  int totalValInt = static_cast<int>(totalVal.as_ulong());
  Fr val1;
  val1.set_ulong(static_cast<unsigned long>(rand() % (totalValInt - 1) + 1));  // i.e., random in [1, totalVal)
  Fr val2 = totalVal - val1;
  testAssertEqual(val1 + val2, totalVal);

  logdbg << "'" << ask.pid << "' sends $" << val1 << " to '" << pid << "'" << endl;
  logdbg << "'" << ask.pid << "' gets $" << val2 << " change" << endl;

  recip.push_back(std::make_tuple(pid, val1));      // send some money to someone else
  recip.push_back(std::make_tuple(ask.pid, val2));  // give yourself some change

  // ...and Tx::Tx() will take care of the budget change

  testAssertTrue(hasBudgetCoin());
  Coin b = *budgetCoin;
  if (removeFromWallet) {
    budgetCoin.reset();  // remove from wallet; caller is responsible for adding budget change back
    testAssertFalse(hasBudgetCoin());
  }

  return Tx(p, ask, c, b, recip, bpk, rpk);
}
}  // namespace libutt
