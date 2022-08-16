#include "testUtils.hpp"

#include "globalParams.hpp"
#include "testUtils.hpp"
#include "budget.hpp"
#include <utt/MintOp.h>
#include <utt/Coin.h>
#include <utt/BurnOp.h>
#include <memory>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <chrono>

using namespace libutt;
using namespace libutt::api;
using namespace libutt::api::operations;
using namespace std::chrono;
int main(int argc, char* argv[]) {
  std::srand(0);
  (void)argc;
  (void)argv;
  size_t thresh = 3;
  size_t n = 4;
  size_t c = 10;
  auto [dkg, rc] = testing::init(n, thresh);
  auto& d = GlobalParams::instance();
  auto registrators = testing::GenerateRegistrators(n, rc);
  auto banks = testing::GenerateCommitters(n, dkg, rc.toPK());
  auto clients = testing::GenerateClients(c, dkg.getPK(), rc.toPK(), rc);
  for (auto& c : clients) {
    testing::registerClient(d, c, registrators, thresh);
  }

  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    uint64_t now = (uint64_t)(duration_cast<hours>(system_clock::now().time_since_epoch()).count());
    auto budget = Budget(d, c, 1000, now + 100U);
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(budget).front());
    }
    auto sbs = testing::getSubGroup((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto coin = c.claimCoins(budget, d, (uint32_t)n, {sigs}).front();
    assertTrue(c.validate(coin));
  }

  // Now, do the same for a budget created by the replicas.
  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    uint64_t now = (uint64_t)(duration_cast<hours>(system_clock::now().time_since_epoch()).count());
    auto budget = Budget(d, c.getPidHash(), 1000, now + 100U);
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(budget).front());
    }
    auto sbs = testing::getSubGroup((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto coin = c.claimCoins(budget, d, (uint32_t)n, {sigs}).front();
    coin.createNullifier(d, c.getPRFSecretKey());
    assertTrue(!coin.getNullifier().empty());
    assertTrue(c.validate(coin));
  }
  return 0;
}