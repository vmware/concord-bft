#include "testUtils.hpp"

#include "UTTParams.hpp"
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
  auto [d, dkg, rc] = testing::init(n, thresh);
  auto registrators = testing::GenerateRegistrators(n, rc);
  auto banks = testing::GenerateCommitters(n, dkg, rc.toPK());
  auto clients = testing::GenerateClients(c, dkg.getPK(), rc.toPK(), rc);
  for (auto& c : clients) {
    testing::registerClient(d, c, registrators, thresh);
  }

  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    auto budget = Budget(d, c, 1000, 123456789);
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(budget).front());
    }
    auto sbs = testing::getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(budget, d, {blinded_sig}).front();
    assertTrue(c.validate(coin));
  }

  // Now, do the same for a budget created by the replicas.
  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    auto budget = Budget(d, c.getPidHash(), 1000, 123456789);
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(budget).front());
    }
    auto sbs = testing::getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(budget, d, {blinded_sig}).front();
    coin.createNullifier(d, c.getPRFSecretKey());
    assertTrue(!coin.getNullifier().empty());
    assertTrue(c.validate(coin));
  }
  return 0;
}