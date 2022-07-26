#include "testUtils.hpp"

#include "details.hpp"
#include "testUtils.hpp"
#include "mint.hpp"
#include "burn.hpp"
#include "transaction.hpp"
#include "budget.hpp"
#include "coin.hpp"
#include "types.hpp"
#include <utt/MintOp.h>
#include <utt/Coin.h>
#include <utt/BurnOp.h>
#include <utt/Tx.h>

#include <memory>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <unordered_map>

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
  auto& d = Details::instance();
  auto registrators = testing::GenerateRegistrators(n, rc);
  auto banks = testing::GenerateCommitters(n, dkg, rc.toPK());
  auto clients = testing::GenerateClients(c, dkg.getPK(), rc.toPK(), rc);
  for (auto& c : clients) {
    testing::registerClient(d, c, registrators, thresh);
  }
  std::unordered_map<std::string, std::vector<libutt::api::Coin>> coins;
  std::unordered_map<std::string, libutt::api::Coin> bcoins;
  for (auto& c : clients) {
    std::vector<types::Signature> rsigs;
    uint64_t now = (uint64_t)(duration_cast<hours>(system_clock::now().time_since_epoch()).count());
    auto budget = Budget(d, c, 1000, now + 100U);
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(budget).front());
    }
    auto sbs = testing::getSubGroup((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, types::Signature> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto coin = c.claimCoins(budget, d, (uint32_t)n, {sigs}).front();
    assertTrue(c.validate(coin));
    bcoins.emplace(c.getPid(), coin);
  }

  for (auto& c : clients) {
    std::vector<types::Signature> rsigs;
    std::string simulatonOfUniqueTxHash = std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
    auto mint = Mint(simulatonOfUniqueTxHash, 100, c.getPid());
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(mint).front());
    }
    auto sbs = testing::getSubGroup((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, types::Signature> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto coin = c.claimCoins(mint, d, (uint32_t)n, {sigs}).front();
    assertTrue(c.validate(coin));
    coins[c.getPid()].emplace_back(std::move(coin));
  }

  // Now, each client transfers a 50$ to its predecessor;
  for (size_t i = 0 ; i < clients.size() ; i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    Transaction tx(d, issuer, {coins[issuer.getPid()].front()}, {bcoins[issuer.getPid()]}, {{issuer.getPid(), 50}, {receiver.getPid(), 50}});
    coins[issuer.getPid()].erase(coins[issuer.getPid()].begin());
    bcoins.erase(issuer.getPid());
    std::unordered_map<size_t, std::vector<types::Signature>> shares;
    for (size_t i = 0; i < banks.size(); i++) {
      shares[i] = banks[i]->sign(tx);
    }
    size_t num_coins = shares[0].size();
    std::vector<std::map<uint32_t, types::Signature>> sigs;
    for (size_t i = 0 ; i < num_coins ; i++) {
        std::map<uint32_t, types::Signature> share_sigs;
        auto sbs = testing::getSubGroup((uint32_t)n, (uint32_t)thresh);
        for (auto s : sbs) {
          share_sigs[s] = shares[s][i];
        }
        sigs.emplace_back(std::move(share_sigs));
    }
    auto issuer_coins = issuer.claimCoins(tx, d, (uint32_t) n, sigs);
    for (auto& coin : issuer_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[issuer.getPid()].emplace_back(std::move(coin));
      } else {
        assertTrue(bcoins.find(issuer.getPid()) == bcoins.end());
        bcoins.emplace(issuer.getPid(), coin);
      }
    }
    auto receiver_coins = receiver.claimCoins(tx, d, (uint32_t) n, sigs);
    for (auto& coin : receiver_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[receiver.getPid()].emplace_back(std::move(coin));
      } else {
        assertTrue(false);
      }
    }
  }

  // At the end of these phaese, the budget of each client is 950 and it has two coins of 50 each
  for (const auto& c : clients) {
    assertTrue(coins[c.getPid()].size() == 2);
    for (const auto& coin : coins[c.getPid()]) assertTrue(coin.getVal() == 50U);
    assertTrue(bcoins[c.getPid()].getVal() == 950U);
  }
  return 0;
}