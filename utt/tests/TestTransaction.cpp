#include "testUtils.hpp"

#include "UTTParams.hpp"
#include "testUtils.hpp"
#include "mint.hpp"
#include "burn.hpp"
#include "transaction.hpp"
#include "budget.hpp"
#include "coin.hpp"
#include "types.hpp"

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
  auto [d, dkg, rc] = testing::init(n, thresh);
  auto registrators = testing::GenerateRegistrators(n, rc);
  auto banks = testing::GenerateCommitters(n, dkg, rc.toPK());
  auto clients = testing::GenerateClients(c, dkg.getPK(), rc.toPK(), rc);
  for (auto& c : clients) {
    testing::registerClient(d, c, registrators, thresh);
  }
  std::unordered_map<std::string, std::vector<libutt::api::Coin>> coins;
  std::unordered_map<std::string, libutt::api::Coin> bcoins;
  std::unordered_map<size_t, std::shared_ptr<libutt::IBEEncryptor>> encryptors_;
  for (size_t i = 0; i < clients.size(); i++) {
    encryptors_[i] = std::make_shared<libutt::IBEEncryptor>(rc.toPK().mpk);
  }
  for (auto& c : clients) {
    std::vector<types::Signature> rsigs;
    auto budget = Budget(d, c, 1000, 123456789);
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(budget).front());
    }
    auto sbs = testing::getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, types::Signature> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(budget, d, {blinded_sig}).front();
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
    auto sbs = testing::getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, types::Signature> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(mint, d, {blinded_sig}).front();
    assertTrue(c.validate(coin));
    coins[c.getPid()].emplace_back(std::move(coin));
  }

  // Now, each client transfers a 50$ to its predecessor;
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    Transaction tx(d,
                   issuer,
                   {coins[issuer.getPid()].front()},
                   {bcoins[issuer.getPid()]},
                   {{issuer.getPid(), 50}, {receiver.getPid(), 50}},
                   *(encryptors_.at((i + 1) % clients.size())));
    coins[issuer.getPid()].erase(coins[issuer.getPid()].begin());
    bcoins.erase(issuer.getPid());
    std::map<size_t, std::vector<types::Signature>> shares;
    std::vector<uint16_t> shares_signers;
    for (size_t i = 0; i < banks.size(); i++) {
      shares[i] = banks[i]->sign(tx);
      shares_signers.push_back(banks[i]->getId());
    }
    size_t num_coins = shares[0].size();
    for (size_t i = 0; i < num_coins; i++) {
      std::vector<types::Signature> share_sigs(n);
      auto sbs = testing::getSubset((uint32_t)n, (uint32_t)n);
      for (auto s : sbs) {
        share_sigs[s] = shares[s][i];
      }
      for (auto& b : banks) {
        for (size_t k = 0; k < share_sigs.size(); k++) {
          auto& sig = share_sigs[k];
          auto& signer = shares_signers[k];
          assertTrue(b->validatePartialSignature(signer, sig, i, tx));
        }
      }
    }

    std::vector<types::Signature> sigs;
    for (size_t i = 0; i < num_coins; i++) {
      std::map<uint32_t, types::Signature> share_sigs;
      auto sbs = testing::getSubset((uint32_t)n, (uint32_t)thresh);
      for (auto s : sbs) {
        share_sigs[s] = shares[s][i];
      }
      auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {share_sigs});
      sigs.emplace_back(std::move(blinded_sig));
    }
    auto issuer_coins = issuer.claimCoins(tx, d, sigs);
    for (auto& coin : issuer_coins) {
      assertTrue(issuer.validate(coin));
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[issuer.getPid()].emplace_back(std::move(coin));
      } else {
        assertTrue(bcoins.find(issuer.getPid()) == bcoins.end());
        bcoins.emplace(issuer.getPid(), coin);
      }
    }
    auto receiver_coins = receiver.claimCoins(tx, d, sigs);
    for (auto& coin : receiver_coins) {
      assertTrue(receiver.validate(coin));
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