#include "UTTParams.hpp"
#include "testUtils.hpp"
#include "mint.hpp"
#include "budget.hpp"
#include "burn.hpp"
#include "commitment.hpp"
#include "transaction.hpp"
#include "config.hpp"
#include "serialization.hpp"

#include <utt/DataUtils.hpp>
#include <memory>
#include <vector>
#include <cstdlib>
#include <ctime>

using namespace libutt;
using namespace libutt::api;
using namespace libutt::api::operations;

namespace {
class ibe_based_test_system : public libutt::api::testing::test_utt_instance {
 protected:
  void SetUp() override { libutt::api::testing::test_utt_instance::setUp(true, true); }
};

class ibe_based_test_system_without_registration : public libutt::api::testing::test_utt_instance {
 protected:
  void SetUp() override { libutt::api::testing::test_utt_instance::setUp(true, false); }
};

class rsa_based_test_system : public libutt::api::testing::test_utt_instance {
 protected:
  void SetUp() override { libutt::api::testing::test_utt_instance::setUp(false, true); }
};

class rsa_based_test_system_without_registration : public libutt::api::testing::test_utt_instance {
 protected:
  void SetUp() override { libutt::api::testing::test_utt_instance::setUp(false, false); }
};

class test_system_minted : public libutt::api::testing::test_utt_instance {
 public:
  void setUp(bool ibe) {
    libutt::api::testing::test_utt_instance::setUp(ibe, true);
    for (auto& c : clients) {
      std::vector<std::vector<uint8_t>> rsigs;
      std::string simulatonOfUniqueTxHash =
          std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
      auto mint = Mint(simulatonOfUniqueTxHash, 100, c.getPid());
      for (size_t i = 0; i < banks.size(); i++) {
        rsigs.push_back(banks[i]->sign(mint).front());
      }
      auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
      std::map<uint32_t, std::vector<uint8_t>> sigs;
      for (auto i : sbs) {
        sigs[i] = rsigs[i];
      }
      auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
      auto coin = c.claimCoins(mint, d, {blinded_sig}).front();
      ASSERT_TRUE(c.validate(coin));
      coins[c.getPid()] = {coin};
    }

    for (auto& c : clients) {
      std::vector<std::vector<uint8_t>> rsigs;
      std::vector<uint16_t> shares_signers;
      auto budget = Budget(d, c, 1000, 123456789);
      for (size_t i = 0; i < banks.size(); i++) {
        rsigs.push_back(banks[i]->sign(budget).front());
        shares_signers.push_back(banks[i]->getId());
      }
      for (auto& b : banks) {
        for (size_t i = 0; i < rsigs.size(); i++) {
          auto& sig = rsigs[i];
          auto& signer = shares_signers[i];
          ASSERT_TRUE(b->validatePartialSignature<Budget>(signer, sig, 0, budget));
        }
      }
      auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
      std::map<uint32_t, std::vector<uint8_t>> sigs;
      for (auto i : sbs) {
        sigs[i] = rsigs[i];
      }
      auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
      auto coin = c.claimCoins(budget, d, {blinded_sig}).front();
      ASSERT_TRUE(c.validate(coin));
      bcoins[c.getPid()] = {coin};
    }
  }
  std::map<std::string, std::vector<libutt::api::Coin>> coins;
  std::map<std::string, std::vector<libutt::api::Coin>> bcoins;
};

class ibe_based_test_system_minted : public test_system_minted {
 protected:
  void SetUp() override { test_system_minted::setUp(true); }
};

class ibe_based_test_system_minted_impl : public ibe_based_test_system_minted {
 public:
  ibe_based_test_system_minted_impl() { ibe_based_test_system_minted::SetUp(); }
  void TestBody() override {}
};

class rsa_based_test_system_minted : public test_system_minted {
 protected:
  void SetUp() override { test_system_minted::setUp(false); }
};

TEST_F(ibe_based_test_system_without_registration, test_distributed_registration) {
  for (auto& c : clients) {
    registerClient(c);
    auto rcm_data = c.rerandomizeRcm(d);
    for (auto& r : registrators) {
      ASSERT_TRUE(r->validateRCM(rcm_data.first, rcm_data.second));
    }
  }
}

TEST_F(rsa_based_test_system_without_registration, test_distributed_registration) {
  for (auto& c : clients) {
    registerClient(c);
    auto rcm_data = c.rerandomizeRcm(d);
    for (auto& r : registrators) {
      ASSERT_TRUE(r->validateRCM(rcm_data.first, rcm_data.second));
    }
  }
}

TEST_F(ibe_based_test_system, test_mint) {
  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    std::vector<uint16_t> shares_signers;
    std::string simulatonOfUniqueTxHash = std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
    auto mint = Mint(simulatonOfUniqueTxHash, 100, c.getPid());
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(mint).front());
      shares_signers.push_back(banks[i]->getId());
    }
    for (auto& b : banks) {
      for (size_t i = 0; i < rsigs.size(); i++) {
        auto& sig = rsigs[i];
        auto& signer = shares_signers[i];
        ASSERT_TRUE(b->validatePartialSignature<Mint>(signer, sig, 0, mint));
      }
    }
    auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(mint, d, {blinded_sig}).front();
    ASSERT_TRUE(c.validate(coin));
  }
}

TEST_F(ibe_based_test_system, test_budget_creation_by_client) {
  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    std::vector<uint16_t> shares_signers;
    auto budget = Budget(d, c, 1000, 123456789);
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(budget).front());
      shares_signers.push_back(banks[i]->getId());
    }
    for (auto& b : banks) {
      for (size_t i = 0; i < rsigs.size(); i++) {
        auto& sig = rsigs[i];
        auto& signer = shares_signers[i];
        ASSERT_TRUE(b->validatePartialSignature<Budget>(signer, sig, 0, budget));
      }
    }
    auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(budget, d, {blinded_sig}).front();
    ASSERT_TRUE(c.validate(coin));
  }
}

TEST_F(ibe_based_test_system, test_budget_creation_by_replicas) {
  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    auto snHash = Fr::random_element()
                      .to_words();  // Assume each replica can compute the same sn using some common execution state
    for (size_t i = 0; i < banks.size(); i++) {
      // Each replica creates its own version of the budget coin and sign it. We expect to have deterministic coin here
      auto budget = Budget(d, snHash, c.getPidHash(), 1000, 123456789, false);
      rsigs.push_back(banks[i]->sign(budget).front());
    }
    auto budget = Budget(d, snHash, c.getPidHash(), 1000, 123456789, false);
    auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(budget, d, {blinded_sig}).front();
    coin.createNullifier(d, c.getPRFSecretKey());
    ASSERT_TRUE(!coin.getNullifier().empty());
    ASSERT_TRUE(c.validate(coin));
  }
}

TEST_F(ibe_based_test_system_minted, test_valid_burn) {
  for (auto& c : clients) {
    Burn b_op{d, c, coins[c.getPid()].front()};
    for (auto& b : banks) {
      ASSERT_TRUE(b->validate(d, b_op));
    }
  }
}

TEST_F(ibe_based_test_system_minted, test_invalid_burn) {
  auto other_utt_sys = ibe_based_test_system_minted_impl();
  for (auto& c : clients) {
    Burn b_op{d, c, coins[c.getPid()].front()};
    for (auto& b : other_utt_sys.banks) {
      ASSERT_FALSE(b->validate(other_utt_sys.d, b_op));
    }
  }
}

TEST_F(ibe_based_test_system, test_serialization_configuration) {
  auto config = libutt::api::Configuration((uint16_t)n, (uint16_t)thresh);
  ASSERT_TRUE(config.isValid());
  auto serialized_config = libutt::api::serialize<libutt::api::Configuration>(config);
  auto deserialized_config = libutt::api::deserialize<libutt::api::Configuration>(serialized_config);
  ASSERT_TRUE(deserialized_config.isValid());
  ASSERT_TRUE(deserialized_config == config);

  const auto& publicConfig = config.getPublicConfig();
  auto serialized_public_config = libutt::api::serialize<libutt::api::PublicConfig>(publicConfig);
  auto deserialized_public_config = libutt::api::deserialize<libutt::api::PublicConfig>(serialized_public_config);
  ASSERT_TRUE(deserialized_public_config == publicConfig);
}

TEST_F(ibe_based_test_system, test_serialization_global_params) {
  std::vector<uint8_t> serialized_params = libutt::api::serialize<libutt::api::UTTParams>(d);
  auto deserialized_params = libutt::api::deserialize<libutt::api::UTTParams>(serialized_params);
  ASSERT_TRUE(deserialized_params == d);
}

TEST_F(ibe_based_test_system, test_serialization_commitment) {
  Commitment rcm = clients[0].getRcm().first;
  std::vector<uint8_t> serialized_rcm = libutt::api::serialize<libutt::api::Commitment>(rcm);
  auto deserialized_rcm = libutt::api::deserialize<libutt::api::Commitment>(serialized_rcm);
  ASSERT_TRUE(rcm == deserialized_rcm);
}

TEST_F(ibe_based_test_system, test_serialization_complete_coin) {
  Fr fr_val;
  fr_val.set_ulong(100);
  libutt::api::Coin c(d,
                      Fr::random_element().to_words(),
                      Fr::random_element().to_words(),
                      fr_val.to_words(),
                      Fr::random_element().to_words(),
                      libutt::api::Coin::Type::Normal,
                      Fr::random_element().to_words());
  std::vector<uint8_t> c_serialized = libutt::api::serialize<libutt::api::Coin>(c);
  libutt::api::Coin c_deserialized = libutt::api::deserialize<libutt::api::Coin>(c_serialized);
  assertTrue(c.getNullifier() == c_deserialized.getNullifier());
  assertTrue(c.hasSig() == c_deserialized.hasSig());
  assertTrue(c.getType() == c_deserialized.getType());
  assertTrue(c.getVal() == c_deserialized.getVal());
  assertTrue(c.getSN() == c_deserialized.getSN());
  assertTrue(c.getExpDateAsCurvePoint() == c_deserialized.getExpDateAsCurvePoint());
}

TEST_F(ibe_based_test_system, test_serialization_incomplete_coin) {
  Fr fr_val;
  fr_val.set_ulong(100);
  libutt::api::Coin c(d,
                      Fr::random_element().to_words(),
                      fr_val.to_words(),
                      Fr::random_element().to_words(),
                      libutt::api::Coin::Type::Normal,
                      Fr::random_element().to_words());
  std::vector<uint8_t> c_serialized = libutt::api::serialize<libutt::api::Coin>(c);
  libutt::api::Coin c_deserialized = libutt::api::deserialize<libutt::api::Coin>(c_serialized);
  assertTrue(c.getNullifier() == c_deserialized.getNullifier());
  assertTrue(c.hasSig() == c_deserialized.hasSig());
  assertTrue(c.getType() == c_deserialized.getType());
  assertTrue(c.getVal() == c_deserialized.getVal());
  assertTrue(c.getSN() == c_deserialized.getSN());
  assertTrue(c.getExpDateAsCurvePoint() == c_deserialized.getExpDateAsCurvePoint());
}
TEST_F(ibe_based_test_system, test_serialization_complete_budget_coin) {
  Fr fr_val;
  fr_val.set_ulong(100);
  libutt::api::Coin c(d,
                      Fr::random_element().to_words(),
                      fr_val.to_words(),
                      Fr::random_element().to_words(),
                      libutt::api::Coin::Type::Budget,
                      Fr::random_element().to_words());
  std::vector<uint8_t> c_serialized = libutt::api::serialize<libutt::api::Coin>(c);
  libutt::api::Coin c_deserialized = libutt::api::deserialize<libutt::api::Coin>(c_serialized);
  assertTrue(c.getNullifier() == c_deserialized.getNullifier());
  assertTrue(c.hasSig() == c_deserialized.hasSig());
  assertTrue(c.getType() == c_deserialized.getType());
  assertTrue(c.getVal() == c_deserialized.getVal());
  assertTrue(c.getSN() == c_deserialized.getSN());
  assertTrue(c.getExpDateAsCurvePoint() == c_deserialized.getExpDateAsCurvePoint());
}

TEST_F(ibe_based_test_system, test_serialization_incomplete_budget_coin) {
  Fr fr_val;
  fr_val.set_ulong(100);
  libutt::api::Coin c(d,
                      Fr::random_element().to_words(),
                      Fr::random_element().to_words(),
                      fr_val.to_words(),
                      Fr::random_element().to_words(),
                      libutt::api::Coin::Type::Budget,
                      Fr::random_element().to_words());
  std::vector<uint8_t> c_serialized = libutt::api::serialize<libutt::api::Coin>(c);
  libutt::api::Coin c_deserialized = libutt::api::deserialize<libutt::api::Coin>(c_serialized);
  assertTrue(c.getNullifier() == c_deserialized.getNullifier());
  assertTrue(c.hasSig() == c_deserialized.hasSig());
  assertTrue(c.getType() == c_deserialized.getType());
  assertTrue(c.getVal() == c_deserialized.getVal());
  assertTrue(c.getSN() == c_deserialized.getSN());
  assertTrue(c.getExpDateAsCurvePoint() == c_deserialized.getExpDateAsCurvePoint());
}

TEST_F(ibe_based_test_system, test_serialization_incomplete_budget_op) {
  libutt::api::operations::Budget b(
      d, Fr::random_element().to_words(), Fr::random_element().to_words(), 100, 987654321);
  std::vector<uint8_t> b_serialized = libutt::api::serialize<libutt::api::operations::Budget>(b);
  libutt::api::operations::Budget b_deserialized =
      libutt::api::deserialize<libutt::api::operations::Budget>(b_serialized);
  assertTrue(b.getHashHex() == b_deserialized.getHashHex());
  const auto b_coin = b.getCoin();
  const auto c_deserialized_coin = b_deserialized.getCoin();
  assertTrue(b_coin.getNullifier() == c_deserialized_coin.getNullifier());
  assertTrue(b_coin.hasSig() == c_deserialized_coin.hasSig());
  assertTrue(b_coin.getType() == c_deserialized_coin.getType());
  assertTrue(b_coin.getVal() == c_deserialized_coin.getVal());
  assertTrue(b_coin.getSN() == c_deserialized_coin.getSN());
  assertTrue(b_coin.getExpDateAsCurvePoint() == c_deserialized_coin.getExpDateAsCurvePoint());
}

TEST_F(ibe_based_test_system, test_serialization_mint_op) {
  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    std::string simulatonOfUniqueTxHash = std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
    auto mint = Mint(simulatonOfUniqueTxHash, 100, c.getPid());
    std::vector<uint8_t> mint_serialized = libutt::api::serialize<libutt::api::operations::Mint>(mint);
    auto mint_deserialized = libutt::api::deserialize<libutt::api::operations::Mint>(mint_serialized);
    assertTrue(mint.getHash() == mint_deserialized.getHash());
    assertTrue(mint.getVal() == mint_deserialized.getVal());
    assertTrue(mint.getRecipentID() == mint_deserialized.getRecipentID());
  }
}

TEST_F(ibe_based_test_system_minted, test_serialization_burn_op) {
  for (auto& c : clients) {
    Burn b_op{d, c, coins[c.getPid()].front()};
    std::vector<uint8_t> burn_serialized = libutt::api::serialize<libutt::api::operations::Burn>(b_op);
    auto burn_deserialized = libutt::api::deserialize<libutt::api::operations::Burn>(burn_serialized);
    assertTrue(b_op.getNullifier() == burn_deserialized.getNullifier());
    const auto& burn_coin = b_op.getCoin();
    const auto& burn_deserialized_coin = burn_deserialized.getCoin();
    assertTrue(burn_coin.getNullifier() == burn_deserialized_coin.getNullifier());
    assertTrue(burn_coin.hasSig() == burn_deserialized_coin.hasSig());
    assertTrue(burn_coin.getType() == burn_deserialized_coin.getType());
    assertTrue(burn_coin.getVal() == burn_deserialized_coin.getVal());
    assertTrue(burn_coin.getSN() == burn_deserialized_coin.getSN());
    assertTrue(burn_coin.getExpDateAsCurvePoint() == burn_deserialized_coin.getExpDateAsCurvePoint());
  }
}

TEST_F(ibe_based_test_system_minted, test_serialization_tx_op) {
  const auto& client1 = clients[0];
  const auto& client2 = clients[1];
  auto coin = coins[client1.getPid()].front();
  auto bcoin = bcoins[client1.getPid()].front();
  libutt::IBE::MSK msk = libutt::deserialize<libutt::IBE::MSK>(config->getIbeMsk());
  auto mpk = msk.toMPK(config->getPublicConfig().getParams().getImpl()->p.ibe);
  auto encryptor = std::make_shared<libutt::IBEEncryptor>(mpk);
  Transaction tx(d, client1, {coin}, {bcoin}, {{client1.getPid(), 50}, {client2.getPid(), 50}}, *(encryptor));

  // Test Transaction de/serialization
  std::vector<uint8_t> serialized_tx = libutt::api::serialize<libutt::api::operations::Transaction>(tx);
  auto deserialized_tx = libutt::api::deserialize<libutt::api::operations::Transaction>(serialized_tx);
  assertTrue(tx.hasBudgetCoin() == deserialized_tx.hasBudgetCoin());
  assertTrue(tx.getBudgetExpirationDate() == deserialized_tx.getBudgetExpirationDate());
  assertTrue(tx.getNullifiers() == deserialized_tx.getNullifiers());
}

TEST_F(ibe_based_test_system_minted, test_transaction) {
  std::unordered_map<size_t, std::shared_ptr<libutt::IBEEncryptor>> encryptors_;
  libutt::IBE::MSK msk = libutt::deserialize<libutt::IBE::MSK>(config->getIbeMsk());
  auto mpk = msk.toMPK(config->getPublicConfig().getParams().getImpl()->p.ibe);
  for (size_t i = 0; i < clients.size(); i++) {
    encryptors_[i] = std::make_shared<libutt::IBEEncryptor>(mpk);
  }
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    Transaction tx(d,
                   issuer,
                   {coins[issuer.getPid()].front()},
                   {bcoins[issuer.getPid()].front()},
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
      auto sbs = getSubset((uint32_t)n, (uint32_t)n);
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
      auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
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
        bcoins[issuer.getPid()] = {coin};
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
    assertTrue(bcoins[c.getPid()].front().getVal() == 950U);
  }
}

TEST_F(rsa_based_test_system_minted, test_transaction) {
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    std::map<std::string, std::string> tx_pub_keys;
    tx_pub_keys[issuer.getPid()] = libutt::api::testing::pkeys[i];
    tx_pub_keys[receiver.getPid()] = libutt::api::testing::pkeys[(i + 1) % clients.size()];
    libutt::RSAEncryptor tx_encryptor(tx_pub_keys);
    Transaction tx(d,
                   issuer,
                   {coins[issuer.getPid()].front()},
                   {bcoins[issuer.getPid()].front()},
                   {{issuer.getPid(), 50}, {receiver.getPid(), 50}},
                   tx_encryptor);
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
      auto sbs = getSubset((uint32_t)n, (uint32_t)n);
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
      auto sbs = getSubset((uint32_t)n, (uint32_t)thresh);
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
        bcoins[issuer.getPid()] = {coin};
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
    assertTrue(bcoins[c.getPid()].front().getVal() == 950U);
  }
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}