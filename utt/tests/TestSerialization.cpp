#include "testUtils.hpp"
#include "coin.hpp"
#include "budget.hpp"
#include "burn.hpp"
#include "mint.hpp"
#include "commitment.hpp"
#include "transaction.hpp"
#include "config.hpp"
#include <utt/DataUtils.hpp>
#include "serialization.hpp"
using namespace libutt;
using namespace libutt::api;
using namespace libutt::api::operations;

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  size_t thresh = 3;
  size_t n = 4;
  size_t c = 2;
  auto [d, dkg, rc] = testing::init(n, thresh);
  auto registrators = testing::GenerateRegistrators(n, rc);
  auto banks = testing::GenerateCommitters(n, dkg, rc.toPK());
  auto clients = testing::GenerateClients(c, dkg.getPK(), rc.toPK(), rc);
  for (auto& c : clients) {
    testing::registerClient(d, c, registrators, thresh);
  }

  // Test UTTParams de/serialization
  std::vector<uint8_t> serialized_params = libutt::api::serialize<libutt::api::UTTParams>(d);
  auto deserialized_params = libutt::api::deserialize<libutt::api::UTTParams>(serialized_params);
  assertTrue(deserialized_params == d);

  // Test Configuration de/serialization
  {
    auto config = libutt::api::Configuration((uint16_t)n, (uint16_t)thresh);
    assertTrue(config.isValid());
    auto serialized_config = libutt::api::serialize<libutt::api::Configuration>(config);
    auto deserialized_config = libutt::api::deserialize<libutt::api::Configuration>(serialized_config);
    assertTrue(deserialized_config.isValid());
    assertTrue(deserialized_config == config);

    const auto& publicConfig = config.getPublicConfig();
    auto serialized_public_config = libutt::api::serialize<libutt::api::PublicConfig>(publicConfig);
    auto deserialized_public_config = libutt::api::deserialize<libutt::api::PublicConfig>(serialized_public_config);
    assertTrue(deserialized_public_config == publicConfig);
  }

  Commitment rcm = clients[0].getRcm().first;

  // Test Commitment de/serialization
  std::vector<uint8_t> serialized_rcm = libutt::api::serialize<libutt::api::Commitment>(rcm);
  auto deserialized_rcm = libutt::api::deserialize<libutt::api::Commitment>(serialized_rcm);
  assertTrue(rcm == deserialized_rcm);
  Fr fr_val;
  fr_val.set_ulong(100);

  /*
    We define a *complete coin* as a UTT coin that contains a nullifier.
    We define an *incomplete coin* as a UTT coin that does not contain a nullifier.
  */
  // Test a complete normal coin de/serialization
  libutt::api::Coin c1(d,
                       Fr::random_element().to_words(),
                       Fr::random_element().to_words(),
                       fr_val.to_words(),
                       Fr::random_element().to_words(),
                       libutt::api::Coin::Type::Normal,
                       Fr::random_element().to_words());
  std::vector<uint8_t> c1_serialized = libutt::api::serialize<libutt::api::Coin>(c1);
  libutt::api::Coin c1_deserialized = libutt::api::deserialize<libutt::api::Coin>(c1_serialized);
  assertTrue(c1.getNullifier() == c1_deserialized.getNullifier());
  assertTrue(c1.hasSig() == c1_deserialized.hasSig());
  assertTrue(c1.getType() == c1_deserialized.getType());
  assertTrue(c1.getVal() == c1_deserialized.getVal());
  assertTrue(c1.getSN() == c1_deserialized.getSN());
  assertTrue(c1.getExpDateAsCurvePoint() == c1_deserialized.getExpDateAsCurvePoint());

  // Test an incomplete normal coin de/serialization
  libutt::api::Coin c2(d,
                       Fr::random_element().to_words(),
                       fr_val.to_words(),
                       Fr::random_element().to_words(),
                       libutt::api::Coin::Type::Normal,
                       Fr::random_element().to_words());
  std::vector<uint8_t> c2_serialized = libutt::api::serialize<libutt::api::Coin>(c2);
  libutt::api::Coin c2_deserialized = libutt::api::deserialize<libutt::api::Coin>(c2_serialized);
  assertTrue(c2.getNullifier() == c2_deserialized.getNullifier());
  assertTrue(c2.hasSig() == c2_deserialized.hasSig());
  assertTrue(c2.getType() == c2_deserialized.getType());
  assertTrue(c2.getVal() == c2_deserialized.getVal());
  assertTrue(c2.getSN() == c2_deserialized.getSN());
  assertTrue(c2.getExpDateAsCurvePoint() == c2_deserialized.getExpDateAsCurvePoint());

  // Test a complete budget coin de/serialization
  libutt::api::Coin c3(d,
                       Fr::random_element().to_words(),
                       fr_val.to_words(),
                       Fr::random_element().to_words(),
                       libutt::api::Coin::Type::Budget,
                       Fr::random_element().to_words());
  std::vector<uint8_t> c3_serialized = libutt::api::serialize<libutt::api::Coin>(c3);
  libutt::api::Coin c3_deserialized = libutt::api::deserialize<libutt::api::Coin>(c3_serialized);
  assertTrue(c3.getNullifier() == c3_deserialized.getNullifier());
  assertTrue(c3.hasSig() == c3_deserialized.hasSig());
  assertTrue(c3.getType() == c3_deserialized.getType());
  assertTrue(c3.getVal() == c3_deserialized.getVal());
  assertTrue(c3.getSN() == c3_deserialized.getSN());
  assertTrue(c3.getExpDateAsCurvePoint() == c3_deserialized.getExpDateAsCurvePoint());

  // Test an incomplete budget coin de/serialization
  libutt::api::Coin c4(d,
                       Fr::random_element().to_words(),
                       Fr::random_element().to_words(),
                       fr_val.to_words(),
                       Fr::random_element().to_words(),
                       libutt::api::Coin::Type::Budget,
                       Fr::random_element().to_words());
  std::vector<uint8_t> c4_serialized = libutt::api::serialize<libutt::api::Coin>(c4);
  libutt::api::Coin c4_deserialized = libutt::api::deserialize<libutt::api::Coin>(c4_serialized);
  assertTrue(c4.getNullifier() == c4_deserialized.getNullifier());
  assertTrue(c4.hasSig() == c4_deserialized.hasSig());
  assertTrue(c4.getType() == c4_deserialized.getType());
  assertTrue(c4.getVal() == c4_deserialized.getVal());
  assertTrue(c4.getSN() == c4_deserialized.getSN());
  assertTrue(c4.getExpDateAsCurvePoint() == c4_deserialized.getExpDateAsCurvePoint());

  // Test Budget request de/serialization
  libutt::api::operations::Budget b1(
      d, Fr::random_element().to_words(), Fr::random_element().to_words(), 100, 987654321);
  std::vector<uint8_t> b1_serialized = libutt::api::serialize<libutt::api::operations::Budget>(b1);
  libutt::api::operations::Budget b1_deserialized =
      libutt::api::deserialize<libutt::api::operations::Budget>(b1_serialized);
  assertTrue(b1.getHashHex() == b1_deserialized.getHashHex());
  const auto b1_coin = b1.getCoin();
  const auto c1_deserialized_coin = b1_deserialized.getCoin();
  assertTrue(b1_coin.getNullifier() == c1_deserialized_coin.getNullifier());
  assertTrue(b1_coin.hasSig() == c1_deserialized_coin.hasSig());
  assertTrue(b1_coin.getType() == c1_deserialized_coin.getType());
  assertTrue(b1_coin.getVal() == c1_deserialized_coin.getVal());
  assertTrue(b1_coin.getSN() == c1_deserialized_coin.getSN());
  assertTrue(b1_coin.getExpDateAsCurvePoint() == c1_deserialized_coin.getExpDateAsCurvePoint());

  for (auto& c : clients) {
    std::vector<std::vector<uint8_t>> rsigs;
    std::string simulatonOfUniqueTxHash = std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
    auto mint = Mint(simulatonOfUniqueTxHash, 100, c.getPid());

    // Test Mint request de/serialization
    std::vector<uint8_t> mint_serialized = libutt::api::serialize<libutt::api::operations::Mint>(mint);
    auto mint_deserialized = libutt::api::deserialize<libutt::api::operations::Mint>(mint_serialized);
    assertTrue(mint.getHash() == mint_deserialized.getHash());
    assertTrue(mint.getVal() == mint_deserialized.getVal());
    assertTrue(mint.getRecipentID() == mint_deserialized.getRecipentID());
    for (size_t i = 0; i < banks.size(); i++) {
      rsigs.push_back(banks[i]->sign(mint).front());
    }
    auto sbs = testing::getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(mint, d, {blinded_sig}).front();
    libutt::api::operations::Burn b_op{d, c, coin};

    // Test Burn request de/serialization
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
  const auto& client1 = clients[0];
  const auto& client2 = clients[1];
  std::vector<types::Signature> rsigs;
  std::string simulatonOfUniqueTxHash = std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
  auto mint = Mint(simulatonOfUniqueTxHash, 100, client1.getPid());
  for (size_t i = 0; i < banks.size(); i++) {
    rsigs.push_back(banks[i]->sign(mint).front());
  }
  auto sbs = testing::getSubset((uint32_t)n, (uint32_t)thresh);
  std::map<uint32_t, types::Signature> sigs;
  for (auto i : sbs) {
    sigs[i] = rsigs[i];
  }
  auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
  auto coin = client1.claimCoins(mint, d, {blinded_sig}).front();

  rsigs.clear();
  auto budget = Budget(d, client1, 1000, 123456789);
  for (size_t i = 0; i < banks.size(); i++) {
    rsigs.push_back(banks[i]->sign(budget).front());
  }
  sbs = testing::getSubset((uint32_t)n, (uint32_t)thresh);
  sigs.clear();
  for (auto i : sbs) {
    sigs[i] = rsigs[i];
  }
  blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
  auto bcoin = client1.claimCoins(budget, d, {blinded_sig}).front();
  auto encryptor = std::make_shared<libutt::IBEEncryptor>(rc.toPK().mpk);
  Transaction tx(d, client1, {coin}, {bcoin}, {{client1.getPid(), 50}, {client2.getPid(), 50}}, *(encryptor));

  // Test Transaction de/serialization
  std::vector<uint8_t> serialized_tx = libutt::api::serialize<libutt::api::operations::Transaction>(tx);
  auto deserialized_tx = libutt::api::deserialize<libutt::api::operations::Transaction>(serialized_tx);
  for (size_t i = 0; i < tx.getInputCoins().size(); i++) {
    const auto& orig_coin = tx.getInputCoins().at(i);
    const auto& des_coin = deserialized_tx.getInputCoins().at(i);
    assertTrue(orig_coin.getNullifier() == des_coin.getNullifier());
    assertTrue(orig_coin.hasSig() == des_coin.hasSig());
    assertTrue(orig_coin.getType() == des_coin.getType());
    assertTrue(orig_coin.getVal() == des_coin.getVal());
    assertTrue(orig_coin.getSN() == des_coin.getSN());
    assertTrue(orig_coin.getExpDateAsCurvePoint() == des_coin.getExpDateAsCurvePoint());
  }
  assertTrue(tx.getNullifiers() == deserialized_tx.getNullifiers());
  auto orig_bcoin = tx.getBudgetCoin();
  auto deserialize_bcoin = deserialized_tx.getBudgetCoin();
  assertTrue(orig_bcoin->getNullifier() == deserialize_bcoin->getNullifier());
  assertTrue(orig_bcoin->hasSig() == deserialize_bcoin->hasSig());
  assertTrue(orig_bcoin->getType() == deserialize_bcoin->getType());
  assertTrue(orig_bcoin->getVal() == deserialize_bcoin->getVal());
  assertTrue(orig_bcoin->getSN() == deserialize_bcoin->getSN());
  assertTrue(orig_bcoin->getExpDateAsCurvePoint() == deserialize_bcoin->getExpDateAsCurvePoint());

  return 0;
}