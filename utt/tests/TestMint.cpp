#include "testUtils.hpp"

#include "UTTParams.hpp"
#include "testUtils.hpp"
#include "mint.hpp"
#include <utt/MintOp.h>
#include <utt/Coin.h>
#include <memory>
#include <vector>
#include <cstdlib>
#include <ctime>

using namespace libutt;
using namespace libutt::api;
using namespace libutt::api::operations;
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
        assertTrue(b->validatePartialSignature<Mint>(signer, sig, 0, mint));
      }
    }
    auto sbs = testing::getSubset((uint32_t)n, (uint32_t)thresh);
    std::map<uint32_t, std::vector<uint8_t>> sigs;
    for (auto i : sbs) {
      sigs[i] = rsigs[i];
    }
    auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {sigs});
    auto coin = c.claimCoins(mint, d, {blinded_sig}).front();
    assertTrue(c.validate(coin));
  }
  return 0;
}