#include "testUtils.hpp"

#include "globalParams.hpp"
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
#include <utt/DataUtils.hpp>

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
std::string privatek1 =
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIICXQIBAAKBgQDAyUwjK+m4EXyFKEha2NfQUY8eIqurRujNMqI1z7B3bF/8Bdg0\n"
    "wffIYJUjiXo13hB6yOq+gD62BGAPRjmgReBniT3d2rLU0Z72zQ64Gof66jCGQt0W\n"
    "0sfwDUv0XsfXXW9p1EGBYwIkgW6UGiABnkZcIUW4dzP2URoRCN/VIypkRwIDAQAB\n"
    "AoGAa3VIvSoTAoisscQ8YHcSBIoRjiihK71AsnAQvpHfuRFthxry4qVjqgs71i0h\n"
    "M7lt0iL/xePSEL7rlFf+cvnAFL4/j1R04ImBjRzWGnaNE8I7nNGGzJo9rL5I1oi3\n"
    "zN2yUucTSGm7qR0MCNVy26zNmCuS/FdBPsfdZ017OTsHtPECQQDlWXAJG6nHyw2o\n"
    "2cLYHzlyrrYgnWJkgFSKzr7VFNlHxfQSWXJ4zuDwhqkm3d176bVm4eHhDDv6f413\n"
    "iQGraKvTAkEA1zAzpxfI7LAqd3sObWYstQb03IXE7yddMgbhoMDCT3gXhNaHKfjT\n"
    "Z/GIk49jh8kyitN2FeYXXi9TiwrXStfhPQJBAMNea6ymjvstwoYKcgsOli5WG7ku\n"
    "uEkqdFoGAdObvfeA7gfPgE7e1AiwfVkpd+l9TVTFqFe/xzv8+fJQmEZ+lJcCQQDN\n"
    "5I7nh7h1zzEy1Qk+345TP262OT/u26kuHqtv1j+VLgDC10jIfg443D+jgITo/Tdg\n"
    "4WeRGHCva3TyCtNoBxq5AkA9KZpKof4ripad4oIuCJpR/ZhQATgQwR9f+FlAxgP0\n"
    "ABmBPCoxy4uGMtSBMqiiGpImbDuivYkhlBl7D8u8vn26\n"
    "-----END RSA PRIVATE KEY-----\n";
std::string publick1 =
    "-----BEGIN PUBLIC KEY-----\n"
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDAyUwjK+m4EXyFKEha2NfQUY8e\n"
    "IqurRujNMqI1z7B3bF/8Bdg0wffIYJUjiXo13hB6yOq+gD62BGAPRjmgReBniT3d\n"
    "2rLU0Z72zQ64Gof66jCGQt0W0sfwDUv0XsfXXW9p1EGBYwIkgW6UGiABnkZcIUW4\n"
    "dzP2URoRCN/VIypkRwIDAQAB\n"
    "-----END PUBLIC KEY-----\n";

std::string privatek2 =
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIICXAIBAAKBgQDMw+qeJXQ/ZxrqSqjRXoN2wFYNwhljh7RTBJuIzaqK2pDL+zaK\n"
    "aREzgufG/7r2uk8njWW7EJbriLcmj1oJA913LPl4YWUEORKl7Cb6wLoPO/E5gAt1\n"
    "JJ0dhDeCRU+E7vtNQ4pLyy2dYS2GHO1Tm88yuFegS3gkINPYgzNggGJ2cQIDAQAB\n"
    "AoGAScyCjqTpFMDQTojB91OdBfukCClghSKvtwv+EnwtbwX/EcVkjtX3QR1485vP\n"
    "goT7akHn3FfKTPFlMRyRUpZ2Bov1whQ1ztuboIonFQ7ohbDTLE3QzUv4L3e8cEbY\n"
    "51MSe8tEUVRxKu53nD3asWxAi/CEyqWvRCzf4s3Q6Xw3h5ECQQD8mBT6ervLr1Qk\n"
    "oKmaAuPTDyZDaSjipD0/d1p1vG8Wb8go14tq89Ts+UIWzH5aGlidxTK9j/luQqlR\n"
    "YVVGNkC3AkEAz4a8jtg2++fvWT0PDz6OBfw/iHJQzSmynlzKQzpRac02UBCPo4an\n"
    "o7wl9uEnucXuVpCSo0JdSf+x4r9dwmCKFwJBAPWlGNG2xicBbPzp2cZTBSheVUG9\n"
    "ZOtz+bRc5/YTuJzDPI6rf4QVeH60sNbnLAGIGaHlAsFi4Jmf7nWcCIftfuUCQEbx\n"
    "hJxAhetvyn7zRKatd9fL99wpWD4Ktyk0B2EcGqDUqnCMeM4qRjzPIRtYtT/oziWB\n"
    "nt943HNjmeguC1tbrVkCQBMd+kbpcoFHKKrC577FM24maWRTfXJeu2/o6pxUFIUY\n"
    "kzkDZ2k+FfvXWaY+N5q5bJCayor8W1QeruHzewrQmgY=\n"
    "-----END RSA PRIVATE KEY-----\n";
std::string publick2 =
    "-----BEGIN PUBLIC KEY-----\n"
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDMw+qeJXQ/ZxrqSqjRXoN2wFYN\n"
    "whljh7RTBJuIzaqK2pDL+zaKaREzgufG/7r2uk8njWW7EJbriLcmj1oJA913LPl4\n"
    "YWUEORKl7Cb6wLoPO/E5gAt1JJ0dhDeCRU+E7vtNQ4pLyy2dYS2GHO1Tm88yuFeg\n"
    "S3gkINPYgzNggGJ2cQIDAQAB\n"
    "-----END PUBLIC KEY-----\n";

std::string privatek3 =
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIICXAIBAAKBgQCnmdqW7f/rg8CiUzLugxc0jPMqzphhtl40IqINAk3pasCQsA3T\n"
    "eHSa1fcKocFMY9bfQRvqiKpnK74d0V0fujFUaPIMlKMLWXEVefwe1fOYrDXOoz7N\n"
    "3Go7x9u/LXwCA6HehXtIavtTPQs1yHldb/bDocEhjfGvU3TLXkAuGWsnmQIDAQAB\n"
    "AoGBAJ6fRHyYICB8f7Kh35BRTYMU64fWI+5GtX3OUWTSi36g5EOL/GnqlSF94+OS\n"
    "F+n+i/ycGJmuYuhmQ/bgkaxXghsDYb7fsdMJ8DEqUJAKbxeOosn8fxwmJkNAJ07J\n"
    "+oAg/xkJ+ukyYnPf0P3UTuTZl0EFEpwu/vnX09QJGtuXgmQhAkEA0c0Co9MdP62r\n"
    "/ybXXeqgaol2YVGzFr/bMz3hhlviV9IOGPRZmeQ8v+f1/lSsqZ8wSP5P8dkBo4UB\n"
    "NSLaHAUL/QJBAMyB72EyHZUEFy3o241myqamfVtN+Dzo6qdPn/PfF/BLjwsEApCO\n"
    "oUObmDDo/yiSSb00XSnn23bGYH1VJJDNJs0CQE1aG+YQ+VC4FJkfVfpvfjOpePcK\n"
    "q0/w7r2mzBbAm+QrMz1qIfsGVoue12itCXgElEXlVc5iZyNF75sKvYXlKnUCQHHC\n"
    "tc5zelEyfVJkff0ieQhLBOCNdtErH50Chg+6wi5BWcje6i5PqRVasEZE1etTtQEy\n"
    "58Av4b0ojPQrMLP76uECQEz0c1RPDwMvznwT3BJxl8t4tixPML0nyBMD8ttnZswG\n"
    "K/1CYV1uMNbchmuVQb2Kd2JyE1gQF8s3ShsbteMc5og=\n"
    "-----END RSA PRIVATE KEY-----\n";

std::string publick3 =
    "-----BEGIN PUBLIC KEY-----\n"
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCnmdqW7f/rg8CiUzLugxc0jPMq\n"
    "zphhtl40IqINAk3pasCQsA3TeHSa1fcKocFMY9bfQRvqiKpnK74d0V0fujFUaPIM\n"
    "lKMLWXEVefwe1fOYrDXOoz7N3Go7x9u/LXwCA6HehXtIavtTPQs1yHldb/bDocEh\n"
    "jfGvU3TLXkAuGWsnmQIDAQAB\n"
    "-----END PUBLIC KEY-----\n";

std::vector<std::string> pr_keys{privatek1, privatek2, privatek3};
std::vector<std::string> pkeys{publick1, publick2, publick3};
int main(int argc, char* argv[]) {
  std::srand(0);
  (void)argc;
  (void)argv;
  size_t thresh = 3;
  size_t n = 4;
  size_t c = 3;
  auto [d, dkg, rc] = testing::init(n, thresh);
  auto registrators = testing::GenerateRegistrators(n, rc);
  auto banks = testing::GenerateCommitters(n, dkg, rc.toPK());
  auto clients = testing::GenerateClients(c, dkg.getPK(), rc.toPK(), pr_keys);
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
    auto sbs = testing::getSubGroup((uint32_t)n, (uint32_t)thresh);
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
    std::map<std::string, std::string> tx_pub_keys;
    tx_pub_keys[issuer.getPid()] = pkeys[i];
    tx_pub_keys[receiver.getPid()] = pkeys[(i + 1) % clients.size()];
    libutt::RSAEncryptor tx_encryptor(tx_pub_keys);
    Transaction tx(d,
                   issuer,
                   {coins[issuer.getPid()].front()},
                   {bcoins[issuer.getPid()]},
                   {{issuer.getPid(), 50}, {receiver.getPid(), 50}},
                   tx_encryptor);
    coins[issuer.getPid()].erase(coins[issuer.getPid()].begin());
    bcoins.erase(issuer.getPid());
    std::unordered_map<size_t, std::vector<types::Signature>> shares;
    for (size_t i = 0; i < banks.size(); i++) {
      shares[i] = banks[i]->sign(tx);
    }
    size_t num_coins = shares[0].size();
    std::vector<types::Signature> sigs;
    for (size_t i = 0; i < num_coins; i++) {
      std::map<uint32_t, types::Signature> share_sigs;
      auto sbs = testing::getSubGroup((uint32_t)n, (uint32_t)thresh);
      for (auto s : sbs) {
        share_sigs[s] = shares[s][i];
      }
      auto blinded_sig = Utils::aggregateSigShares((uint32_t)n, {share_sigs});
      sigs.emplace_back(std::move(blinded_sig));
    }
    auto issuer_coins = issuer.claimCoins(tx, d, sigs);
    for (auto& coin : issuer_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[issuer.getPid()].emplace_back(std::move(coin));
      } else {
        assertTrue(bcoins.find(issuer.getPid()) == bcoins.end());
        bcoins.emplace(issuer.getPid(), coin);
      }
    }
    auto receiver_coins = receiver.claimCoins(tx, d, sigs);
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