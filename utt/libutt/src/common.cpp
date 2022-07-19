#include "common.hpp"
#include <utt/RandSig.h>
#include <utt/Comm.h>
#include <utt/Serialization.h>
#include <utt/Params.h>
namespace libutt::api {
std::vector<uint8_t> Utils::aggregateSigShares(Details& d,
                                               Commitment::Type t,
                                               uint32_t n,
                                               const std::map<uint32_t, std::vector<uint8_t>>& rsigs,
                                               std::vector<std::vector<uint64_t>> randomness) {
  std::vector<libutt::RandSigShare> shares;
  std::vector<size_t> signers;
  for (const auto& [sid, rsig] : rsigs) {
    shares.push_back(libutt::deserialize<libutt::RandSigShare>(rsig));
    signers.push_back((size_t)sid);
  }
  std::vector<Fr> fr_randomness(randomness.size());
  for (size_t i = 0; i < randomness.size(); i++) fr_randomness[i].from_words(randomness[i]);
  libutt::CommKey ck;
  switch (t) {
    case Commitment::Type::REGISTRATION:
      ck = d.getParams().ck_reg;
      break;
    case Commitment::Type::VALUE:
      ck = d.getParams().ck_val;
      break;
    case Commitment::Type::COIN:
      ck = d.getParams().ck_coin;
      break;
  }
  auto csig = libutt::RandSigShare::aggregate((size_t)(n), shares, signers, ck, fr_randomness);
  auto str_csig = libutt::serialize<libutt::RandSig>(csig);
  return std::vector<uint8_t>(str_csig.begin(), str_csig.end());
}
}  // namespace libutt::api