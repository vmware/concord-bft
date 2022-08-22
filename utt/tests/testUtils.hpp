#pragma once
#include "globalParams.hpp"
#include "coinsSigner.hpp"
#include "client.hpp"
#include "registrator.hpp"
#include "common.hpp"
#include "coin.hpp"
#include <utt/RegAuth.h>
#include <utt/RandSigDKG.h>
#include <utt/Serialization.h>
#include <utt/Params.h>
#include <utt/IBE.h>
#include <utt/Serialization.h>
#include <utt/Address.h>
#include <utt/Comm.h>
#include <utt/Coin.h>

#include <vector>
#include <memory>
#include <cstdlib>
#include <iostream>
#include <ctime>
#include <unordered_map>
using namespace libutt;
using namespace libutt::api;
namespace libutt::api::testing {
std::vector<uint32_t> getSubset(uint32_t n, uint32_t size) {
  std::srand((unsigned int)std::time(0));
  std::map<uint32_t, uint32_t> ret;
  for (uint32_t i = 0; i < n; i++) ret[i] = i;
  for (uint32_t i = 0; i < n - size; i++) {
    uint32_t index = (uint32_t)(std::rand()) % (uint32_t)(ret.size());
    ret.erase(index);
  }
  std::vector<uint32_t> rret;
  for (const auto& [k, v] : ret) {
    (void)k;
    rret.push_back(v);
  }
  return rret;
}
std::tuple<libutt::api::GlobalParams, RandSigDKG, RegAuthSK> init(size_t n, size_t thresh) {
  GlobalParams d;
  d.init();

  auto dkg = RandSigDKG(thresh, n, Params::NumMessages);
  d.getParams() = Params::random(dkg.getCK());
  auto rc = RegAuthSK::generateKeyAndShares(d.getParams().ck_reg, thresh, n, d.getParams().ibe);
  d.getParams().ck_reg = rc.ck_reg;
  return {d, dkg, rc};
}
std::vector<std::shared_ptr<Registrator>> GenerateRegistrators(size_t n, const RegAuthSK& rsk) {
  std::vector<std::shared_ptr<Registrator>> registrators;
  for (size_t i = 0; i < n; i++) {
    registrators.push_back(std::make_shared<Registrator>(
        std::to_string(i), serialize<RegAuthShareSK>(rsk.shares[i]), serialize<RegAuthPK>(rsk.toPK())));
  }
  return registrators;
}

std::vector<std::shared_ptr<CoinsSigner>> GenerateCommitters(size_t n, const RandSigDKG& dkg, const RegAuthPK& rvk) {
  std::vector<std::shared_ptr<CoinsSigner>> banks;
  for (size_t i = 0; i < n; i++) {
    banks.push_back(std::make_shared<CoinsSigner>(std::to_string(i),
                                                  serialize<RandSigShareSK>(dkg.skShares[i]),
                                                  serialize<RandSigPK>(dkg.getPK()),
                                                  serialize<RegAuthPK>(rvk)));
  }
  return banks;
}

std::vector<Client> GenerateClients(size_t c, const RandSigPK& bvk, const RegAuthPK& rvk, const RegAuthSK& rsk) {
  std::vector<Client> clients;
  std::string bpk = libutt::serialize<libutt::RandSigPK>(bvk);
  std::string rpk = libutt::serialize<libutt::RegAuthPK>(rvk);

  for (size_t i = 0; i < c; i++) {
    std::string pid = "client_" + std::to_string(i);
    auto sk = rsk.msk.deriveEncSK(rsk.p, pid);
    auto mpk = rsk.toPK().mpk;
    std::string csk = libutt::serialize<libutt::IBE::EncSK>(sk);
    std::string cmpk = libutt::serialize<libutt::IBE::MPK>(mpk);
    clients.push_back(Client(pid, bpk, rpk, csk, cmpk));
  }
  return clients;
}

std::vector<Client> GenerateClients(size_t c,
                                    const RandSigPK& bvk,
                                    const RegAuthPK& rvk,
                                    const std::vector<std::string>& pk) {
  std::vector<Client> clients;
  std::string bpk = libutt::serialize<libutt::RandSigPK>(bvk);
  std::string rpk = libutt::serialize<libutt::RegAuthPK>(rvk);

  for (size_t i = 0; i < c; i++) {
    std::string pid = "client_" + std::to_string(i);
    clients.push_back(Client(pid, bpk, rpk, pk.at(i)));
  }
  return clients;
}
void registerClient(const GlobalParams& d,
                    Client& c,
                    std::vector<std::shared_ptr<Registrator>>& registrators,
                    size_t thresh) {
  size_t n = registrators.size();
  std::vector<std::vector<uint8_t>> shares;
  auto prf = c.getPRFSecretKey();
  Fr fr_s2 = Fr::random_element();
  types::CurvePoint s2;
  auto rcm1 = c.generateInputRCM();
  for (auto& r : registrators) {
    auto [ret_s2, sig] = r->signRCM(c.getPidHash(), fr_s2.to_words(), rcm1);
    shares.push_back(sig);
    if (s2.empty()) {
      s2 = ret_s2;
    }
  }
  auto sids = getSubset((uint32_t)n, (uint32_t)thresh);
  std::map<uint32_t, std::vector<uint8_t>> rsigs;
  for (auto i : sids) {
    rsigs[i] = shares[i];
  }
  auto sig = Utils::aggregateSigShares((uint32_t)n, rsigs);
  sig = Utils::unblindSignature(d, Commitment::Type::REGISTRATION, {Fr::zero().to_words(), Fr::zero().to_words()}, sig);
  c.setRCMSig(d, s2, sig);
}
}  // namespace libutt::api::testing