#pragma once
#include "UTTParams.hpp"
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
#include <chrono>
using namespace libutt;
using namespace libutt::api;
namespace libutt::api::testing {
struct GpData {
  libutt::CommKey cck;
  libutt::CommKey rck;
};

std::vector<uint32_t> getSubset(uint32_t n, uint32_t size) {
  std::srand((unsigned int)std::chrono::system_clock::now().time_since_epoch().count());
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
std::tuple<libutt::api::UTTParams, RandSigDKG, RegAuthSK> init(size_t n, size_t thresh) {
  UTTParams::BaseLibsInitData base_libs_init_data;
  UTTParams::initLibs(base_libs_init_data);
  auto dkg = RandSigDKG(thresh, n, Params::NumMessages);
  auto rc = RegAuthSK::generateKeyAndShares(thresh, n);
  GpData gp_data{dkg.getCK(), rc.ck_reg};
  UTTParams d = UTTParams::create((void*)(&gp_data));
  rc.setIBEParams(((libutt::Params*)d.getParams())->ibe);
  return {d, dkg, rc};
}
std::vector<std::shared_ptr<Registrator>> GenerateRegistrators(size_t n, const RegAuthSK& rsk) {
  std::vector<std::shared_ptr<Registrator>> registrators;
  std::map<uint16_t, std::string> validation_keys;
  for (size_t i = 0; i < n; i++) {
    auto& sk = rsk.shares[i];
    validation_keys[(uint16_t)i] = serialize<RegAuthSharePK>(sk.toPK());
  }
  for (size_t i = 0; i < n; i++) {
    registrators.push_back(std::make_shared<Registrator>(
        (uint16_t)i, serialize<RegAuthShareSK>(rsk.shares[i]), validation_keys, serialize<RegAuthPK>(rsk.toPK())));
  }
  return registrators;
}

std::vector<std::shared_ptr<CoinsSigner>> GenerateCommitters(size_t n, const RandSigDKG& dkg, const RegAuthPK& rvk) {
  std::vector<std::shared_ptr<CoinsSigner>> banks;
  std::map<uint16_t, std::string> share_verification_keys_;
  for (size_t i = 0; i < n; i++) {
    share_verification_keys_[(uint16_t)i] = serialize<RandSigSharePK>(dkg.skShares[i].toPK());
  }
  for (size_t i = 0; i < n; i++) {
    banks.push_back(std::make_shared<CoinsSigner>((uint16_t)i,
                                                  serialize<RandSigShareSK>(dkg.skShares[i]),
                                                  serialize<RandSigPK>(dkg.getPK()),
                                                  share_verification_keys_,
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
void registerClient(const UTTParams& d,
                    Client& c,
                    std::vector<std::shared_ptr<Registrator>>& registrators,
                    size_t thresh) {
  size_t n = registrators.size();
  std::vector<std::vector<uint8_t>> shares;
  std::vector<uint16_t> shares_signers;
  auto prf = c.getPRFSecretKey();
  Fr fr_s2 = Fr::random_element();
  types::CurvePoint s2;
  auto rcm1 = c.generateInputRCM();
  for (auto& r : registrators) {
    auto [ret_s2, sig] = r->signRCM(c.getPidHash(), fr_s2.to_words(), rcm1);
    shares.push_back(sig);
    shares_signers.push_back(r->getId());
    if (s2.empty()) {
      s2 = ret_s2;
    }
  }
  for (auto& r : registrators) {
    for (size_t i = 0; i < shares.size(); i++) {
      auto& sig = shares[i];
      auto& signer = shares_signers[i];
      assertTrue(r->validatePartialRCMSig(signer, c.getPidHash(), s2, rcm1, sig));
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