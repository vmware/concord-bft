#pragma once
#include "details.hpp"
#include "bankIdentity.hpp"
#include "clientIdentity.hpp"
#include "registratorIdentity.hpp"
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
using namespace libutt;
using namespace libutt::api;
namespace libutt::api::testing {
std::vector<uint32_t> getSubGroup(uint32_t n, uint32_t size) {
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
std::pair<RandSigDKG, RegAuthSK> init(size_t n, size_t thresh) {
  auto& d = Details::instance();
  d.init();

  auto dkg = RandSigDKG(thresh, n, Params::NumMessages);
  d.getParams() = Params::random(dkg.getCK());
  auto rc = RegAuthSK::generateKeyAndShares(d.getParams().ck_reg, thresh, n, d.getParams().ibe);
  d.getParams().ck_reg = rc.ck_reg;
  return {dkg, rc};
}
std::vector<std::shared_ptr<RegistratorIdentity>> GenerateRegistrators(size_t n, const RegAuthSK& rsk) {
  std::vector<std::shared_ptr<RegistratorIdentity>> registrators;
  for (size_t i = 0; i < n; i++) {
    registrators.push_back(std::make_shared<RegistratorIdentity>(
        std::to_string(i), serialize<RegAuthShareSK>(rsk.shares[i]), serialize<RegAuthPK>(rsk.toPK())));
  }
  return registrators;
}

std::vector<std::shared_ptr<BankIdentity>> GenerateCommitters(size_t n, const RandSigDKG& dkg, const RegAuthPK& rvk) {
  std::vector<std::shared_ptr<BankIdentity>> banks;
  for (size_t i = 0; i < n; i++) {
    banks.push_back(std::make_shared<BankIdentity>(std::to_string(i), serialize<RandSigShareSK>(dkg.skShares[i]), serialize<RandSigPK>(dkg.getPK()),  serialize<RegAuthPK>(rvk)));
  }
  return banks;
}

std::vector<ClientIdentity> GenerateClients(size_t c, const RandSigPK& bvk, const RegAuthPK& rvk) {
  std::vector<ClientIdentity> clients;
  std::string bpk = libutt::serialize<libutt::RandSigPK>(bvk);
  std::string rpk = libutt::serialize<libutt::RegAuthPK>(rvk);

  for (size_t i = 0; i < c; i++) {
    clients.push_back(ClientIdentity("client_" + std::to_string(i), bpk, rpk));
  }
  return clients;
}

void registerClient(Details& d,
                    ClientIdentity& c,
                    std::vector<std::shared_ptr<RegistratorIdentity>>& registrators,
                    size_t thresh) {
  size_t n = registrators.size();
  std::vector<RegistrationDetails> rd;
  auto prf = c.getPRFSecretKey();
  for (auto& r : registrators) {
    rd.push_back(r->registerClient(d, c.getPid(), c.getPidHash(), prf));
  }
  auto sids = getSubGroup((uint32_t)n, (uint32_t)thresh);
  std::map<uint32_t, std::vector<uint8_t>> rsigs;
  for (auto i : sids) {
    rsigs[i] = rd[i].rcm_sig_;
  }
  auto sig = Utils::aggregateSigShares(
      d, Commitment::Type::REGISTRATION, (uint32_t)n, rsigs, {Fr::zero().to_words(), Fr::zero().to_words()});
  c.setRCM(rd[sids.front()].rcm_, sig);
  c.setIBEDetails(rd[sids.front()].dsk_priv_, rd[sids.front()].dsk_pub_);
}
}  // namespace libutt::api::testing