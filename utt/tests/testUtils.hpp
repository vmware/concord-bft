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

#include "gtest/gtest.h"

using namespace libutt;
using namespace libutt::api;
namespace libutt::api::testing {

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

class test_utt_instance : public ::testing::Test {
 public:
 protected:
  struct GpData {
    libutt::CommKey cck;
    libutt::CommKey rck;
  };
  void setUp(bool ibe, bool register_clients) {
    UTTParams::BaseLibsInitData base_libs_init_data;
    UTTParams::initLibs(base_libs_init_data);
    auto dkg = RandSigDKG(thresh, n, Params::NumMessages);
    rsk = RegAuthSK::generateKeyAndShares(thresh, n);
    GpData gp_data{dkg.getCK(), rsk.ck_reg};
    d = UTTParams::create((void*)(&gp_data));
    rsk.setIBEParams(d.getParams().ibe);
    rvk = rsk.toPK();
    bvk = dkg.getPK();

    std::map<uint16_t, std::string> validation_keys;
    for (size_t i = 0; i < n; i++) {
      auto& sk = rsk.shares[i];
      validation_keys[(uint16_t)i] = serialize<RegAuthSharePK>(sk.toPK());
    }
    for (size_t i = 0; i < n; i++) {
      registrators.push_back(std::make_shared<Registrator>(
          (uint16_t)i, serialize<RegAuthShareSK>(rsk.shares[i]), validation_keys, serialize<RegAuthPK>(rsk.toPK())));
    }

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

    if (ibe) {
      GenerateIbeClients();
    } else {
      GenerateRsaClients(pr_keys);
    }

    if (register_clients) {
      for (auto& c : clients) {
        registerClient(c);
      }
    }
  }
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

  void GenerateRsaClients(const std::vector<std::string>& pk) {
    c = pr_keys.size();
    std::vector<Client> clients;
    std::string bpk = libutt::serialize<libutt::RandSigPK>(bvk);
    std::string rpk = libutt::serialize<libutt::RegAuthPK>(rvk);

    for (size_t i = 0; i < c; i++) {
      std::string pid = "client_" + std::to_string(i);
      clients.push_back(Client(pid, bpk, rpk, pk.at(i)));
    }
  }

  void GenerateIbeClients() {
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
  }

  void registerClient(Client& c) {
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
    sig =
        Utils::unblindSignature(d, Commitment::Type::REGISTRATION, {Fr::zero().to_words(), Fr::zero().to_words()}, sig);
    c.setRCMSig(d, s2, sig);
  }

 public:
  size_t thresh = 2;
  size_t n = 4;
  size_t c = 10;
  libutt::RandSigPK bvk;
  libutt::RegAuthPK rvk;
  libutt::RegAuthSK rsk;
  libutt::api::UTTParams d;

  std::vector<std::shared_ptr<CoinsSigner>> banks;
  std::vector<std::shared_ptr<Registrator>> registrators;
  std::vector<Client> clients;
};
}  // namespace libutt::api::testing