#include "registratorIdentity.hpp"
#include "commitment.hpp"
#include "common.hpp"
#include <utt/RegAuth.h>
#include <utt/RandSig.h>
#include <utt/Serialization.h>
#include <utt/Comm.h>
#include <utt/IBE.h>
#include <utt/PolyCrypto.h>
#include <utt/Params.h>
namespace libutt::api {
RegistratorIdentity::RegistratorIdentity(const std::string& id, const std::string& rsk, const std::string& rbk) {
  id_ = id;
  rsk_.reset(new libutt::RegAuthShareSK());
  *rsk_ = libutt::deserialize<libutt::RegAuthShareSK>(rsk);
  rpk_.reset(new libutt::RegAuthPK());
  *rpk_ = libutt::deserialize<libutt::RegAuthPK>(rbk);
}
RegistrationDetails RegistratorIdentity::registerClient(Details& d,
                                                        const std::string& pid,
                                                        const std::vector<uint64_t>& pid_hash,
                                                        const std::vector<uint64_t>& prf) const {
  RegistrationDetails rd;
  Fr fr_pid;
  fr_pid.from_words(pid_hash);
  Fr fr_prf;
  fr_prf.from_words(prf);
  std::vector<std::vector<uint64_t>> m = {pid_hash, prf, Fr::zero().to_words()};
  auto comm = Commitment(d, Commitment::Type::REGISTRATION, m, true);
  rd.rcm_ = comm;
  std::string h1 = Commitment::getCommitmentHash(rd.rcm_);
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  auto res = rsk_->sk.shareSign({(fr_pid * H), (fr_prf * H)}, H);
  auto res_str = libutt::serialize<libutt::RandSigShare>(res);
  rd.rcm_sig_ = std::vector<uint8_t>(res_str.begin(), res_str.end());
  rd.dsk_pub_ = getMPK();
  rd.dsk_priv_ = generateSecretForPid(pid);
  return rd;
}

std::vector<uint8_t> RegistratorIdentity::generateSecretForPid(const std::string& pid) const {
  auto res = rsk_->msk.deriveEncSK(rsk_->p, pid);
  auto res_str = libutt::serialize<libutt::IBE::EncSK>(res);
  return std::vector<uint8_t>(res_str.begin(), res_str.end());
}

std::vector<uint8_t> RegistratorIdentity::getMPK() const {
  auto res_str = libutt::serialize<libutt::IBE::MPK>(rsk_->toPK().mpk);
  return std::vector<uint8_t>(res_str.begin(), res_str.end());
}
bool RegistratorIdentity::validateRCM(const Commitment& comm, const std::vector<uint8_t>& sig) {
  libutt::RandSig rsig = libutt::deserialize<libutt::RandSig>(sig);
  return rsig.verify(*comm.comm_, rpk_->vk);
}
}  // namespace libutt::api