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
                                                        const Commitment& partial_comm) const {
  RegistrationDetails rd;
  Fr reg_sn;
  // The clinet sends a commitment that contains its PRF key, hence, by taking the hash as sequence number we do commit
  // to this PRF keyu as well So, eventually, the commitment here is for the client pid, and PRF secret key.
  reg_sn.set_ulong(Commitment::getCommitmentSn(partial_comm));
  std::vector<std::vector<uint64_t>> m = {pid_hash, reg_sn.to_words(), Fr::zero().to_words()};
  auto comm = Commitment(d, Commitment::Type::REGISTRATION, m, true);
  rd.rcm_ = comm;
  Fr fr_pid;
  fr_pid.from_words(pid_hash);
  std::string h1 = Commitment::getCommitmentHash(rd.rcm_);
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  auto res = rsk_->sk.shareSign({(fr_pid * H), (reg_sn * H)}, H);
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