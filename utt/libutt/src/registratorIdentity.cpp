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
RegistratorIdentity::RegistratorIdentity(const std::string& id,
                                         const std::string& rsk,
                                         const std::string& rbk,
                                         const RegAuthSK& tmp) {
  id_ = id;
  rsk_.reset(new libutt::RegAuthShareSK());
  *rsk_ = libutt::deserialize<libutt::RegAuthShareSK>(rsk);
  rpk_.reset(new libutt::RegAuthPK());
  *rpk_ = libutt::deserialize<libutt::RegAuthPK>(rbk);
  tmp_.reset(new libutt::RegAuthSK());
  *tmp_ = tmp;
}

types::Signature RegistratorIdentity::ComputeRCMSig(const types::CurvePoint& pid_hash, const Commitment& rcm1) const {
  Fr fr_pid;
  fr_pid.from_words(pid_hash);
  auto h1 = hashToHex(pid_hash);
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  auto res = rsk_->sk.shareSign({(fr_pid * H), *(rcm1.comm_)}, H);
  auto res_str = libutt::serialize<libutt::RandSigShare>(res);
  return types::Signature(res_str.begin(), res_str.end());
}

bool RegistratorIdentity::validateRCM(const Commitment& comm, const types::Signature& sig) {
  libutt::RandSig rsig = libutt::deserialize<libutt::RandSig>(sig);
  return rsig.verify(*comm.comm_, rpk_->vk);
}
}  // namespace libutt::api