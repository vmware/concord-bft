#include "registrator.hpp"
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
struct Registrator::Impl {
  Impl(const libutt::RegAuthShareSK& rsk,
       const libutt::RegAuthPK& rpk,
       const std::map<uint16_t, libutt::RegAuthSharePK>& validation_keys)
      : rsk_{rsk}, rpk_{rpk}, validation_keys_{validation_keys} {}
  libutt::RegAuthShareSK rsk_;
  libutt::RegAuthPK rpk_;
  std::map<uint16_t, libutt::RegAuthSharePK> validation_keys_;
};
Registrator::Registrator(uint16_t id,
                         const std::string& rsk,
                         const std::map<uint16_t, std::string>& validation_keys,
                         const std::string& rbk) {
  id_ = id;
  std::map<uint16_t, libutt::RegAuthSharePK> validation_keys_;
  for (const auto& [id, pk] : validation_keys) {
    validation_keys_[id] = libutt::deserialize<libutt::RegAuthSharePK>(pk);
  }
  impl_.reset(new Registrator::Impl(
      libutt::deserialize<libutt::RegAuthShareSK>(rsk), libutt::deserialize<libutt::RegAuthPK>(rbk), validation_keys_));
}

std::pair<types::CurvePoint, types::Signature> Registrator::signRCM(const types::CurvePoint& pid_hash,
                                                                    const types::CurvePoint& s2,
                                                                    const Commitment& rcm1) const {
  Fr fr_pid;
  fr_pid.from_words(pid_hash);
  Fr fr_s2;
  fr_s2.from_words(s2);
  auto h1 = hashToHex(pid_hash);
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  auto res = impl_->rsk_.sk.shareSign({(fr_pid * H), (fr_s2 * H) + *((libutt::Comm*)rcm1.getInternals())}, H);
  auto res_str = libutt::serialize<libutt::RandSigShare>(res);
  return {s2, types::Signature(res_str.begin(), res_str.end())};
}
bool Registrator::validatePartialRCMSig(uint16_t id,
                                        const types::CurvePoint& pid_hash,
                                        const types::CurvePoint& s2,
                                        const Commitment& rcm1,
                                        const types::Signature& sig) const {
  Fr fr_pid;
  fr_pid.from_words(pid_hash);
  Fr fr_s2;
  fr_s2.from_words(s2);
  auto h1 = hashToHex(pid_hash);
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  libutt::RandSigShare rsig = libutt::deserialize<libutt::RandSigShare>(sig);
  return rsig.verify({(fr_pid * H), (fr_s2 * H) + *((libutt::Comm*)rcm1.getInternals())},
                     impl_->validation_keys_.at(id).vk);
}
bool Registrator::validateRCM(const Commitment& comm, const types::Signature& sig) const {
  libutt::RandSig rsig = libutt::deserialize<libutt::RandSig>(sig);
  return rsig.verify(*((libutt::Comm*)comm.getInternals()), impl_->rpk_.vk);
}
}  // namespace libutt::api