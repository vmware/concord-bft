#include "registrator.hpp"
#include "commitment.hpp"
#include "common.hpp"
#include "include/commitment.impl.hpp"
#include "include/registrator.impl.hpp"
#include <utt/PolyCrypto.h>
namespace libutt::api {
Registrator::Registrator(uint16_t id,
                         const std::string& rsk,
                         const std::map<uint16_t, std::string>& validation_keys,
                         const std::string& rbk) {
  id_ = id;
  pImpl_ = new Registrator::Impl();
  pImpl_->rsk_ = libutt::deserialize<libutt::RegAuthShareSK>(rsk);
  pImpl_->rpk_ = libutt::deserialize<libutt::RegAuthPK>(rbk);
  for (const auto& [id, pk] : validation_keys) {
    pImpl_->validation_keys_[id] = libutt::deserialize<libutt::RegAuthSharePK>(pk);
  }
}
Registrator::Registrator() { pImpl_ = new Registrator::Impl(); }
Registrator::~Registrator() { delete pImpl_; }
Registrator::Registrator(const Registrator& other) {
  pImpl_ = new Registrator::Impl();
  *this = other;
}
Registrator& Registrator::operator=(const Registrator& other) {
  if (this == &other) return *this;
  pImpl_->rsk_ = other.pImpl_->rsk_;
  pImpl_->rpk_ = other.pImpl_->rpk_;
  pImpl_->validation_keys_ = other.pImpl_->validation_keys_;
  return *this;
}
Registrator::Registrator(Registrator&& other) {
  pImpl_ = new Registrator::Impl();
  *this = std::move(other);
}
Registrator& Registrator::operator=(Registrator&& other) {
  pImpl_->rsk_ = std::move(other.pImpl_->rsk_);
  pImpl_->rpk_ = std::move(other.pImpl_->rpk_);
  pImpl_->validation_keys_ = std::move(other.pImpl_->validation_keys_);
  return *this;
}
std::pair<types::CurvePoint, types::Signature> Registrator::signRCM(const types::CurvePoint& pid_hash,
                                                                    const types::CurvePoint& s2,
                                                                    const Commitment& rcm1) const {
  Fr fr_pid;
  fr_pid.from_words(pid_hash);
  Fr fr_s2;
  fr_s2.from_words(s2);
  auto hash_base = pid_hash;
  hash_base.push_back(rcm1.pImpl_->nonce);
  auto h1 = hashToHex(hash_base);
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  auto res = pImpl_->rsk_.sk.shareSign({(fr_pid * H), (fr_s2 * H) + rcm1.pImpl_->comm_}, H);
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
  auto hash_base = pid_hash;
  hash_base.push_back(rcm1.pImpl_->nonce);
  auto h1 = hashToHex(hash_base);
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  libutt::RandSigShare rsig = libutt::deserialize<libutt::RandSigShare>(sig);
  return rsig.verify({(fr_pid * H), (fr_s2 * H) + rcm1.pImpl_->comm_}, pImpl_->validation_keys_.at(id).vk);
}
bool Registrator::validateRCM(const Commitment& comm, const types::Signature& sig) const {
  libutt::RandSig rsig = libutt::deserialize<libutt::RandSig>(sig);
  return rsig.verify(comm.pImpl_->comm_, pImpl_->rpk_.vk);
}
}  // namespace libutt::api