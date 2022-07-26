#include "clientIdentity.hpp"
#include "coin.hpp"
#include "burn.hpp"
#include <utt/IBE.h>
#include <utt/Address.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/Coin.h>
#include <utt/BurnOp.h>
#include <vector>
#include <sstream>
namespace libutt::api {
ClientIdentity::ClientIdentity(const std::string& pid,
                               const std::string& bpk,
                               const std::string& rvk,
                               const std::string& csk,
                               const std::string& mpk) {
  ask_.reset(new libutt::AddrSK());
  ask_->pid = pid;
  ask_->s = Fr::random_element();
  ask_->pid_hash = AddrSK::pidHash(pid);
  bpk_.reset(new libutt::RandSigPK());
  *bpk_ = libutt::deserialize<libutt::RandSigPK>(bpk);
  rpk_.reset(new libutt::RegAuthPK());
  *rpk_ = libutt::deserialize<libutt::RegAuthPK>(rvk);
  ask_->e = libutt::deserialize<libutt::IBE::EncSK>(csk);
  ask_->mpk_ = libutt::deserialize<libutt::IBE::MPK>(mpk);
}
Commitment ClientIdentity::generateFullRCM(Details& d) {
  std::vector<std::vector<uint64_t>> m = {getPidHash(), ask_->s.to_words(), Fr::zero().to_words()};
  auto comm = Commitment(d, Commitment::Type::REGISTRATION, m, true);
  return comm;
}
Commitment ClientIdentity::generatePartialRCM(Details& d) {
  std::vector<std::vector<uint64_t>> m = {Fr::zero().to_words(), ask_->s.to_words(), Fr::zero().to_words()};
  auto comm = Commitment(d, Commitment::Type::REGISTRATION, m, true);

  auto& reg_ck = d.getParams().ck_reg;
  auto h1 = hashToHex(getPidHash());
  G1 H = libutt::hashToGroup<G1>("ps16base|" + h1);
  CommKey ck_extra({H, reg_ck.getGen1()});
  *(comm.comm_) = libutt::Comm::create(ck_extra, {ask_->s, Fr::zero()}, false);
  return comm;
}

std::string ClientIdentity::getPid() const { return ask_->pid; }
std::vector<uint64_t> ClientIdentity::getPidHash() const { return ask_->getPidHash().to_words(); }
std::vector<uint64_t> ClientIdentity::getPRFSecretKey() const { return ask_->s.to_words(); }

void ClientIdentity::setRCM(const Commitment& comm, const std::vector<uint8_t>& sig) {
  rcm_ = comm;
  rcm_sig_ = sig;
  ask_->rcm = *(comm.comm_);
  ask_->rs = libutt::deserialize<libutt::RandSig>(sig);
}

std::pair<Commitment, std::vector<uint8_t>> ClientIdentity::getRcm() const {
  auto tmp = libutt::serialize<libutt::RandSig>(ask_->rs);
  return {rcm_, std::vector<uint8_t>(tmp.begin(), tmp.end())};
}
template <>
bool ClientIdentity::validate<Coin>(const Coin& c) {
  return c.coin_->hasValidSig(*bpk_);
}

}  // namespace libutt::api