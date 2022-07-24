#include "clientIdentity.hpp"
#include "coin.hpp"
#include <utt/IBE.h>
#include <utt/Address.h>
#include <utt/RandSig.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/Coin.h>
#include <vector>
#include <sstream>
namespace libutt::api {
ClientIdentity::ClientIdentity(const std::string& pid, const std::string& bpk) {
  ask_.reset(new libutt::AddrSK());
  ask_->pid = pid;
  ask_->s = Fr::random_element();
  ask_->pid_hash = AddrSK::pidHash(pid);
  bpk_.reset(new libutt::RandSigPK());
  *bpk_ = libutt::deserialize<libutt::RandSigPK>(bpk);
}
Commitment ClientIdentity::generatePartialRCM(Details& d) {
  std::vector<std::vector<uint64_t>> m = {getPidHash(), ask_->s.to_words(), Fr::zero().to_words()};
  auto comm = Commitment(d, Commitment::Type::REGISTRATION, m, true);
  return comm;
}

std::string ClientIdentity::getPid() const { return ask_->pid; }
std::vector<uint64_t> ClientIdentity::getPidHash() const { return ask_->getPidHash().to_words(); }
std::vector<uint64_t> ClientIdentity::getPRFSecretKey() const { return ask_->s.to_words(); }

void ClientIdentity::setIBEDetails(const std::vector<uint8_t>& sk, const std::vector<uint8_t>& mpk) {
  ask_->e = libutt::deserialize<libutt::IBE::EncSK>(std::string(sk.begin(), sk.end()));
  ask_->mpk_ = libutt::deserialize<libutt::IBE::MPK>(std::string(mpk.begin(), mpk.end()));
}

void ClientIdentity::setRCM(const Commitment& comm, const std::vector<uint8_t>& sig) {
  rcm_ = comm;
  rcm_sig_ = sig;
  ask_->rcm = *(comm.comm_);
  ask_->rs = libutt::deserialize<libutt::RandSig>(sig);
}

std::pair<Commitment, std::vector<uint8_t>> ClientIdentity::getRcm() const { 
  auto tmp = libutt::serialize<libutt::RandSig>(ask_->rs);
  return {rcm_, std::vector<uint8_t>(tmp.begin(), tmp.end())}; }
template <>
bool ClientIdentity::validate<Coin>(const Coin& c) {
  return c.coin_->hasValidSig(*bpk_);
}
void ClientIdentity::randomizeRCM() {
  auto r_delta = rcm_.randomize(Details::instance(), Commitment::Type::REGISTRATION);
  Fr fr_r_delta;
  fr_r_delta.from_words(r_delta);
}

}  // namespace libutt::api