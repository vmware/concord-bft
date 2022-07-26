#include "commitment.hpp"
#include <utt/Comm.h>
#include <utt/Params.h>
#include <utt/RandSig.h>
#include <utt/Serialization.h>
#include <utt/PolyCrypto.h>

#include <sstream>

libutt::api::Commitment operator+(libutt::api::Commitment lhs, const libutt::api::Commitment& rhs) {
  lhs += rhs;
  return lhs;
}

namespace libutt::api {
libutt::CommKey& Commitment::getCommitmentKey(Details& d, Commitment::Type t) {
  switch (t) {
    case Commitment::Type::REGISTRATION:
      return d.getParams().ck_reg;
    case Commitment::Type::VALUE:
      return d.getParams().ck_val;
    case Commitment::Type::COIN:
      return d.getParams().ck_coin;
  }
  throw std::runtime_error("Unkknown commitment key type");
}

Commitment::Commitment(Details& d, Type t, const std::vector<types::CurvePoint>& messages, bool withG2) {
  std::vector<Fr> fr_messages(messages.size());
  for (size_t i = 0; i < messages.size(); i++) {
    fr_messages[i].from_words(messages.at(i));
  }
  comm_.reset(new libutt::Comm());
  *comm_ = libutt::Comm::create(Commitment::getCommitmentKey(d, t), fr_messages, withG2);
}
Commitment::Commitment(const std::string& comm) {
  comm_.reset(new libutt::Comm());
  *comm_ = libutt::deserialize<libutt::Comm>(comm);
}
Commitment::Commitment() { comm_.reset(new libutt::Comm()); }
Commitment& Commitment::operator=(const Commitment& comm) {
  *comm_ = *comm_ = *comm.comm_;
  return *this;
}

Commitment::Commitment(const Commitment& comm) {
  comm_.reset(new libutt::Comm());
  *comm_ = *comm.comm_;
}

size_t Commitment::getCommitmentSn(const Commitment& comm) {
  return std::hash<std::string>{}(libutt::serialize<libutt::Comm>(*(comm.comm_)));
}
std::string Commitment::getCommitmentHash(const Commitment& comm) {
  return hashToHex("rcm|" + libutt::serialize<libutt::Comm>(*(comm.comm_)));
}
Commitment& Commitment::operator+=(const Commitment& comm) {
  (*comm_) += *(comm.comm_);
  return *this;
}

types::CurvePoint Commitment::randomize(Details& d, Type t) {
  Fr u_delta = Fr::random_element();
  comm_->rerandomize(Commitment::getCommitmentKey(d, t), u_delta);
  return u_delta.to_words();
}
}  // namespace libutt::api