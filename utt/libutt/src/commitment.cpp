#include "commitment.hpp"
#include <utt/Comm.h>
#include <utt/Params.h>
#include <utt/RandSig.h>
#include <utt/Serialization.h>
#include <utt/PolyCrypto.h>

#include <sstream>

libutt::api::Commitment operator+(libutt::api::Commitment lhs, const libutt::api::Commitment& rhs) {
  libutt::api::Commitment comm = lhs;
  comm += rhs;
  return comm;
}

namespace libutt::api {
libutt::CommKey getCommitmentKey(libutt::Params& p, Commitment::Type t) {
  switch (t) {
    case Commitment::Type::REGISTRATION:
      return p.ck_reg;
    case Commitment::Type::VALUE:
      return p.ck_val;
    case Commitment::Type::COIN:
      return p.ck_coin;
      return libutt::CommKey();
  }
}
std::vector<Commitment> Commitment::create(Details& d,
                                           Type t,
                                           const std::vector<std::vector<uint64_t>>& messages,
                                           std::vector<std::vector<uint64_t>>& randomizations,
                                           bool withG2) {
  std::vector<Fr> fr_messages(messages.size());
  for (size_t i = 0; i < messages.size(); i++) {
    fr_messages[i].from_words(messages.at(i));
  }
  std::vector<Fr> fr_randomness(randomizations.size());
  for (size_t i = 0; i < randomizations.size(); i++) {
    fr_randomness[i].from_words(randomizations.at(i));
  }

  auto comms = libutt::Comm::create(getCommitmentKey(d.getParams(), t), fr_messages, fr_randomness, withG2);
  std::vector<Commitment> ret(comms.size());
  for (size_t i = 0; i < comms.size(); i++) {
    *(ret[i].comm_) = comms[i];
  }
  return ret;
}

Commitment::Commitment(Details& d, Type t, const std::vector<std::vector<uint64_t>>& messages, bool withG2) {
  std::vector<Fr> fr_messages(messages.size());
  for (size_t i = 0; i < messages.size(); i++) {
    fr_messages[i].from_words(messages.at(i));
  }
  comm_.reset(new libutt::Comm());
  *comm_ = libutt::Comm::create(getCommitmentKey(d.getParams(), t), fr_messages, withG2);
}
Commitment::Commitment(const std::string& comm) {
  comm_.reset(new libutt::Comm());
  *comm_ = libutt::deserialize<libutt::Comm>(comm);
}
Commitment::Commitment() { comm_.reset(new libutt::Comm()); }
Commitment& Commitment::operator=(const Commitment& comm) {
  comm_.reset(new libutt::Comm());
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

std::vector<uint64_t> Commitment::randomize(Details& d, Type t) {
  Fr u_delta = Fr::random_element();
  comm_->rerandomize(getCommitmentKey(d.getParams(), t), u_delta);
  return u_delta.to_words();
}
}  // namespace libutt::api