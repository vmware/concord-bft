#include "commitment.hpp"
#include "include/commitment.impl.hpp"
#include "include/params.impl.hpp"
#include <utt/Serialization.h>
#include <sstream>

libutt::api::Commitment operator+(libutt::api::Commitment lhs, const libutt::api::Commitment& rhs) {
  lhs += rhs;
  return lhs;
}

std::ostream& operator<<(std::ostream& out, const libutt::api::Commitment& comm) {
  out << comm.pImpl_->comm_;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::Commitment& comm) {
  in >> comm.pImpl_->comm_;
  return in;
}

bool operator==(const libutt::api::Commitment& comm1, const libutt::api::Commitment& comm2) {
  if (!comm1.pImpl_ && !comm2.pImpl_) return true;
  if (!comm1.pImpl_ || !comm2.pImpl_) return false;
  return comm1.pImpl_->comm_ == comm2.pImpl_->comm_;
}
namespace libutt::api {

Commitment::Commitment(const UTTParams& d, Type t, const std::vector<types::CurvePoint>& messages, bool withG2) {
  std::vector<Fr> fr_messages(messages.size());
  for (size_t i = 0; i < messages.size(); i++) {
    fr_messages[i].from_words(messages.at(i));
  }
  pImpl_ = new Commitment::Impl();
  pImpl_->comm_ =
      libutt::Comm::create(Impl::getCommitmentKey(d.pImpl_->p, (Commitment::Impl::Type)t), fr_messages, withG2);
}

Commitment::Commitment() { pImpl_ = new Commitment::Impl(); }
Commitment& Commitment::operator=(const Commitment& comm) {
  if (this == &comm) return *this;
  pImpl_->comm_ = comm.pImpl_->comm_;
  return *this;
}

Commitment::Commitment(const Commitment& comm) {
  pImpl_ = new Commitment::Impl();
  *this = comm;
}

Commitment& Commitment::operator+=(const Commitment& comm) {
  pImpl_->comm_ += comm.pImpl_->comm_;
  return *this;
}

Commitment::~Commitment() { delete pImpl_; }
Commitment::Commitment(Commitment&& other) {
  pImpl_ = new Commitment::Impl();
  *this = std::move(other);
}
Commitment& Commitment::operator=(Commitment&& other) {
  pImpl_->comm_ = std::move(other.pImpl_->comm_);
  return *this;
}

types::CurvePoint Commitment::rerandomize(const UTTParams& d,
                                          Type t,
                                          std::optional<types::CurvePoint> base_randomness) {
  Fr u_delta = Fr::random_element();
  if (base_randomness.has_value()) u_delta.from_words(*base_randomness);
  pImpl_->comm_.rerandomize(Impl::getCommitmentKey(d.pImpl_->p, (Commitment::Impl::Type)t), u_delta);
  return u_delta.to_words();
}
}  // namespace libutt::api