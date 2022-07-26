#include "nullifier.hpp"
#include <utt/Nullifier.h>
#include <utt/Params.h>
#include <utt/Serialization.h>

namespace libutt::api {
Nullifier::Nullifier(Details& d,
                     const types::CurvePoint& prf_secret_key,
                     const types::CurvePoint& sn,
                     const types::CurvePoint& randomization) {
  Fr sprf;
  sprf.from_words(prf_secret_key);
  Fr frsn;
  frsn.from_words(sn);
  Fr frrandom;
  frrandom.from_words(randomization);
  nullifier_.reset(new libutt::Nullifier(d.getParams().null, sprf, frsn, frrandom));
}

bool Nullifier::validate(Details& d) const { return nullifier_->verify(d.getParams().null); }

std::string Nullifier::toString() const { return libutt::serialize<libutt::Nullifier>(*nullifier_); }
}  // namespace libutt::api