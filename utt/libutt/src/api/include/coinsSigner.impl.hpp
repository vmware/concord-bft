#pragma once
#include <utt/RandSig.h>
#include <utt/RegAuth.h>

namespace libutt::api {
struct CoinsSigner::Impl {
  libutt::RandSigShareSK bsk_;
  libutt::RandSigPK bvk_;
  libutt::RegAuthPK rvk_;
  std::map<uint16_t, libutt::RandSigSharePK> shares_verification_keys_;
};
}  // namespace libutt::api