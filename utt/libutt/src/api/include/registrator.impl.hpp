#pragma once
#include <utt/RegAuth.h>
#include <utt/RandSig.h>
#include <utt/Serialization.h>

namespace libutt::api {
struct Registrator::Impl {
  RegAuthShareSK rsk_;
  RegAuthPK rpk_;
  std::map<uint16_t, RegAuthSharePK> validation_keys_;
};
}  // namespace libutt::api