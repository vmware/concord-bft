#pragma once
#include <utt/Address.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>
#include <utt/DataUtils.hpp>

namespace libutt::api {
struct Client::Impl {
  libutt::AddrSK ask_;
  libutt::RandSigPK bpk_;
  libutt::RegAuthPK rpk_;
  std::shared_ptr<libutt::IDecryptor> decryptor_;
};
}  // namespace libutt::api