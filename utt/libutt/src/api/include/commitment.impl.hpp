#pragma once
#include <utt/Comm.h>
#include <utt/Params.h>

namespace libutt::api {
struct Commitment::Impl {
  enum Type { REGISTRATION = 0, COIN };
  libutt::Comm comm_;
  static const libutt::CommKey& getCommitmentKey(const libutt::Params& d, Type t) {
    switch (t) {
      case Type::REGISTRATION:
        return d.getRegCK();
      case Type::COIN:
        return d.getCoinCK();
    }
    throw std::runtime_error("Unknown commitment key type");
  }
};
}  // namespace libutt::api