#pragma once
#include <utt/Params.h>

namespace libutt::api {
struct UTTParams::Impl {
  libutt::Params p;
};
}  // namespace libutt::api