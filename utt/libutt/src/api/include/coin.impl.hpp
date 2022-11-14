#pragma once
#include "coin.hpp"
#include <utt/Coin.h>
namespace libutt::api {
struct Coin::Impl {
  libutt::Coin c;
  ~Impl() = default;
};
}  // namespace libutt::api
