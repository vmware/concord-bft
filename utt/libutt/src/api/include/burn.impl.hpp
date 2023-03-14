#pragma once
#include "burn.hpp"
#include <utt/BurnOp.h>

namespace libutt::api::operations {
struct Burn::Impl {
  libutt::BurnOp burn_;
};
}  // namespace libutt::api::operations