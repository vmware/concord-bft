#pragma once
#include <utt/MintOp.h>

namespace libutt::api::operations {
struct Mint::Impl {
  libutt::MintOp mint_;
};
}  // namespace libutt::api::operations