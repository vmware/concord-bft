#pragma once
#include <utt/Tx.h>

namespace libutt::api::operations {
struct Transaction::Impl {
  libutt::Tx tx_;
};
}  // namespace libutt::api::operations
