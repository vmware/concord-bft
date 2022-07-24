#pragma once
#include "clientIdentity.hpp"
#include "coin.hpp"
#include "details.hpp"

#include <memory>
#include <optional>
#include <vector>
namespace libutt {
class Tx;
}
namespace libutt::api {
class Transaction {
  Transaction(Details& d,
              const ClientIdentity&,
              const std::vector<Coin>&,
              const std::optional<Coin>&,
              const std::vector<std::tuple<std::string, uint64_t>>&);

 private:
  std::shared_ptr<libutt::Tx> tx_;
};
}  // namespace libutt::api