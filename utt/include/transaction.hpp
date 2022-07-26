#pragma once
#include "clientIdentity.hpp"
#include "coin.hpp"
#include "details.hpp"
#include "bankIdentity.hpp"
#include "clientIdentity.hpp"
#include <memory>
#include <optional>
#include <vector>
namespace libutt {
class Tx;
}

namespace libutt::api::operations {
class Transaction {
  Transaction(Details& d,
              const ClientIdentity&,
              const std::vector<Coin>&,
              const std::optional<Coin>&,
              const std::vector<std::tuple<std::string, uint64_t>>&);
  std::vector<std::string> getNullifiers() const;

 private:
  friend class libutt::api::BankIdentity;
  friend class libutt::api::ClientIdentity;
  std::shared_ptr<libutt::Tx> tx_;
};
}  // namespace libutt::api::operations