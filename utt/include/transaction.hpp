#pragma once
#include "client.hpp"
#include "coin.hpp"
#include "globalParams.hpp"
#include "committer.hpp"
#include "client.hpp"
#include <memory>
#include <optional>
#include <vector>
namespace libutt {
class Tx;
}

namespace libutt::api::operations {
class Transaction {
 public:
  Transaction(const GlobalParams& d,
              const Client&,
              const std::vector<Coin>&,
              const std::optional<Coin>&,
              const std::vector<std::tuple<std::string, uint64_t>>&);
  std::vector<std::string> getNullifiers() const;

 private:
  friend class libutt::api::Committer;
  friend class libutt::api::Client;
  std::shared_ptr<libutt::Tx> tx_;
};
}  // namespace libutt::api::operations