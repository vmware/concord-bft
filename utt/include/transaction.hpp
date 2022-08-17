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
class IEncryptor;
}  // namespace libutt

namespace libutt::api::operations {
class Transaction {
 public:
  Transaction(const GlobalParams& d,
              const Client&,
              const std::vector<Coin>&,
              const std::optional<Coin>&,
              const std::vector<std::tuple<std::string, uint64_t>>&,
              const IEncryptor& encryptor);
  std::vector<std::string> getNullifiers() const;

 private:
  friend class libutt::api::CoinsSigner;
  friend class libutt::api::Client;
  std::shared_ptr<libutt::Tx> tx_;
};
}  // namespace libutt::api::operations