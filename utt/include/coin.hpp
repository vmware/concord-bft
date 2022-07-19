#pragma once
#include "details.hpp"
#include "clientIdentity.hpp"
#include "nullifier.hpp"
#include "commitment.hpp"
#include <memory>
namespace libutt {
class Coin;
}
namespace libutt::api {
class ClientIdentity;
class Coin {
 public:
  enum Type { Normal = 0x0, Budget };
  Coin(Details& d,
       const std::vector<uint64_t>& sn,
       const std::vector<uint64_t>& val,
       Type p,
       const std::vector<uint64_t>& exp_date,
       ClientIdentity& cid);
  const Nullifier& getNullifier() const;
  bool hasSig() const;
  void setSig(const std::vector<uint8_t>& sig);
  Type getType() const;
  std::vector<uint8_t> getSig() const;
  void randomize();

 private:
  friend class ClientIdentity;
  std::unique_ptr<libutt::Coin> coin_;
  std::unique_ptr<Nullifier> nullifier_;
  bool has_sig_{false};

  Type type_;
};
}  // namespace libutt::api