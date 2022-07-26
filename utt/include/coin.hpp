#pragma once
#include "details.hpp"
#include "clientIdentity.hpp"
#include "nullifier.hpp"
#include "commitment.hpp"
#include "types.hpp"
#include <memory>
namespace libutt {
class Coin;
}
namespace libutt::api {
class ClientIdentity;
namespace operations {
class Burn;
class Mint;
}  // namespace operations
class Coin {
 public:
  enum Type { Normal = 0x0, Budget };
  Coin(Details& d,
       const types::CurvePoint& prf,
       const types::CurvePoint& sn,
       const types::CurvePoint& val,
       Type p,
       const types::CurvePoint& exp_date,
       ClientIdentity& cid);
  Coin() {}
  Coin(const Coin& c);
  Coin& operator=(const Coin& c);
  const std::string getNullifier() const;
  bool hasSig() const;
  void setSig(const types::Signature& sig);
  Type getType() const;
  types::Signature getSig() const;
  void randomize();

 private:
  friend class ClientIdentity;
  friend class operations::Burn;
  std::unique_ptr<libutt::Coin> coin_;
  bool has_sig_{false};

  Type type_;
};
}  // namespace libutt::api