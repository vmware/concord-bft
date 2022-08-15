#pragma once
#include "globalParams.hpp"
#include "client.hpp"
#include "commitment.hpp"
#include "types.hpp"
#include <memory>
namespace libutt {
class Coin;
}
namespace libutt::api {
class Client;
namespace operations {
class Burn;
class Mint;
class Transaction;
class Budget;
}  // namespace operations

class Coin {
 public:
  enum Type { Normal = 0x0, Budget };
  Coin(const GlobalParams& d,
       const types::CurvePoint& prf,
       const types::CurvePoint& sn,
       const types::CurvePoint& val,
       const types::CurvePoint& pidhash,
       Type p,
       const types::CurvePoint& exp_date);
  Coin(const GlobalParams& d,
       const types::CurvePoint& sn,
       const types::CurvePoint& val,
       const types::CurvePoint& pidhash,
       Type p,
       const types::CurvePoint& exp_date);
  Coin();
  Coin(const Coin& c);
  Coin& operator=(const Coin& c);
  const std::string getNullifier() const;
  bool hasSig() const;
  void setSig(const types::Signature& sig);
  Type getType() const;
  types::Signature getSig() const;
  void rerandomize();
  uint64_t getVal() const;
  types::CurvePoint getPidHash() const;
  types::CurvePoint getSN() const;
  std::string getExpDate() const;

 private:
  friend class Client;
  friend class operations::Burn;
  friend class operations::Transaction;
  friend class operations::Budget;
  std::unique_ptr<libutt::Coin> coin_;
  std::string exp_date_str_;
  bool has_sig_{false};

  Type type_;
};
}  // namespace libutt::api