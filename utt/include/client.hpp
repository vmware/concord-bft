#pragma once
#include "commitment.hpp"
#include "globalParams.hpp"
#include "common.hpp"
#include "types.hpp"
#include <memory>
namespace libutt {
class AddrSK;
class RandSigPK;
class RegAuthPK;
}  // namespace libutt
namespace libutt::api {
class Coin;
namespace operations {
class Burn;
class Transaction;
}  // namespace operations
class Client {
 public:
  Client(const std::string& pid,
         const std::string& bpk,
         const std::string& rpk,
         const std::string& csk,
         const std::string& mpk);
  Commitment generateInputRCM();
  Commitment generateRCM(const GlobalParams& d);
  void setPRFKey(const types::CurvePoint& s2);
  const std::string& getPid() const;
  types::CurvePoint getPRFSecretKey() const;
  types::CurvePoint getPidHash() const;
  void setRCM(const Commitment& comm, const types::Signature& sig);
  std::pair<Commitment, types::Signature> getRcm() const;
  template <typename T>
  std::vector<libutt::api::Coin> claimCoins(const T&,
                                            const GlobalParams& d,
                                            uint32_t n,
                                            const std::vector<std::map<uint32_t, types::Signature>>& rsigs) const;

  template <typename T>
  bool validate(const T&) const;

 private:
  friend class operations::Burn;
  friend class operations::Transaction;
  std::unique_ptr<libutt::AddrSK> ask_;
  std::unique_ptr<libutt::RandSigPK> bpk_;
  std::unique_ptr<libutt::RegAuthPK> rpk_;
  Commitment rcm_;
  types::Signature rcm_sig_;
};
}  // namespace libutt::api
