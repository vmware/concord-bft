#pragma once
#include "commitment.hpp"
#include "details.hpp"
#include "common.hpp"
#include "types.hpp"
#include <memory>
namespace libutt {
class AddrSK;
class RandSigPK;
class RegAuthPK;
}  // namespace libutt
namespace libutt::api {
namespace operations {
class Burn;
}
class ClientIdentity {
 public:
  ClientIdentity(const std::string& pid,
                 const std::string& bpk,
                 const std::string& rpk,
                 const std::string& csk,
                 const std::string& mpk);
  Commitment generatePartialRCM(Details& d);
  Commitment generateFullRCM(Details& d);
  std::string getPid() const;
  types::CurvePoint getPRFSecretKey() const;
  types::CurvePoint getPidHash() const;
  void setRCM(const Commitment& comm, const types::Signature& sig);
  std::pair<Commitment, types::Signature> getRcm() const;
  template <typename T>
  bool validate(const T&);

 private:
  friend class operations::Burn;
  std::unique_ptr<libutt::AddrSK> ask_;
  std::unique_ptr<libutt::RandSigPK> bpk_;
  std::unique_ptr<libutt::RegAuthPK> rpk_;
  Commitment rcm_;
  types::Signature rcm_sig_;
};
}  // namespace libutt::api
