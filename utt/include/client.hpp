#pragma once
#include "commitment.hpp"
#include "globalParams.hpp"
#include "common.hpp"
#include "types.hpp"
#include <memory>
#include <unordered_map>
namespace libutt {
class AddrSK;
class RandSigPK;
class RegAuthPK;
class IEncryptor;
class IDecryptor;
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
  Client(const std::string& pid,
         const std::string& bpk,
         const std::string& rpk,
         const std::string& rsaSk,
         const std::unordered_map<std::string, std::string>& rsa_pub_keys);
  Commitment generateInputRCM();
  void setPRFKey(const types::CurvePoint& s2);
  const std::string& getPid() const;
  types::CurvePoint getPRFSecretKey() const;
  types::CurvePoint getPidHash() const;

  void setRCMSig(const GlobalParams& d, const types::CurvePoint& s2, const types::Signature& sig);
  std::pair<Commitment, types::Signature> rerandomizeRcm(const GlobalParams& d) const;
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
  std::shared_ptr<libutt::IDecryptor> decryptor_;
  std::shared_ptr<libutt::IEncryptor> encryptor_;
  Commitment rcm_;
  types::Signature rcm_sig_;
  bool complete_s = false;
};
}  // namespace libutt::api
