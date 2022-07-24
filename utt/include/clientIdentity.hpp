#pragma once
#include "commitment.hpp"
#include "details.hpp"
#include "common.hpp"
#include <memory>
namespace libutt {
class AddrSK;
class RandSigPK;
}  // namespace libutt
namespace libutt::api {
class ClientIdentity {
 public:
  ClientIdentity(const std::string& pid, const std::string& bpk);
  Commitment generatePartialRCM(Details& d);
  std::string getPid() const;
  std::vector<uint64_t> getPRFSecretKey() const;
  std::vector<uint64_t> getPidHash() const;
  void setIBEDetails(const std::vector<uint8_t>& sk, const std::vector<uint8_t>& mpk);
  void setRCM(const Commitment& comm, const std::vector<uint8_t>& sig);
  std::pair<Commitment, std::vector<uint8_t>> getRcm() const;
  template <typename T>
  bool validate(const T&);

 private:
  std::unique_ptr<libutt::AddrSK> ask_;
  std::unique_ptr<libutt::RandSigPK> bpk_;
  Commitment rcm_;
  std::vector<uint8_t> rcm_sig_;
};
}  // namespace libutt::api
