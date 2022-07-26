#pragma once
#include "commitment.hpp"
#include "details.hpp"
#include <string>
#include <memory>
#include <vector>
namespace libutt {
class RegAuthShareSK;
class RegAuthPK;
class RegAuthSK;
}  // namespace libutt
namespace libutt::api {
class RegistratorIdentity {
 public:
  RegistratorIdentity(const std::string& id, const std::string& rsk, const std::string& rbk, const RegAuthSK& tmp);
  
  std::vector<uint8_t> ComputeRCMSig(const std::vector<uint64_t>& pid_hash, const Commitment& rcm1) const;

  bool validateRCM(const Commitment& comm, const std::vector<uint8_t>& sig);

 private:
  std::string id_;
  std::unique_ptr<RegAuthShareSK> rsk_;
  std::unique_ptr<RegAuthPK> rpk_;
  std::unique_ptr<RegAuthSK> tmp_;
};
}  // namespace libutt::api