#pragma once
#include "commitment.hpp"
#include "common.hpp"
#include "details.hpp"
#include <string>
#include <memory>
#include <vector>
namespace libutt {
class RegAuthShareSK;
class RegAuthPK;
}  // namespace libutt
namespace libutt::api {
class RegistratorIdentity {
 public:
  RegistratorIdentity(const std::string& id, const std::string& rsk, const std::string& rbk);
  RegistrationDetails registerClient(Details& d,
                                     const std::string& pid,
                                     const std::vector<uint64_t>& pid_hash,
                                     const Commitment& partiail_comm) const;
  bool validateRCM(const Commitment& comm, const std::vector<uint8_t>& sig);

 private:
  std::vector<uint8_t> generateSecretForPid(const std::string& pid) const;
  std::vector<uint8_t> getMPK() const;
  std::string id_;
  std::unique_ptr<RegAuthShareSK> rsk_;
  std::unique_ptr<RegAuthPK> rpk_;
};
}  // namespace libutt::api