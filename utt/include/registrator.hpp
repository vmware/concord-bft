#pragma once
#include "commitment.hpp"
#include "globalParams.hpp"
#include <string>
#include <memory>
#include <vector>
namespace libutt {
class RegAuthShareSK;
class RegAuthPK;
class RegAuthSK;
}  // namespace libutt
namespace libutt::api {
class Registrator {
 public:
  Registrator(const std::string& id, const std::string& rsk, const std::string& rbk, const RegAuthSK& tmp);

  types::Signature ComputeRCMSig(const types::CurvePoint& pid_hash, const Commitment& rcm1) const;

  bool validateRCM(const Commitment& comm, const types::Signature& sig) const;

 private:
  std::string id_;
  std::unique_ptr<RegAuthShareSK> rsk_;
  std::unique_ptr<RegAuthPK> rpk_;
  std::unique_ptr<RegAuthSK> tmp_;
};
}  // namespace libutt::api