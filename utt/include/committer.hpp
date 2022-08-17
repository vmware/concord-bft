#pragma once
#include "types.hpp"
#include "globalParams.hpp"
#include <string>
#include <memory>
#include <vector>
namespace libutt {
class RandSigShareSK;
class RegAuthPK;
class RandSigPK;
}  // namespace libutt
namespace libutt::api {
class CoinsSigner {
 public:
  CoinsSigner(const std::string& id, const std::string& bsk, const std::string& bvk, const std::string& rvk);
  template <typename T>
  std::vector<types::Signature> sign(T& data) const;
  const std::string& getId() const;
  template <typename T>
  bool validate(const GlobalParams& p, const T&, const types::Signature& sig) const;

 private:
  std::string bid_;
  std::unique_ptr<libutt::RandSigShareSK> bsk_;
  std::unique_ptr<libutt::RandSigPK> bvk_;
  std::unique_ptr<libutt::RegAuthPK> rvk_;
};
}  // namespace libutt::api