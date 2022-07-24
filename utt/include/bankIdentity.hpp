#pragma once
#include <string>
#include <memory>
#include <vector>
namespace libutt {
class RandSigShareSK;
class RegAuthPK;
class RandSigPK;
}
namespace libutt::api {
class BankIdentity {
 public:
  BankIdentity(const std::string& id, const std::string& bsk, const std::string& bvk, const std::string& rvk);
  template <typename T>
  std::vector<uint8_t> sign(T& data) const;
  const std::string& getId() const;
  template<typename T> 
  bool validate(const T&, std::vector<uint8_t> sig) const;
 private:
  std::string bid_;
  std::unique_ptr<libutt::RandSigShareSK> bsk_;
  std::unique_ptr<libutt::RandSigPK> bvk_;
  std::unique_ptr<libutt::RegAuthPK> rvk_;
};
}  // namespace libutt::api