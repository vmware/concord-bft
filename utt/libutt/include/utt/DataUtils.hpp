#pragma once

#include <utt/IBE.h>
#include <vector>
#include <cstdint>
namespace libutt {
class IEncryptor {
 public:
  virtual std::vector<uint8_t> encrypt(const std::string& id, const std::vector<uint8_t>& msg) const = 0;
};

class IDecryptor {
 public:
  virtual std::vector<uint8_t> decrypt(const std::vector<uint8_t>&) const = 0;
};

class IBEEncryptor : public IEncryptor {
 public:
  IBEEncryptor(const IBE::MPK& mpk);
  std::vector<uint8_t> encrypt(const std::string& id, const std::vector<uint8_t>& msg) const override;

 private:
  IBE::MPK mpk_;
};

class IBEDecryptor : public IDecryptor {
 public:
  IBEDecryptor(const IBE::EncSK& esk);
  std::vector<uint8_t> decrypt(const std::vector<uint8_t>&) const override;

 private:
  IBE::EncSK esk_;
};
}  // namespace libutt