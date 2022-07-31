#pragma once

#include <utt/IBE.h>

#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/bio.h>
#include <openssl/evp.h>

#include <vector>
#include <cstdint>
#include <unordered_map>
#include <memory>
namespace libutt {
class IEncryptor {
 public:
  virtual std::vector<uint8_t> encrypt(const std::string& id, const std::vector<uint8_t>& msg) const = 0;
  virtual ~IEncryptor() {}
};

class IDecryptor {
 public:
  virtual std::vector<uint8_t> decrypt(const std::vector<uint8_t>&) const = 0;
  virtual ~IDecryptor() {}
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

class RSAEncryptor : public IEncryptor {
 public:
  RSAEncryptor(const std::unordered_map<std::string, std::string>& rsa_public_keys_map);
  ~RSAEncryptor() override;
  std::vector<uint8_t> encrypt(const std::string& id, const std::vector<uint8_t>& msg) const override;

 private:
  mutable std::unordered_map<std::string, EVP_PKEY*> encryptors_;
};

class RSADecryptor : public IDecryptor {
 public:
  RSADecryptor(const std::string& rsa_private_key);
  std::vector<uint8_t> decrypt(const std::vector<uint8_t>&) const override;
  ~RSADecryptor() override;

 private:
  EVP_PKEY* pkey_;
};

class EncryptionSystem {
 public:
  template <typename encIn, typename decIn>
  static std::pair<std::shared_ptr<IEncryptor>, std::shared_ptr<IDecryptor>> create(const encIn& enc, const decIn& dec);
};
}  // namespace libutt