#include <utt/DataUtils.hpp>
#include <xutils/AutoBuf.h>
#include <utt/Serialization.h>

#include <cstring>
#include <string>
#include <sstream>

namespace libutt {
IBEEncryptor::IBEEncryptor(const IBE::MPK& mpk) : mpk_{mpk} {}
std::vector<uint8_t> IBEEncryptor::encrypt(const std::string& id, const std::vector<uint8_t>& msg) const {
  AutoBuf<unsigned char> buf(msg.size());
  memcpy(buf.getBuf(), msg.data(), msg.size());
  auto ctxt = mpk_.encrypt(id, buf);
  auto ctxt_str = libutt::serialize<libutt::IBE::Ctxt>(ctxt);
  return std::vector<uint8_t>(ctxt_str.begin(), ctxt_str.end());
}

IBEDecryptor::IBEDecryptor(const IBE::EncSK& esk) : esk_{esk} {}
std::vector<uint8_t> IBEDecryptor::decrypt(const std::vector<uint8_t>& data) const {
  auto ctxt = libutt::deserialize<libutt::IBE::Ctxt>(std::string(data.begin(), data.end()));
  auto [succ, dtxt] = esk_.decrypt(ctxt);
  if (!succ) return {};
  std::vector<uint8_t> ret(dtxt.size());
  memcpy(ret.data(), dtxt.getBuf(), dtxt.size());
  return ret;
}

RSAEncryptor::RSAEncryptor(const std::unordered_map<std::string, std::string>& rsa_public_keys_map) {
  for (const auto& [id, pub_key_str] : rsa_public_keys_map) {
    BIO* keybio;
    keybio = BIO_new_mem_buf(pub_key_str.c_str(), -1);
    if (!keybio) {
      throw std::runtime_error("Failed to create key BIO");
    }
    RSA* pb_key = NULL;
    if (!PEM_read_bio_RSA_PUBKEY(keybio, &pb_key, nullptr, nullptr)) {
      throw std::runtime_error("Failed to create rsa public key");
    }
    encryptors_[id] = EVP_PKEY_new();
    EVP_PKEY_assign_RSA(encryptors_[id], pb_key);
    BIO_free(keybio);
    // RSA_free(pb_key);
  }
}
RSAEncryptor::~RSAEncryptor() {
  for (auto [_, rsa_pub_key] : encryptors_) {
    if (rsa_pub_key) EVP_PKEY_free(rsa_pub_key);
  }
}
std::vector<uint8_t> RSAEncryptor::encrypt(const std::string& id, const std::vector<uint8_t>& msg) const {
  if (encryptors_.find(id) == encryptors_.end()) throw std::runtime_error("Unknown id for encryption id: " + (id));
  EVP_PKEY_CTX* ctx;
  ctx = EVP_PKEY_CTX_new(encryptors_.at(id), NULL);
  if (!ctx) throw std::runtime_error("unable to encrypt data");
  if (EVP_PKEY_encrypt_init(ctx) <= 0) throw std::runtime_error("unable to encrypt data");
  // if (EVP_PKEY_CTX_set_rsa_padding(ctx, RSA_OAEP_PADDING) <= 0) throw std::runtime_error("unable to encrypt data");
  size_t out_len{0};
  if (EVP_PKEY_encrypt(ctx, NULL, &out_len, msg.data(), msg.size()) <= 0)
    throw std::runtime_error("unable to encrypt data");
  std::vector<uint8_t> ctxt(out_len);
  if (EVP_PKEY_encrypt(ctx, ctxt.data(), &out_len, msg.data(), msg.size()) <= 0)
    throw std::runtime_error("unable to encrypt data");
  ctxt.resize(out_len);
  return ctxt;
}

RSADecryptor::RSADecryptor(const std::string& rsa_private_key) {
  BIO* keybio;
  keybio = BIO_new_mem_buf(rsa_private_key.c_str(), -1);
  if (!keybio) {
    throw std::runtime_error("Failed to create key BIO");
  }
  RSA* priv_key = NULL;
  if (!PEM_read_bio_RSAPrivateKey(keybio, &priv_key, nullptr, nullptr)) {
    throw std::runtime_error("Failed to create rsa public key");
  }
  pkey_ = EVP_PKEY_new();
  EVP_PKEY_assign_RSA(pkey_, priv_key);
  BIO_free(keybio);
}
RSADecryptor::~RSADecryptor() { EVP_PKEY_free(pkey_); }
std::vector<uint8_t> RSADecryptor::decrypt(const std::vector<uint8_t>& ctxt) const {
  EVP_PKEY_CTX* ctx;
  ctx = EVP_PKEY_CTX_new(pkey_, NULL);
  if (!ctx) throw std::runtime_error("unable to decrypt data");
  if (EVP_PKEY_decrypt_init(ctx) <= 0) throw std::runtime_error("unable to decrypt data");
  size_t out_len{0};
  if (EVP_PKEY_decrypt(ctx, NULL, &out_len, ctxt.data(), ctxt.size()) <= 0) return {};
  std::vector<uint8_t> ptxt(out_len);
  if (EVP_PKEY_decrypt(ctx, ptxt.data(), &out_len, ctxt.data(), ctxt.size()) <= 0) return {};
  ptxt.resize(out_len);
  return ptxt;
}
template <>
std::pair<std::shared_ptr<IEncryptor>, std::shared_ptr<IDecryptor>>
EncryptionSystem::create<libutt::IBE::MPK, libutt::IBE::EncSK>(const libutt::IBE::MPK& enc,
                                                               const libutt::IBE::EncSK& dec) {
  return {std::make_shared<IBEEncryptor>(enc), std::make_shared<IBEDecryptor>(dec)};
}

template <>
std::pair<std::shared_ptr<IEncryptor>, std::shared_ptr<IDecryptor>>
EncryptionSystem::create<std::unordered_map<std::string, std::string>, std::string>(
    const std::unordered_map<std::string, std::string>& enc, const std::string& dec) {
  return {std::make_shared<RSAEncryptor>(enc), std::make_shared<RSADecryptor>(dec)};
}
}  // namespace libutt