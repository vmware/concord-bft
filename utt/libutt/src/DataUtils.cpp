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
    RSA* pb_key;
    if (!PEM_read_bio_RSA_PUBKEY(keybio, &pb_key, nullptr, nullptr)) {
      throw std::runtime_error("Failed to create rsa public key");
    }
    encryptors_[id] = EVP_PKEY_new();
    EVP_PKEY_assign_RSA(encryptors_[id], pb_key);
    BIO_free(keybio);
    RSA_free(pb_key);
  }
}
RSAEncryptor::~RSAEncryptor() {
  for (auto [_, rsa_pub_key] : encryptors_) {
    if (rsa_pub_key) EVP_PKEY_free(rsa_pub_key);
  }
}
std::vector<uint8_t> RSAEncryptor::encrypt(const std::string& id, const std::vector<uint8_t>& msg) const {
  if (encryptors_.find(id) == encryptors_.end()) return {};
  EVP_CIPHER_CTX* ctx;
  int ciphertext_len{0};
  int len{0};
  unsigned char iv[EVP_MAX_IV_LENGTH] = {};
  int encrypted_key_len = EVP_PKEY_size(encryptors_.at(id));
  unsigned char* encrypted_key = (unsigned char*)malloc((size_t)encrypted_key_len);

  if (!(ctx = EVP_CIPHER_CTX_new())) return {};
  if (1 != EVP_SealInit(ctx, EVP_aes_256_cbc(), &encrypted_key, &encrypted_key_len, iv, &(encryptors_.at(id)), 1))
    return {};

  int blocksize = EVP_CIPHER_CTX_block_size(ctx);
  size_t metadata_size = sizeof(uint64_t) + (size_t)(encrypted_key_len) + msg.size();
  std::vector<uint8_t> ciphertext(metadata_size + (uint64_t)(blocksize - 1));
  std::memcpy(ciphertext.data(), &encrypted_key, sizeof(uint64_t));
  std::memcpy(ciphertext.data() + sizeof(uint64_t), encrypted_key, (size_t)encrypted_key_len);

  if (1 != EVP_SealUpdate(ctx, ciphertext.data() + metadata_size, &len, msg.data(), msg.size())) return {};
  ciphertext_len = len;

  if (1 != EVP_SealFinal(ctx, ciphertext.data() + metadata_size + len, &len)) return {};
  ciphertext_len += len;
  ciphertext.resize((size_t)(ciphertext_len) + metadata_size);
  free(encrypted_key);
  EVP_CIPHER_CTX_free(ctx);
  return ciphertext;
}

RSADecryptor::RSADecryptor(const std::string& rsa_private_key) {
  BIO* keybio;
  keybio = BIO_new_mem_buf(rsa_private_key.c_str(), -1);
  if (!keybio) {
    throw std::runtime_error("Failed to create key BIO");
  }
  RSA* priv_key;
  if (!PEM_read_bio_RSAPrivateKey(keybio, &priv_key, nullptr, nullptr)) {
    throw std::runtime_error("Failed to create rsa public key");
  }
  pkey_ = EVP_PKEY_new();
  EVP_PKEY_assign_RSA(pkey_, priv_key);
  BIO_free(keybio);
  RSA_free(priv_key);
}
RSADecryptor::~RSADecryptor() { EVP_PKEY_free(pkey_); }
std::vector<uint8_t> RSADecryptor::decrypt(const std::vector<uint8_t>& ctxt) const {
  uint64_t encrpted_key_size{0};
  std::memcpy(&encrpted_key_size, ctxt.data(), sizeof(uint64_t));
  unsigned char* encrypted_key = (unsigned char*)malloc((size_t)encrpted_key_size);
  std::memcpy(encrypted_key, ctxt.data() + sizeof(uint64_t), encrpted_key_size);
  const uint8_t* data = ctxt.data() + sizeof(uint64_t) + encrpted_key_size;
  uint64_t ciphertext_len = ctxt.size() - (sizeof(uint64_t) + encrpted_key_size);
  EVP_CIPHER_CTX* ctx;
  int len{0};
  int plaintext_len{0};
  std::vector<uint8_t> ptxt(ctxt.size());
  unsigned char iv[EVP_MAX_IV_LENGTH] = {};
  /* Create and initialise the context */
  if (!(ctx = EVP_CIPHER_CTX_new())) return {};
  if (1 != EVP_OpenInit(ctx, EVP_aes_256_cbc(), encrypted_key, (int)encrpted_key_size, iv, pkey_)) return {};
  if (1 != EVP_OpenUpdate(ctx, ptxt.data(), &len, data, ciphertext_len)) return {};
  plaintext_len = len;
  if (1 != EVP_OpenFinal(ctx, ptxt.data() + len, &len)) return {};
  plaintext_len += len;
  ptxt.resize((size_t)plaintext_len);
  EVP_CIPHER_CTX_free(ctx);
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