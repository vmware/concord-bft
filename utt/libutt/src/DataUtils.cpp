#include <utt/DataUtils.hpp>
#include <xutils/AutoBuf.h>
#include <utt/Serialization.h>

#include <cstring>
#include <string>
#include <sstream>

auto bio_deleter = [](BIO* bio) { BIO_free(bio); };
auto ctx_deleter = [](EVP_PKEY_CTX* epc) { EVP_PKEY_CTX_free(epc); };

typedef std::unique_ptr<BIO, decltype(bio_deleter)> ManagedBIO;
typedef std::unique_ptr<EVP_PKEY_CTX, decltype(ctx_deleter)> ManagedEPC;
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

RSAEncryptor::RSAEncryptor(const std::map<std::string, std::string>& rsa_public_keys_map) {
  for (const auto& [id, pub_key_str] : rsa_public_keys_map) {
    ManagedBIO key_bio{nullptr, bio_deleter};
    key_bio.reset(BIO_new_mem_buf(pub_key_str.c_str(), -1));
    if (!key_bio) {
      throw std::runtime_error("Failed to create key BIO");
    }
    RSA* pb_key = NULL;
    if (!PEM_read_bio_RSA_PUBKEY(key_bio.get(), &pb_key, nullptr, nullptr)) {
      throw std::runtime_error("Failed to create rsa public key");
    }
    encryptors_[id] = EVP_PKEY_new();
    EVP_PKEY_assign_RSA(encryptors_[id], pb_key);
  }
}
RSAEncryptor::~RSAEncryptor() {
  for (auto [_, rsa_pub_key] : encryptors_) {
    if (rsa_pub_key) EVP_PKEY_free(rsa_pub_key);
  }
}
std::vector<uint8_t> RSAEncryptor::encrypt(const std::string& id, const std::vector<uint8_t>& msg) const {
  if (encryptors_.find(id) == encryptors_.end()) throw std::runtime_error("Unknown id for encryption id: " + (id));
  ManagedEPC ctx{nullptr, ctx_deleter};
  ctx.reset(EVP_PKEY_CTX_new(encryptors_.at(id), NULL));
  if (!ctx) throw std::runtime_error("unable to encrypt data");
  if (EVP_PKEY_encrypt_init(ctx.get()) <= 0) throw std::runtime_error("unable to encrypt data");
  size_t out_len{0};
  if (EVP_PKEY_encrypt(ctx.get(), NULL, &out_len, msg.data(), msg.size()) <= 0)
    throw std::runtime_error("unable to encrypt data");
  std::vector<uint8_t> ctxt(out_len);
  if (EVP_PKEY_encrypt(ctx.get(), ctxt.data(), &out_len, msg.data(), msg.size()) <= 0)
    throw std::runtime_error("unable to encrypt data");
  ctxt.resize(out_len);
  return ctxt;
}

RSADecryptor::RSADecryptor(const std::string& rsa_private_key) {
  ManagedBIO key_bio{nullptr, bio_deleter};
  key_bio.reset(BIO_new_mem_buf(rsa_private_key.c_str(), -1));
  if (!key_bio) {
    throw std::runtime_error("Failed to create key BIO");
  }
  RSA* priv_key = NULL;
  if (!PEM_read_bio_RSAPrivateKey(key_bio.get(), &priv_key, nullptr, nullptr)) {
    throw std::runtime_error("Failed to create rsa public key");
  }
  pkey_ = EVP_PKEY_new();
  EVP_PKEY_assign_RSA(pkey_, priv_key);
}

RSADecryptor::~RSADecryptor() { EVP_PKEY_free(pkey_); }
std::vector<uint8_t> RSADecryptor::decrypt(const std::vector<uint8_t>& ctxt) const {
  ManagedEPC ctx{nullptr, ctx_deleter};
  ctx.reset(EVP_PKEY_CTX_new(pkey_, NULL));
  if (!ctx) throw std::runtime_error("unable to decrypt data");
  if (EVP_PKEY_decrypt_init(ctx.get()) <= 0) throw std::runtime_error("unable to decrypt data");
  size_t out_len{0};
  if (EVP_PKEY_decrypt(ctx.get(), NULL, &out_len, ctxt.data(), ctxt.size()) <= 0) return {};
  std::vector<uint8_t> ptxt(out_len);
  if (EVP_PKEY_decrypt(ctx.get(), ptxt.data(), &out_len, ctxt.data(), ctxt.size()) <= 0) return {};
  ptxt.resize(out_len);
  return ptxt;
}
}  // namespace libutt