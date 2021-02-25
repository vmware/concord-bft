#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#include "gtest/gtest.h"
#include <iostream>

const std::string input{"This is a sample text"};
const std::vector<uint8_t> salt{0x50, 0x54, 0xB7, 0xAF, 0x35, 0xB8, 0xB6, 0xED};
const std::string password{"XaQZrOYEQw"};
const std::string salt_prefix{"Salted__"};
const std::string encrypted{"U2FsdGVkX19QVLevNbi27WbILeleXn5BkWHg3A80UkcEIDFRQPN1ODx/eqZ3UEA4"};

TEST(SecretsManagerPOC, OpenSSL_Encryption) {
  std::vector<uint8_t> key(256 / 8);
  std::vector<uint8_t> iv(16);

  ASSERT_TRUE(EVP_BytesToKey(EVP_aes_256_cbc(),
                             EVP_sha256(),
                             salt.data(),
                             (const unsigned char *)password.c_str(),
                             password.length(),
                             1,
                             key.data(),
                             iv.data()) > 0);

  std::stringstream key_str;
  for (int k : key) {
    key_str << std::setfill('0') << std::setw(2) << std::right << std::hex << k;
  }
  ASSERT_EQ(key_str.str(), "ba502b2803c6c4270b8530b24b9147fad46fe57931410a49920e5058794133c4");

  std::stringstream iv_str;
  for (int i : iv) {
    iv_str << std::setfill('0') << std::setw(2) << std::right << std::hex << i;
  }
  ASSERT_EQ(iv_str.str(), "4a72528785658c36aa7e329e9a65174d");

  // encryption
  {
    EVP_CIPHER_CTX *ctx;
    ctx = EVP_CIPHER_CTX_new();
    // assert iv and key size
    EVP_CipherInit_ex(ctx, EVP_aes_256_cbc(), NULL, key.data(), iv.data(), 1 /* 1 is encryption */);
    ASSERT_EQ(EVP_CIPHER_CTX_key_length(ctx), key.size());
    ASSERT_EQ(EVP_CIPHER_CTX_iv_length(ctx), iv.size());

    // encrypt
    int ct_bufsize = 1024;
    std::vector<uint8_t> ct(ct_bufsize);
    std::vector<uint8_t> ct_final(ct_bufsize);
    ASSERT_EQ(EVP_CipherUpdate(ctx, ct.data(), &ct_bufsize, (const unsigned char *)input.data(), input.size()), 1);
    ASSERT_GE(ct.size(), ct_bufsize);
    ct.resize(ct_bufsize);
    EVP_CipherFinal_ex(ctx, ct_final.data(), &ct_bufsize);
    ct_final.resize(ct_bufsize);

    // concatenate salt prefix + salt + ciphertext
    std::vector<uint8_t> out;
    out.reserve(salt_prefix.length() + salt.size() + ct.size() + ct_final.size());
    out.insert(out.end(), salt_prefix.begin(), salt_prefix.end());
    out.insert(out.end(), salt.begin(), salt.end());
    out.insert(out.end(), ct.begin(), ct.end());
    out.insert(out.end(), ct_final.begin(), ct_final.end());
    ASSERT_EQ(out.size(), salt_prefix.length() + salt.size() + ct.size() + ct_final.size());

    // base64 encode
    const uint32_t expected_len = 4 * ((out.size() + 2) / 3);
    // std::vector<uint8_t> output(expected_len + 1);
    std::string output(expected_len + 1, '0');
    const uint32_t output_len = EVP_EncodeBlock((unsigned char *)output.data(), out.data(), out.size());
    ASSERT_GE(output.size(), output_len);
    output.resize(output_len);

    ASSERT_EQ(encrypted, output);
  }
}