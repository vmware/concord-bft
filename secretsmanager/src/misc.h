// This implementation is copied from CryptoPP: https://www.cryptopp.com/wiki/OPENSSL_EVP_BytesToKey

#pragma once

#include <cryptopp/cryptlib.h>
#include <cryptopp/secblock.h>

const int OPENSSL_PKCS5_SALT_LEN = 8;

// From crypto/evp/evp_key.h. Signature changed a bit to match Crypto++.
inline unsigned int OPENSSL_EVP_BytesToKey(CryptoPP::HashTransformation &hash,
                                           const unsigned char *salt,
                                           const unsigned char *data,
                                           int dlen,
                                           unsigned int count,
                                           unsigned char *key,
                                           unsigned int ksize,
                                           unsigned char *iv,
                                           unsigned int vsize) {
  if (data == NULL) return (0);

  unsigned int nkey = ksize;
  unsigned int niv = vsize;
  unsigned int nhash = hash.DigestSize();
  CryptoPP::SecByteBlock digest(nhash);

  unsigned int addmd = 0, i;

  for (;;) {
    hash.Restart();

    if (addmd++) hash.Update(digest.data(), digest.size());

    hash.Update(data, dlen);

    if (salt != NULL) hash.Update(salt, OPENSSL_PKCS5_SALT_LEN);

    hash.TruncatedFinal(digest.data(), digest.size());

    for (i = 1; i < count; i++) {
      hash.Restart();
      hash.Update(digest.data(), digest.size());
      hash.TruncatedFinal(digest.data(), digest.size());
    }

    i = 0;
    if (nkey) {
      for (;;) {
        if (nkey == 0) break;
        if (i == nhash) break;
        if (key != NULL) *(key++) = digest[i];
        nkey--;
        i++;
      }
    }
    if (niv && (i != nhash)) {
      for (;;) {
        if (niv == 0) break;
        if (i == nhash) break;
        if (iv != NULL) *(iv++) = digest[i];
        niv--;
        i++;
      }
    }
    if ((nkey == 0) && (niv == 0)) break;
  }

  return ksize;
}