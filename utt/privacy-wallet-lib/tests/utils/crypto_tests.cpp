// UTT Wallet-cli
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include "utils/crypto.hpp"
#include <openssl/pem.h>
#include <openssl/evp.h>
#include <utt/DataUtils.hpp>
using namespace utt::client::utils::crypto;
namespace {
std::string cert =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDazCCAlMCFEHQ6KJVVZyrj5SnSHZQkkRsvWoYMA0GCSqGSIb3DQEBCwUAMHIx\n"
    "CzAJBgNVBAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1Nb3Vu\n"
    "dGFpbiBWaWV3MRMwEQYDVQQKDApNeSBDb21wYW55MQswCQYDVQQLDAJJVDEUMBIG\n"
    "A1UEAwwLZXhhbXBsZS5jb20wHhcNMjMwMTE5MDQ1NTI0WhcNMjQwMTE5MDQ1NTI0\n"
    "WjByMQswCQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwN\n"
    "TW91bnRhaW4gVmlldzETMBEGA1UECgwKTXkgQ29tcGFueTELMAkGA1UECwwCSVQx\n"
    "FDASBgNVBAMMC2V4YW1wbGUuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB\n"
    "CgKCAQEAua5OPJ1GOYJSxCs8cE/6zO2B3pdhdVrzrbII7aONQAUW2+iYb9m2WiPC\n"
    "be26WQyJeZ2WP0uWvC5D+3YdO4fDPcF5FLGXtMGei8B2Pah+gTE0wzlF6vthK4v0\n"
    "B4SoZ2WRgvHj3tr2O8frhWgtryReJZg4D49UR0WRbJ85HLSVq4mrb0BXY58E2Wqi\n"
    "XnfLMQO/aiJ+LFhnnpVOn3Vf8/XTdLav1hM2CIY0P0I9J9SI5qGSo4Tbn3xu8lOr\n"
    "KjtDFnt4BpLW4wMZ1d5OieGePoqq4j8n2mdjhWurLVDESxlytO8Qcs2zTrRDNxrQ\n"
    "O3D6DKbgjKWcJ5Tq5MgjaWtzUy4fvQIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQBw\n"
    "NhgLBjb7MDLRh0zExs7v2xNztDAU7s50wXTZx87EVbGYzJFT4CpYb7igYv23AGHH\n"
    "fxZIYVRvoD0xndfG2zfEIparrFenp9zmlxddnI/CWgmvIpk5/nGAeKCODHesarvy\n"
    "MIyu8HA2ZachTZhfmXmRtJrtYOHQl/5EErPhSDglK5gHQivtS/D6V59mC88fMi3U\n"
    "3y946/RM3Z+X5PTeICXiKR+BSZ4+bRzUDvxMp+rLBozwaetYT3zZdP4QAn9Si+28\n"
    "3lqvsB0uOj606AcfhA+PZgbEZpyU3HOzDNrV7bk6VU0vE7rAp09u1F58wmA81YQr\n"
    "LhsFkWCqmGWU85I7zacS\n"
    "-----END CERTIFICATE-----";
std::string public_key =
    "-----BEGIN PUBLIC KEY-----\n"
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAua5OPJ1GOYJSxCs8cE/6\n"
    "zO2B3pdhdVrzrbII7aONQAUW2+iYb9m2WiPCbe26WQyJeZ2WP0uWvC5D+3YdO4fD\n"
    "PcF5FLGXtMGei8B2Pah+gTE0wzlF6vthK4v0B4SoZ2WRgvHj3tr2O8frhWgtryRe\n"
    "JZg4D49UR0WRbJ85HLSVq4mrb0BXY58E2WqiXnfLMQO/aiJ+LFhnnpVOn3Vf8/XT\n"
    "dLav1hM2CIY0P0I9J9SI5qGSo4Tbn3xu8lOrKjtDFnt4BpLW4wMZ1d5OieGePoqq\n"
    "4j8n2mdjhWurLVDESxlytO8Qcs2zTrRDNxrQO3D6DKbgjKWcJ5Tq5MgjaWtzUy4f\n"
    "vQIDAQAB\n"
    "-----END PUBLIC KEY-----";

std::string private_key =
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC5rk48nUY5glLE\n"
    "KzxwT/rM7YHel2F1WvOtsgjto41ABRbb6Jhv2bZaI8Jt7bpZDIl5nZY/S5a8LkP7\n"
    "dh07h8M9wXkUsZe0wZ6LwHY9qH6BMTTDOUXq+2Eri/QHhKhnZZGC8ePe2vY7x+uF\n"
    "aC2vJF4lmDgPj1RHRZFsnzkctJWriatvQFdjnwTZaqJed8sxA79qIn4sWGeelU6f\n"
    "dV/z9dN0tq/WEzYIhjQ/Qj0n1IjmoZKjhNuffG7yU6sqO0MWe3gGktbjAxnV3k6J\n"
    "4Z4+iqriPyfaZ2OFa6stUMRLGXK07xByzbNOtEM3GtA7cPoMpuCMpZwnlOrkyCNp\n"
    "a3NTLh+9AgMBAAECggEANWXSLA5SprE62h1Q+T+W8Z4P7hJ8vYIVd8suVCDnuxR7\n"
    "mWxPgkMK9Os5u+FU6Mz5MBdIoRU82Qs5E7TI/ViypizghDn6VcokrS4BEwREtSSQ\n"
    "duAeok/+hsZtvEfDIlEMQqsLjAhOLaz1p1zpXmfIB2m6HYdrhj+UbbdwdjfcnwKv\n"
    "5tk87EfKOuuJxxFXP0+HkjcwY0cMLohO/XwK8Rwr/5YXXe4pEDMnk2qoZPmsT7wj\n"
    "6FRUkc96GZIxXaIsb7Dr5KId7RWZGqnCMOa5SKq0MjpKDlZf64XozgjuBy8iJ3VD\n"
    "uuIO/s2PhV0mlcD+sd4yBpcUCyDGxTUtHUZ2sm6YYQKBgQDh7wYSi74UECVQxuM+\n"
    "vBTLKvSRn7niHbVKwD5qaYc3GJBZlU+FXC/3v9KBv+CfJXBs3gNc1oz44ME/9vzk\n"
    "iKSgoK0Yqkl8TFv4pKl5Hc+Y5FpOtHAYjPbKbqcsq/DIRX9/S31x/OsO8Y0jmi7f\n"
    "/5OOdy6SDOzzo6xhsXHV/eh6SQKBgQDSY/kSxOE+/OO6f9zVXJ9tRn2GsngkonyK\n"
    "DFnLPbMmQi1+7xiLKluJaTKjVAoCRDw4/DOcgLM1PGwrcZGo5WCQ05SeDd0GHA8E\n"
    "bcVhFARkvaA5BcuiJeTrjsI0NKpaLbOhEidpm9VQ3XUQAUFjrFMmHk5gcuhgy4HJ\n"
    "ztrLhFVZ1QKBgEh7Z0ZR4JQNLfuBIuxAaKdZS4bgaED7aOrnS97VphRt4/lpZk6R\n"
    "aa4gswb/KK/F0hCLFScWiblaWYUM1sr2b2I8yetszhB7atIU+W2qu6wALlyrlH67\n"
    "0nzVDPrO2ntVmHadIEyOaFat9aqjT0B7fLoq0Bz42pe7PZVF2RBe2dNJAoGAVZ1D\n"
    "JSUi+AvW6/TOO7DmW4R83kxP4bCRd2fRPoiMF3yEoQvQ9Ai3mTJK3fX74LI9w361\n"
    "zfD9fCNrbT5Y5N76rdS7vJmtoKfYYJf+4yNPKmOUCMBX/lLnVggQ9UedLvc8Csal\n"
    "bS9x3edQlMO+BT6B05gvksYP1BvcY/AeTwU56kUCgYA3FknRNCWuHT2IWUh48Mil\n"
    "lxFM5ta4OmSfTQpnX3XBWHF9Y4pOOpmVq8gqTM2H5usDcwj50IN07KJp85YLLttf\n"
    "9fSeBocDjfg08irMKJ7p1jB2sTWBrLK/8PniRHrwRlbsOXPZQDvxG/9hGLmDd7Nt\n"
    "ugW9Dp+NZ6L5qPZtbK/8xQ==\n"
    "-----END PRIVATE KEY-----";
TEST(crypto_tests, test_get_public_key) {
  ASSERT_EQ(public_key, getCertificatePublicKey(cert));
  ASSERT_NO_THROW(libutt::RSAEncryptor({{"id", public_key}}));
}

TEST(crypto_tests, test_sign_and_verify) {
  std::string data = "hello world";
  auto sig = signData(std::vector<uint8_t>(data.begin(), data.end()), private_key);
  ASSERT_GT(sig.size(), 0);

  BIO* key_bio = BIO_new(BIO_s_mem());
  EVP_PKEY* pub_key = NULL;
  BIO_write(key_bio, public_key.c_str(), (int)public_key.size());
  pub_key = PEM_read_bio_PUBKEY(key_bio, NULL, NULL, NULL);

  EVP_MD_CTX* ctx = EVP_MD_CTX_create();
  ASSERT_EQ(EVP_DigestVerifyInit(ctx, nullptr, EVP_sha256(), nullptr, pub_key), 1);
  ASSERT_EQ(EVP_DigestVerifyUpdate(ctx, data.c_str(), (size_t)data.size()), 1);
  ASSERT_EQ(EVP_DigestVerifyFinal(ctx, sig.data(), sig.size()), 1);
  EVP_MD_CTX_destroy(ctx);
  EVP_PKEY_free(pub_key);
  BIO_free(key_bio);
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
