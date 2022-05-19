// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//

#include "gtest/gtest.h"
#include <iterator>
#include <fstream>
#include <util/filesystem.hpp>
#include "crypto_utils.hpp"
#include "Logger.hpp"
using namespace concord::util::crypto;
namespace {
TEST(crypto_utils, generate_rsa_keys_hex_format) {
  ASSERT_NO_THROW(Crypto::instance().generateRsaKeyPair(2048, KeyFormat::HexaDecimalStrippedFormat));
  auto keys = Crypto::instance().generateRsaKeyPair(2048, KeyFormat::HexaDecimalStrippedFormat);
  LOG_INFO(GL, keys.first << " | " << keys.second);
}

TEST(crypto_utils, generate_rsa_keys_pem_format) {
  ASSERT_NO_THROW(Crypto::instance().generateRsaKeyPair(2048, KeyFormat::PemFormat));
  auto keys = Crypto::instance().generateRsaKeyPair(2048, KeyFormat::PemFormat);
  LOG_INFO(GL, keys.first << " | " << keys.second);
}

TEST(crypto_utils, generate_ECDSA_keys_pem_format) {
  ASSERT_NO_THROW(Crypto::instance().generateECDSAKeyPair(KeyFormat::PemFormat));
  auto keys = Crypto::instance().generateECDSAKeyPair(KeyFormat::PemFormat);
  LOG_INFO(GL, keys.first << " | " << keys.second);
}

TEST(crypto_utils, generate_ECDSA_keys_hex_format) {
  ASSERT_NO_THROW(Crypto::instance().generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat));
  auto keys = Crypto::instance().generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  LOG_INFO(GL, keys.first << " | " << keys.second);
}

TEST(crypto_utils, test_rsa_keys_hex) {
  auto keys = Crypto::instance().generateRsaKeyPair(2048, KeyFormat::HexaDecimalStrippedFormat);
  RSASigner signer(keys.first, KeyFormat::HexaDecimalStrippedFormat);
  RSAVerifier verifier(keys.second, KeyFormat::HexaDecimalStrippedFormat);
  std::string data = "Hello world";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(crypto_utils, test_rsa_keys_pem) {
  auto keys = Crypto::instance().generateRsaKeyPair(2048, KeyFormat::PemFormat);
  RSASigner signer(keys.first, KeyFormat::PemFormat);
  RSAVerifier verifier(keys.second, KeyFormat::PemFormat);
  std::string data = "Hello world";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(crypto_utils, test_rsa_keys_combined_a) {
  auto keys = Crypto::instance().generateRsaKeyPair(2048, KeyFormat::HexaDecimalStrippedFormat);
  auto pemKeys = Crypto::instance().RsaHexToPem(keys);
  RSASigner signer(keys.first, KeyFormat::HexaDecimalStrippedFormat);
  RSAVerifier verifier(pemKeys.second, KeyFormat::PemFormat);
  std::string data = "Hello world";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(crypto_utils, test_rsa_keys_combined_b) {
  auto keys = Crypto::instance().generateRsaKeyPair(2048, KeyFormat::HexaDecimalStrippedFormat);
  auto pemKeys = Crypto::instance().RsaHexToPem(keys);
  RSASigner signer(pemKeys.first, KeyFormat::PemFormat);
  RSAVerifier verifier(keys.second, KeyFormat::HexaDecimalStrippedFormat);
  std::string data = "Hello world";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(crypto_utils, test_ecdsa_keys_hex) {
  auto keys = Crypto::instance().generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  ECDSASigner signer(keys.first, KeyFormat::HexaDecimalStrippedFormat);
  ECDSAVerifier verifier(keys.second, KeyFormat::HexaDecimalStrippedFormat);
  std::string data = "Hello world";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(crypto_utils, test_ecdsa_keys_pem) {
  auto keys = Crypto::instance().generateECDSAKeyPair(KeyFormat::PemFormat);
  ECDSASigner signer(keys.first, KeyFormat::PemFormat);
  ECDSAVerifier verifier(keys.second, KeyFormat::PemFormat);
  std::string data = "Hello world";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(crypto_utils, test_ecdsa_keys_pem_combined_a) {
  auto keys = Crypto::instance().generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  auto pemKeys = Crypto::instance().ECDSAHexToPem(keys);
  ECDSASigner signer(keys.first, KeyFormat::HexaDecimalStrippedFormat);
  ECDSAVerifier verifier(pemKeys.second, KeyFormat::PemFormat);
  std::string data = "Hello world";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(crypto_utils, test_ecdsa_keys_pem_combined_b) {
  auto keys = Crypto::instance().generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  auto pemKeys = Crypto::instance().ECDSAHexToPem(keys);
  ECDSASigner signer(pemKeys.first, KeyFormat::PemFormat);
  ECDSAVerifier verifier(keys.second, KeyFormat::HexaDecimalStrippedFormat);
  std::string data = "Hello world";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}
TEST(crypto_utils, test_get_subject_field_by_name) {
  const char* cert_buff =
      "-----BEGIN CERTIFICATE-----\n"
      "MIIDazCCAvGgAwIBAgIVANDQpFnRXNbNQucybAYeEr8ML2tQMAoGCCqGSM49BAMC\n"
      "MHsxCzAJBgNVBAYTAk5BMQswCQYDVQQIDAJOQTELMAkGA1UEBwwCTkExCzAJBgNV\n"
      "BAoMAk5BMS4wLAYDVQQLDCVwNjFlNmMwZDhfMjg2YV80OGFmX2FkYjlfY2ViZjM5\n"
      "NzkxYjE4MRUwEwYDVQQDDAwxMC4yMDIuNjkuNTIwHhcNMjIwMzIxMDYxMzU1WhcN\n"
      "MjUwMzIxMDYxMzU1WjB7MQswCQYDVQQGEwJOQTELMAkGA1UECAwCTkExCzAJBgNV\n"
      "BAcMAk5BMQswCQYDVQQKDAJOQTEuMCwGA1UECwwlcDYxZTZjMGQ4XzI4NmFfNDhh\n"
      "Zl9hZGI5X2NlYmYzOTc5MWIxODEVMBMGA1UEAwwMMTAuMjAyLjY5LjUyMHYwEAYH\n"
      "KoZIzj0CAQYFK4EEACIDYgAE0e1ApCMI3MGzd6uWPphlS7TTrz0iSHbgwKnluhDu\n"
      "4qB5tFuPdxtjz3r+nt4bH6JNvuzxVb6T98fGs0i6KQQIK2K7ZLpzqGpGVsvkX0MG\n"
      "v3YaczQL8dgWHr669j+WUaJ+o4IBMzCCAS8wgYEGA1UdDgR6BHgwdjAQBgcqhkjO\n"
      "PQIBBgUrgQQAIgNiAATR7UCkIwjcwbN3q5Y+mGVLtNOvPSJIduDAqeW6EO7ioHm0\n"
      "W493G2PPev6e3hsfok2+7PFVvpP3x8azSLopBAgrYrtkunOoakZWy+RfQwa/dhpz\n"
      "NAvx2BYevrr2P5ZRon4wgYMGA1UdIwR8MHqAeDB2MBAGByqGSM49AgEGBSuBBAAi\n"
      "A2IABNHtQKQjCNzBs3erlj6YZUu00689Ikh24MCp5boQ7uKgebRbj3cbY896/p7e\n"
      "Gx+iTb7s8VW+k/fHxrNIuikECCtiu2S6c6hqRlbL5F9DBr92GnM0C/HYFh6+uvY/\n"
      "llGifjAPBgNVHRMBAf8EBTADAQH/MBIGA1UdEQEB/wQIMAaHBArKRTQwCgYIKoZI\n"
      "zj0EAwIDaAAwZQIxALx5mkzfjTB722cGew9XvPA5bdo99Rzwocm1CzQ7Rtiygyd3\n"
      "1UFlrk3+ZXA/jMbBUAIwdRkurXvEu9WYTv8wB+iJQOOXgRQbi2Q4RhJIxBwMB+mw\n"
      "QYzq1al1kVKZ0vaZqaKx\n"
      "-----END CERTIFICATE-----\n";
  /*
   * # command to get the subject from cert:
   * openssl x509 -noout -subject -in test.cert
   * sample output: "subject=C = NA, ST = NA, L = NA, O = NA, OU = p61e6c0d8_286a_48af_adb9_cebf39791b18, CN
   * = 10.202.69.52"
   */
  auto expected_val = "p61e6c0d8_286a_48af_adb9_cebf39791b18";
  std::string cert_path = "./test_cert.pem";
  std::ofstream FILE(cert_path, std::ios::out | std::ofstream::binary);
  std::string_view s{cert_buff};
  std::copy(s.begin(), s.end(), std::ostreambuf_iterator<char>(FILE));
  FILE.close();
  auto field_val = CertificateUtils::getSubjectFieldByName(cert_path, "OU");
  fs::remove(cert_path);
  ASSERT_TRUE(field_val == expected_val);
}

TEST(crypto_utils, test_get_subject_field_list_by_name) {
  const char* cert_bundle_buff =
      "-----BEGIN CERTIFICATE-----\n"
      "MIIDazCCAvGgAwIBAgIVAMW6ZNtBiV31vmGdu4UC+VLAUphXMAoGCCqGSM49BAMC\n"
      "MHsxCzAJBgNVBAYTAk5BMQswCQYDVQQIDAJOQTELMAkGA1UEBwwCTkExCzAJBgNV\n"
      "BAoMAk5BMS0wKwYDVQQLDCQyZGM1ZWMwOS1jYWI1LTQyNTQtOWNkNy1jZWY2NWUy\n"
      "NzlhNzUxFjAUBgNVBAMMDTEwLjIwMi42OC4xMDUwHhcNMjIwNTExMTEyNTE2WhcN\n"
      "MjUwNTExMTEyNTE2WjB7MQswCQYDVQQGEwJOQTELMAkGA1UECAwCTkExCzAJBgNV\n"
      "BAcMAk5BMQswCQYDVQQKDAJOQTEtMCsGA1UECwwkMmRjNWVjMDktY2FiNS00MjU0\n"
      "LTljZDctY2VmNjVlMjc5YTc1MRYwFAYDVQQDDA0xMC4yMDIuNjguMTA1MHYwEAYH\n"
      "KoZIzj0CAQYFK4EEACIDYgAEHwX7LKIdbK6sFe89jyimNL7+vJTdhXOjDXpoc5aw\n"
      "2lGvHBzyx5ldzcXWUZZqfuv5ESkkdDS9ljbTZeSg73groyN2BCSOHFUGp78qd1Bb\n"
      "9L/3K8IvjUdkEWs+frTj/4K4o4IBMzCCAS8wgYEGA1UdDgR6BHgwdjAQBgcqhkjO\n"
      "PQIBBgUrgQQAIgNiAAQfBfssoh1srqwV7z2PKKY0vv68lN2Fc6MNemhzlrDaUa8c\n"
      "HPLHmV3NxdZRlmp+6/kRKSR0NL2WNtNl5KDveCujI3YEJI4cVQanvyp3UFv0v/cr\n"
      "wi+NR2QRaz5+tOP/grgwgYMGA1UdIwR8MHqAeDB2MBAGByqGSM49AgEGBSuBBAAi\n"
      "A2IABB8F+yyiHWyurBXvPY8opjS+/ryU3YVzow16aHOWsNpRrxwc8seZXc3F1lGW\n"
      "an7r+REpJHQ0vZY202XkoO94K6MjdgQkjhxVBqe/KndQW/S/9yvCL41HZBFrPn60\n"
      "4/+CuDAPBgNVHRMBAf8EBTADAQH/MBIGA1UdEQEB/wQIMAaHBArKRGkwCgYIKoZI\n"
      "zj0EAwIDaAAwZQIwO5FMHvsf55FcXkvYcaMcm8Qbw5Va8aJwrZ0GWs/oqpV5+u5F\n"
      "HRODyYrgRZ+ePqEFAjEAgyiaClVYdx+BeepEKDZzjOFZ9HSjCmRERA+u3wrIrSnV\n"
      "GkYBKnHob2fBuUOmNi+3\n"
      "-----END CERTIFICATE-----\n"
      "-----BEGIN CERTIFICATE-----\n"
      "MIIDaDCCAu6gAwIBAgIUc22vyLSr69VqKEQIRN1RSTa/aGcwCgYIKoZIzj0EAwIw\n"
      "ejELMAkGA1UEBhMCTkExCzAJBgNVBAgMAk5BMQswCQYDVQQHDAJOQTELMAkGA1UE\n"
      "CgwCTkExLTArBgNVBAsMJDBhMDVkYmY2LTYxN2MtNGFjNy1hZTZmLTNmMjM2NjE3\n"
      "YzE3ODEVMBMGA1UEAwwMMTAuMjAyLjY4Ljk4MB4XDTIyMDUxMTExMjUxNloXDTI1\n"
      "MDUxMTExMjUxNlowejELMAkGA1UEBhMCTkExCzAJBgNVBAgMAk5BMQswCQYDVQQH\n"
      "DAJOQTELMAkGA1UECgwCTkExLTArBgNVBAsMJDBhMDVkYmY2LTYxN2MtNGFjNy1h\n"
      "ZTZmLTNmMjM2NjE3YzE3ODEVMBMGA1UEAwwMMTAuMjAyLjY4Ljk4MHYwEAYHKoZI\n"
      "zj0CAQYFK4EEACIDYgAEyr+VkrJYle/ODqJbUEHfzqFKrfc8QvukahyofySnbQR2\n"
      "weRVaJifWnRGyEAeXx8R+F72TsdStxgLGDOKWBAVYmZu35keFhFdCsFlkcK+YzaM\n"
      "mhFZbeKX2963e1+AqHfCo4IBMzCCAS8wgYEGA1UdDgR6BHgwdjAQBgcqhkjOPQIB\n"
      "BgUrgQQAIgNiAATKv5WSsliV784OoltQQd/OoUqt9zxC+6RqHKh/JKdtBHbB5FVo\n"
      "mJ9adEbIQB5fHxH4XvZOx1K3GAsYM4pYEBViZm7fmR4WEV0KwWWRwr5jNoyaEVlt\n"
      "4pfb3rd7X4Cod8IwgYMGA1UdIwR8MHqAeDB2MBAGByqGSM49AgEGBSuBBAAiA2IA\n"
      "BMq/lZKyWJXvzg6iW1BB386hSq33PEL7pGocqH8kp20EdsHkVWiYn1p0RshAHl8f\n"
      "Efhe9k7HUrcYCxgzilgQFWJmbt+ZHhYRXQrBZZHCvmM2jJoRWW3il9vet3tfgKh3\n"
      "wjAPBgNVHRMBAf8EBTADAQH/MBIGA1UdEQEB/wQIMAaHBArKRGIwCgYIKoZIzj0E\n"
      "AwIDaAAwZQIxAJM6NrI9Q6voEO+Qhrw1Ni6qFrmrLj/QUKm8aV4ZG1cxpzB2+Rtm\n"
      "22BNOhwv5bbHrQIwCDTcPWxwwJdMtbwKbPRhNlA+NffDnEZwRf4P2rjWUHT4zXB2\n"
      "Vc7q42pSkNkweva7\n"
      "-----END CERTIFICATE-----\n"
      "-----BEGIN CERTIFICATE-----\n"
      "MIIDaTCCAu+gAwIBAgIVAPuItCvsMcAT4DuY8Dd7LJY2s5cZMAoGCCqGSM49BAMC\n"
      "MHoxCzAJBgNVBAYTAk5BMQswCQYDVQQIDAJOQTELMAkGA1UEBwwCTkExCzAJBgNV\n"
      "BAoMAk5BMS0wKwYDVQQLDCQ1ZmM4M2I2MC1kOTFjLTRmOWUtYTFiYi04YjA5MDEx\n"
      "OTZjZTIxFTATBgNVBAMMDDEwLjIwMi42OC45OTAeFw0yMjA1MTExMTI1MTZaFw0y\n"
      "NTA1MTExMTI1MTZaMHoxCzAJBgNVBAYTAk5BMQswCQYDVQQIDAJOQTELMAkGA1UE\n"
      "BwwCTkExCzAJBgNVBAoMAk5BMS0wKwYDVQQLDCQ1ZmM4M2I2MC1kOTFjLTRmOWUt\n"
      "YTFiYi04YjA5MDExOTZjZTIxFTATBgNVBAMMDDEwLjIwMi42OC45OTB2MBAGByqG\n"
      "SM49AgEGBSuBBAAiA2IABAmcOdlXyXgtmwO3uUOe1FqiyuXtTjVGHaqRszsZuB0V\n"
      "bZgLMlG3h/6lEPEEWOWK+Hgw/5wTW4AhrGQMD3cFZnEgvsXMdNg+j1oSkz5XzXxG\n"
      "XtUzfOwe2MpUQYWnmg0GoKOCATMwggEvMIGBBgNVHQ4EegR4MHYwEAYHKoZIzj0C\n"
      "AQYFK4EEACIDYgAECZw52VfJeC2bA7e5Q57UWqLK5e1ONUYdqpGzOxm4HRVtmAsy\n"
      "UbeH/qUQ8QRY5Yr4eDD/nBNbgCGsZAwPdwVmcSC+xcx02D6PWhKTPlfNfEZe1TN8\n"
      "7B7YylRBhaeaDQagMIGDBgNVHSMEfDB6gHgwdjAQBgcqhkjOPQIBBgUrgQQAIgNi\n"
      "AAQJnDnZV8l4LZsDt7lDntRaosrl7U41Rh2qkbM7GbgdFW2YCzJRt4f+pRDxBFjl\n"
      "ivh4MP+cE1uAIaxkDA93BWZxIL7FzHTYPo9aEpM+V818Rl7VM3zsHtjKVEGFp5oN\n"
      "BqAwDwYDVR0TAQH/BAUwAwEB/zASBgNVHREBAf8ECDAGhwQKykRjMAoGCCqGSM49\n"
      "BAMCA2gAMGUCMQD708wpRfsSyAi7CO25Le65qZYv+Rq1tMQQ8NKo0G/wGnZ61Cij\n"
      "F46LDp9kU3za26YCMDzFwxVnLeE4axZQHBr0DtTNl78hK0nkkZXGstXzzPKuvPKK\n"
      "Ics8ac7ZqplwGOhUHw==\n"
      "-----END CERTIFICATE-----\n"
      "-----BEGIN CERTIFICATE-----\n"
      "MIIDaDCCAu6gAwIBAgIUKQMCUaXS7YKH7RZfXYku6VMR6PkwCgYIKoZIzj0EAwIw\n"
      "ejELMAkGA1UEBhMCTkExCzAJBgNVBAgMAk5BMQswCQYDVQQHDAJOQTELMAkGA1UE\n"
      "CgwCTkExLTArBgNVBAsMJGE4NjY3ZTNhLWU4ZWEtNGM0Ni05OGM0LTFiNmY0Njdl\n"
      "NzE0YjEVMBMGA1UEAwwMMTAuMjAyLjY4LjU1MB4XDTIyMDUxMTExMjUxNloXDTI1\n"
      "MDUxMTExMjUxNlowejELMAkGA1UEBhMCTkExCzAJBgNVBAgMAk5BMQswCQYDVQQH\n"
      "DAJOQTELMAkGA1UECgwCTkExLTArBgNVBAsMJGE4NjY3ZTNhLWU4ZWEtNGM0Ni05\n"
      "OGM0LTFiNmY0NjdlNzE0YjEVMBMGA1UEAwwMMTAuMjAyLjY4LjU1MHYwEAYHKoZI\n"
      "zj0CAQYFK4EEACIDYgAELvDwrU0LqHdDQjpBwsuVNM+MSuMlsqPjHkoDjhJlz/D5\n"
      "mrYEoBw3u8gSQgidxggMprV/vaeT8f957ducx29rQsAGsq7Dj+EWoxs76DWa9kQQ\n"
      "K4vx2752c8DwtGczdmQ5o4IBMzCCAS8wgYEGA1UdDgR6BHgwdjAQBgcqhkjOPQIB\n"
      "BgUrgQQAIgNiAAQu8PCtTQuod0NCOkHCy5U0z4xK4yWyo+MeSgOOEmXP8PmatgSg\n"
      "HDe7yBJCCJ3GCAymtX+9p5Px/3nt25zHb2tCwAayrsOP4RajGzvoNZr2RBAri/Hb\n"
      "vnZzwPC0ZzN2ZDkwgYMGA1UdIwR8MHqAeDB2MBAGByqGSM49AgEGBSuBBAAiA2IA\n"
      "BC7w8K1NC6h3Q0I6QcLLlTTPjErjJbKj4x5KA44SZc/w+Zq2BKAcN7vIEkIIncYI\n"
      "DKa1f72nk/H/ee3bnMdva0LABrKuw4/hFqMbO+g1mvZEECuL8du+dnPA8LRnM3Zk\n"
      "OTAPBgNVHRMBAf8EBTADAQH/MBIGA1UdEQEB/wQIMAaHBArKRDcwCgYIKoZIzj0E\n"
      "AwIDaAAwZQIxANTd53YjBLh0+Pn2DrtntNo/GJ8UvoLQu8jwBUTTOMSTelcDWTFb\n"
      "TihwwaqsCcyoGwIwYmq5wiW/wcCDyRc2imX+7WcEysUypmXsHndK5SSQeL/YhxOQ\n"
      "Tq6P8fksSvYwF3mG\n"
      "-----END CERTIFICATE-----\n";
  /*
   * # command to get the subject from cert bundle:
   * openssl crl2pkcs7 -nocrl -certfile server.cert | openssl pkcs7 -print_certs -noout | grep subject | grep .
   * samle output:
   *     subject=C = NA, ST = NA, L = NA, O = NA, OU = 2dc5ec09-cab5-4254-9cd7-cef65e279a75, CN = 10.202.68.105
   *     subject=C = NA, ST = NA, L = NA, O = NA, OU = 0a05dbf6-617c-4ac7-ae6f-3f236617c178, CN = 10.202.68.98
   *     subject=C = NA, ST = NA, L = NA, O = NA, OU = 5fc83b60-d91c-4f9e-a1bb-8b0901196ce2, CN = 10.202.68.99
   *     subject=C = NA, ST = NA, L = NA, O = NA, OU = a8667e3a-e8ea-4c46-98c4-1b6f467e714b, CN = 10.202.68.55
   */
  auto expected_val = std::set<std::string>{{"2dc5ec09-cab5-4254-9cd7-cef65e279a75"},
                                            {"0a05dbf6-617c-4ac7-ae6f-3f236617c178"},
                                            {"5fc83b60-d91c-4f9e-a1bb-8b0901196ce2"},
                                            {"a8667e3a-e8ea-4c46-98c4-1b6f467e714b"}};
  std::string cert_path = "./test_cert.pem";
  std::ofstream FILE(cert_path, std::ios::out | std::ofstream::binary);
  std::string_view s{cert_bundle_buff};
  std::copy(s.begin(), s.end(), std::ostreambuf_iterator<char>(FILE));
  FILE.close();
  auto field_val = CertificateUtils::getSubjectFieldListByName(cert_path, "OU");
  auto out_val = std::set<std::string>{};
  for (auto& val : field_val) {
    out_val.emplace(std::move(val));
  }
  fs::remove(cert_path);
  ASSERT_TRUE(out_val == expected_val);
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
