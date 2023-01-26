// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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

#include "log/logger.hpp"
#include "crypto/openssl/certificates.hpp"

namespace {
using namespace concord::crypto;

std::string ca_cert =
    "-----BEGIN CERTIFICATE-----\n"
    "MIICmDCCAgECFAzoTmCj+eMLIiCT+l6NFaExqUgwMA0GCSqGSIb3DQEBCwUAMIGJ\n"
    "MQswCQYDVQQGEwJOWTELMAkGA1UECAwCTlkxDDAKBgNVBAcMA05ZQzETMBEGA1UE\n"
    "CgwKYmxvY2tjaGlhbjETMBEGA1UECwwKYmxvY2tjaGFpbjEPMA0GA1UEAwwGcm9v\n"
    "dGNhMSQwIgYJKoZIhvcNAQkBFhVyb290Y2FAYmxvY2tjaGFpbi5jb20wIBcNMjMw\n"
    "MTE1MTY1NjMxWhgPMjEyMjEyMjIxNjU2MzFaMIGJMQswCQYDVQQGEwJOWTELMAkG\n"
    "A1UECAwCTlkxDDAKBgNVBAcMA05ZQzETMBEGA1UECgwKYmxvY2tjaGlhbjETMBEG\n"
    "A1UECwwKYmxvY2tjaGFpbjEPMA0GA1UEAwwGcm9vdGNhMSQwIgYJKoZIhvcNAQkB\n"
    "FhVyb290Y2FAYmxvY2tjaGFpbi5jb20wgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJ\n"
    "AoGBAJn9W4WHYSiY7NELH/qXhduE5zhckO/lObfQcTZOEH+fPj52HYN/rFa+j18c\n"
    "td0nZbP2p2iMAuqDrwjZQxX6j6btE+cF/DDyrZRqneqDTT8wJuDEXHnOyS1P8HIj\n"
    "LN3pqLE//OL/bX5XVxaYYlU6zpWIJtpxA4x9wL0kQFcbv5evAgMBAAEwDQYJKoZI\n"
    "hvcNAQELBQADgYEAiwJoL7ECrY83JQ6hv5jjSIpzspmGNdRfjl0jYDJGAtc9jfYs\n"
    "rDDQQ1xCdqHCTFwYpf4pyD3wN25nDHDkDjD9RUI1ZeiGX/DySdWydLsnJ0JsK4j/\n"
    "Sl6Hw9mvdc7CBJqqYmO5sYB2PZPbKQsz6gVnpvsi3eHLMfbpECr9Egb5nxs=\n"
    "-----END CERTIFICATE-----";

std::string issuer_cert =
    "-----BEGIN CERTIFICATE-----\n"
    "MIICfTCCAeYCAQEwDQYJKoZIhvcNAQELBQAwgYkxCzAJBgNVBAYTAk5ZMQswCQYD\n"
    "VQQIDAJOWTEMMAoGA1UEBwwDTllDMRMwEQYDVQQKDApibG9ja2NoaWFuMRMwEQYD\n"
    "VQQLDApibG9ja2NoYWluMQ8wDQYDVQQDDAZyb290Y2ExJDAiBgkqhkiG9w0BCQEW\n"
    "FXJvb3RjYUBibG9ja2NoYWluLmNvbTAgFw0yMzAxMTUxNjU3MzlaGA8yMTIyMTIy\n"
    "MjE2NTczOVowgYExCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOWTEMMAoGA1UEBwwD\n"
    "TllDMRMwEQYDVQQKDApibG9ja2NoYWluMRMwEQYDVQQLDApibG9ja2NoYWluMQsw\n"
    "CQYDVQQDDAJsMTEgMB4GCSqGSIb3DQEJARYRbDFAYmxvY2tjaGFpbi5jb20wgZ8w\n"
    "DQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAPVnaz7YAXednY3cLWGuKKLNLuz0C9oK\n"
    "LhaJiaQf/qCM+xwQQLAuQeLcojwLI3J2byyjEXGYR3nn2eCmuAiVujrpBiyOs/WP\n"
    "Sf+pYvdSpBeOMzIOHGVdiVdd/d8n67bsXu96BOjWZtunwU6l3yBxKHTofP7YgRqj\n"
    "+zznYn0eFQfFAgMBAAEwDQYJKoZIhvcNAQELBQADgYEARz0NpS8U+m9JC5Al2vI2\n"
    "Z/2TfHl7qpvKdUqf18hfv7B7cA0bdzjN0sVmzfaEZludyBuzxwsjavRW0gldwxnW\n"
    "0Xve2Q1MCMQYTWgLBw1zNOQKdtJXwuuIJcxuf6CTzOVGJer60CCzdLh24yPmCjI6\n"
    "T22xO0WBEgL8jnW79YKP2u4=\n"
    "-----END CERTIFICATE-----";

std::string client_cert =
    "-----BEGIN CERTIFICATE-----\n"
    "MIICcDCCAdkCAQIwDQYJKoZIhvcNAQELBQAwgYExCzAJBgNVBAYTAlVTMQswCQYD\n"
    "VQQIDAJOWTEMMAoGA1UEBwwDTllDMRMwEQYDVQQKDApibG9ja2NoYWluMRMwEQYD\n"
    "VQQLDApibG9ja2NoYWluMQswCQYDVQQDDAJsMTEgMB4GCSqGSIb3DQEJARYRbDFA\n"
    "YmxvY2tjaGFpbi5jb20wIBcNMjMwMTE1MTY1ODQ5WhgPMjEyMjEyMjIxNjU4NDla\n"
    "MH0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOWTEMMAoGA1UEBwwDTllDMRMwEQYD\n"
    "VQQKDApibG9ja2NoYWluMRMwEQYDVQQLDApibG9ja2NoYWluMQswCQYDVQQDDAJs\n"
    "MjEcMBoGCSqGSIb3DQEJARYNbDJAYmxvY2tjaGFpbjCBnzANBgkqhkiG9w0BAQEF\n"
    "AAOBjQAwgYkCgYEAoCruqa/GELZzchsgZLTRFunlwpzPJjl/+OHgnHlFUkyYxSsQ\n"
    "irTkY2EP9ShF0K9krTdbEJBRTNNCwtDDI2wMEzFk8Oz4Cwi19fEKilxv4yvwvvh2\n"
    "7Eq5+ko8KnMDY6PU7UXqP1chaLPtlVvDXNmfS578LdxinU2f7bn21Q8cLN8CAwEA\n"
    "ATANBgkqhkiG9w0BAQsFAAOBgQCHDYppZAQk4tPRMmYB94tg8BaVcBlmgruRthFk\n"
    "4CsUoBVa4Wz695wOoqkD5li42+h9ZO8e8gHQAd2kCyffXuWE8FpbzFcbjZMj2P4V\n"
    "LXGVcnIcbHqAgNWzgsKTL5bk5vLbDyCnvEC5ZXhReOAD4CvaIdX8nttyy2ZJ1nq/\n"
    "RbYxlQ==\n"
    "-----END CERTIFICATE-----";

std::string client_public_key =
    "-----BEGIN PUBLIC KEY-----\n"
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCgKu6pr8YQtnNyGyBktNEW6eXC\n"
    "nM8mOX/44eCceUVSTJjFKxCKtORjYQ/1KEXQr2StN1sQkFFM00LC0MMjbAwTMWTw\n"
    "7PgLCLX18QqKXG/jK/C++HbsSrn6SjwqcwNjo9TtReo/VyFos+2VW8Nc2Z9Lnvwt\n"
    "3GKdTZ/tufbVDxws3wIDAQAB\n"
    "-----END PUBLIC KEY-----";

TEST(test_certificates_utils, test_self_signed_correctness) { ASSERT_TRUE(verifyCertificatesChain(ca_cert)); }

TEST(test_certificates_utils, test_ca_issuer_correctness) {
  ASSERT_TRUE(verifyCertificatesChain(issuer_cert + "\n" + ca_cert));
}

TEST(test_certificates_utils, test_issuer_client_correctness) {
  ASSERT_TRUE(verifyCertificatesChain(client_cert + "\n" + issuer_cert));
}

TEST(test_certificates_utils, test_complete_cert_chain_correctness) {
  ASSERT_TRUE(verifyCertificatesChain(client_cert + "\n" + issuer_cert + "\n" + ca_cert));
}

TEST(test_certificates_utils, test_incorrect_chain) {
  ASSERT_FALSE(verifyCertificatesChain(issuer_cert + "\n" + client_cert + "\n" + ca_cert));
}

TEST(test_certificates_utils, test_incorrect_self_signed_issuer) { ASSERT_FALSE(verifyCertificatesChain(issuer_cert)); }

TEST(test_certificates_utils, test_incorrect_self_signed_client) { ASSERT_FALSE(verifyCertificatesChain(client_cert)); }

TEST(test_certificates_utils, test_get_public_key_0_index) {
  ASSERT_EQ(getCertificatePublicKey(client_cert, 0), client_public_key);
}

TEST(test_certificates_utils, test_get_public_key_1_index) {
  ASSERT_EQ(getCertificatePublicKey(issuer_cert + "\n" + client_cert, 1), client_public_key);
}

TEST(test_certificates_utils, out_of_boundaries) { ASSERT_ANY_THROW(getCertificatePublicKey(client_cert, 1)); }

TEST(test_certificates_utils, test_get_subject_fields) {
  std::map<std::string, std::string> expected_subject{
      {"C", "US"}, {"CN", "l2"}, {"L", "NYC"}, {"ST", "NY"}, {"O", "blockchain"}, {"OU", "blockchain"}};
  auto subject = getSubjectFields(client_cert, 0);
  for (const auto& [attr, attr_val] : subject) ASSERT_EQ(expected_subject[attr], attr_val);
}

}  // namespace

int main(int argc, char** argv) {
  logging::initLogger("logging.properties");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
