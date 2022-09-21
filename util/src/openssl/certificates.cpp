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
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "crypto/openssl/certificates.hpp"
#include "Logger.hpp"
#include "util/filesystem.hpp"

#include <regex>

namespace concord::crypto {
using std::unique_ptr;
using std::string;
using concord::crypto::SignatureAlgorithm;
using concord::crypto::openssl::UniquePKEY;
using concord::crypto::openssl::UniqueX509;
using concord::crypto::openssl::UniqueBIO;
using concord::crypto::openssl::OPENSSL_SUCCESS;
using concord::crypto::openssl::OPENSSL_FAILURE;
using concord::crypto::openssl::OPENSSL_ERROR;

const std::unordered_map<std::string, int> name_to_id_map = {{"C", NID_countryName},
                                                             {"L", NID_localityName},
                                                             {"ST", NID_stateOrProvinceName},
                                                             {"O", NID_organizationName},
                                                             {"OU", NID_organizationalUnitName},
                                                             {"CN", NID_commonName}};

string generateSelfSignedCert(const string& origin_cert_path,
                              const string& public_key,
                              const string& signing_key,
                              const SignatureAlgorithm signingAlgo) {
  unique_ptr<FILE, decltype(&fclose)> fp(fopen(origin_cert_path.c_str(), "r"), fclose);
  if (nullptr == fp) {
    LOG_ERROR(OPENSSL_LOG, "Certificate file not found, path: " << origin_cert_path);
    return string();
  }

  UniqueX509 cert(PEM_read_X509(fp.get(), nullptr, nullptr, nullptr));
  if (nullptr == cert) {
    LOG_ERROR(OPENSSL_LOG, "Cannot parse certificate, path: " << origin_cert_path);
    return string();
  }

  UniquePKEY priv_key(EVP_PKEY_new());
  UniqueBIO priv_bio(BIO_new(BIO_s_mem()));

  if (BIO_write(priv_bio.get(), static_cast<const char*>(signing_key.c_str()), signing_key.size()) <= 0) {
    LOG_ERROR(OPENSSL_LOG, "Unable to create private key object");
    return string();
  }

  if (nullptr == PEM_read_bio_PrivateKey(priv_bio.get(), reinterpret_cast<EVP_PKEY**>(&priv_key), nullptr, nullptr)) {
    LOG_ERROR(OPENSSL_LOG, "Unable to create private key object");
    return string();
  }
  UniquePKEY pub_key(EVP_PKEY_new());
  UniqueBIO pub_bio(BIO_new(BIO_s_mem()));

  if (BIO_write(pub_bio.get(), static_cast<const char*>(public_key.c_str()), public_key.size()) <= 0) {
    LOG_ERROR(OPENSSL_LOG, "Unable to write public key object");
    return string();
  }
  if (nullptr == PEM_read_bio_PUBKEY(pub_bio.get(), reinterpret_cast<EVP_PKEY**>(&pub_key), nullptr, nullptr)) {
    LOG_ERROR(OPENSSL_LOG, "Unable to create public key object");
    return string();
  }

  if (OPENSSL_FAILURE == X509_set_pubkey(cert.get(), pub_key.get())) {
    LOG_ERROR(OPENSSL_LOG, "Failed to set public key for certificate.");
    return {};
  }

  if (SignatureAlgorithm::RSA == signingAlgo) {
    if (OPENSSL_FAILURE == X509_sign(cert.get(), priv_key.get(), EVP_sha256())) {
      LOG_ERROR(OPENSSL_LOG, "Failed to sign certificate using RSA private key.");
      return {};
    }
  } else if (SignatureAlgorithm::EdDSA == signingAlgo) {
    if (OPENSSL_FAILURE == X509_sign(cert.get(), priv_key.get(), nullptr)) {
      LOG_ERROR(OPENSSL_LOG, "Failed to sign certificate using EdDSA private key.");
      return {};
    }
  }

  UniqueBIO outbio(BIO_new(BIO_s_mem()));
  if (OPENSSL_FAILURE == PEM_write_bio_X509(outbio.get(), cert.get())) {
    LOG_ERROR(OPENSSL_LOG, "Unable to create certificate object");
    return string();
  }
  string certStr;
  int certLen = BIO_pending(outbio.get());
  certStr.resize(certLen);
  const auto res = BIO_read(outbio.get(), (void*)&(certStr.front()), certLen);
  if (OPENSSL_FAILURE == res || OPENSSL_ERROR == res) {
    LOG_ERROR(OPENSSL_LOG, "Failed to read data from the BIO certifiate object.");
    return {};
  }
  return certStr;
}

bool verifyCertificate(X509& cert, const string& public_key) {
  UniquePKEY pub_key(EVP_PKEY_new());
  UniqueBIO pub_bio(BIO_new(BIO_s_mem()));

  if (BIO_write(pub_bio.get(), static_cast<const char*>(public_key.c_str()), public_key.size()) <= 0) {
    return false;
  }
  if (nullptr == PEM_read_bio_PUBKEY(pub_bio.get(), reinterpret_cast<EVP_PKEY**>(&pub_key), nullptr, nullptr)) {
    return false;
  }
  return (OPENSSL_SUCCESS == X509_verify(&cert, pub_key.get()));
}

bool verifyCertificate(const X509& cert_to_verify,
                       const string& cert_root_directory,
                       uint32_t& remote_peer_id,
                       string& conn_type,
                       bool use_unified_certs) {
  // First get the source ID
  static constexpr size_t SIZE = 512;
  string subject(SIZE, 0);
  X509_NAME_oneline(X509_get_subject_name(&cert_to_verify), subject.data(), SIZE);

  int peerIdPrefixLength = 3;
  std::regex r("OU=\\d*", std::regex_constants::icase);
  std::smatch sm;
  regex_search(subject, sm, r);
  if (sm.length() <= peerIdPrefixLength) {
    LOG_ERROR(OPENSSL_LOG, "OU not found or empty: " << subject);
    return false;
  }

  auto remPeer = sm.str().substr(peerIdPrefixLength, sm.str().length() - peerIdPrefixLength);
  if (0 == remPeer.length()) {
    LOG_ERROR(OPENSSL_LOG, "OU empty " << subject);
    return false;
  }

  uint32_t remotePeerId;
  try {
    remotePeerId = stoul(remPeer, nullptr);
  } catch (const std::invalid_argument& ia) {
    LOG_ERROR(OPENSSL_LOG, "cannot convert OU, " << subject << ", " << ia.what());
    return false;
  } catch (const std::out_of_range& e) {
    LOG_ERROR(OPENSSL_LOG, "cannot convert OU, " << subject << ", " << e.what());
    return false;
  }
  remote_peer_id = remotePeerId;
  string CN;
  CN.resize(SIZE);
  X509_NAME_get_text_by_NID(X509_get_subject_name(&cert_to_verify), NID_commonName, CN.data(), SIZE);
  string cert_type = "server";
  if (CN.find("cli") != string::npos) {
    cert_type = "client";
  }
  conn_type = cert_type;

  // Get the local stored certificate for this peer
  const fs::path local_cert_path = (use_unified_certs)
                                       ? (fs::path(cert_root_directory) / std::to_string(remotePeerId) / "node.cert")
                                       : (fs::path(cert_root_directory) / std::to_string(remotePeerId) /
                                          fs::path(cert_type) / fs::path(cert_type + ".cert"));
  std::unique_ptr<FILE, decltype(&fclose)> fp(fopen(local_cert_path.c_str(), "r"), fclose);
  if (nullptr == fp) {
    LOG_ERROR(OPENSSL_LOG, "Certificate file not found, path: " << local_cert_path.string());
    return false;
  }

  UniqueX509 localCert(PEM_read_X509(fp.get(), nullptr, nullptr, nullptr));
  if (nullptr == localCert) {
    LOG_ERROR(OPENSSL_LOG, "Cannot parse certificate, path: " << local_cert_path.string());
    return false;
  }

  // this is actual comparison, compares hash of 2 certs
  return (X509_cmp(&cert_to_verify, localCert.get()) == 0);
}

std::string getSubjectFieldByName(const std::string& cert_path, const std::string& attribute_name) {
  if (name_to_id_map.find(attribute_name) == name_to_id_map.end()) {
    LOG_ERROR(GL, "Invalid attribute name: " << attribute_name);
    return std::string{};
  }
  const auto& nid = name_to_id_map.at(attribute_name);
  auto deleter = [](FILE* fp) {
    if (fp) fclose(fp);
  };
  char buf[1024];
  std::unique_ptr<FILE, decltype(deleter)> fp(fopen(cert_path.c_str(), "r"), deleter);
  if (!fp) {
    LOG_ERROR(GL, "Certificate file not found, path: " << cert_path);
    return std::string();
  }

  X509* cert = PEM_read_X509(fp.get(), NULL, NULL, NULL);
  if (!cert) {
    LOG_ERROR(GL, "Cannot parse certificate, path: " << cert_path);
    return std::string();
  }
  // The returned value of X509_get_subject_name is an internal pointer
  // which MUST NOT be freed.
  X509_NAME* name = X509_get_subject_name(cert);
  auto name_len = X509_NAME_get_text_by_NID(name, nid, buf, sizeof(buf));
  if (name_len == -1 || name_len == -2) {
    LOG_ERROR(GL, "name entry not found or invalid. error_code:" << name_len);
    X509_free(cert);
    return std::string{};
  }
  X509_free(cert);
  return std::string(buf);
}
std::vector<std::string> getSubjectFieldListByName(const std::string& cert_bundle_path,
                                                   const std::string& attribute_name) {
  auto attribute_list = std::vector<std::string>{};
  X509_STORE* store = NULL;
  if (name_to_id_map.find(attribute_name) == name_to_id_map.end()) {
    LOG_ERROR(GL, "Invalid attribute name: " << attribute_name);
    return {};
  }
  const auto& nid = name_to_id_map.at(attribute_name);
  char buf[1024];
  if (!(store = X509_STORE_new())) {
    LOG_ERROR(GL, "Error creating X509_STORE_CTX object\n");
    return {};
  }
  auto ret = X509_STORE_load_locations(store, cert_bundle_path.c_str(), NULL);
  if (ret != 1) {
    LOG_ERROR(GL, "Error loading CA cert or chain file\n");
    return {};
  }
  auto objs = X509_STORE_get0_objects(store);
  for (int i = 0; i < sk_X509_OBJECT_num(objs); i++) {
    X509_OBJECT* x509_obj = sk_X509_OBJECT_value(objs, i);
    if (x509_obj) {
      X509* cert = X509_OBJECT_get0_X509(x509_obj);
      if (cert) {
        X509_NAME* name = X509_get_subject_name(cert);
        auto name_len = X509_NAME_get_text_by_NID(name, nid, buf, sizeof(buf));
        if (name_len == -1 || name_len == -2) {
          continue;
        }
        attribute_list.emplace_back(std::string(buf));
      }
    }
  }
  X509_STORE_free(store);
  return attribute_list;
}

}  // namespace concord::crypto
