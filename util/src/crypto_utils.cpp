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

#include "crypto_utils.hpp"
#include "string.hpp"
#include <unordered_map>

namespace concord::util::crypto {

const std::unordered_map<std::string, int> name_to_id_map = {{"C", NID_countryName},
                                                             {"L", NID_localityName},
                                                             {"ST", NID_stateOrProvinceName},
                                                             {"O", NID_organizationName},
                                                             {"OU", NID_organizationalUnitName},
                                                             {"CN", NID_commonName}};

bool isValidKey(const std::string& keyName, const std::string& key, size_t expectedSize) {
  auto isValidHex = isValidHexString(key);
  if ((expectedSize == 0 or (key.length() == expectedSize)) and isValidHex) {
    return true;
  }
  throw std::runtime_error("Invalid " + keyName + " key (" + key + ") of size " + std::to_string(expectedSize) +
                           " bytes.");
}

std::string CertificateUtils::getSubjectFieldByName(const std::string& cert_path, const std::string& attribute_name) {
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

std::vector<std::string> CertificateUtils::getSubjectFieldListByName(const std::string& cert_bundle_path,
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
}  // namespace concord::util::crypto
