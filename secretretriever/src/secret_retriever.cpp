// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "secret_retriever.hpp"
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <nlohmann/json.hpp>
#include <optional>
#include <regex>
#include "Logger.hpp"
#include "include/httplib.h"

using json = nlohmann::json;
using namespace std::string_literals;
using namespace concord::secretsmanager;

namespace concord::secretsretriever {

static const int MAX_RETRIES = 60;
static const int RETRY_DELAY = 1;

auto logger = logging::getLogger("concord.secretretriever");

SecretData ParseJson(json j) {
  return SecretData{j["key"].get<std::string>(),
                    j["iv"].get<std::string>(),
                    j["algorithm"].get<std::string>(),
                    j["key_length"].get<uint32_t>(),
                    j["additional_info"].is_string() ? j["additional_info"].get<std::string>() : ""};
}

std::optional<SecretData> FileSecretRetrieve(const std::string &secret_url) {
  std::regex url_regex(R"(^file://(.*))", std::regex::extended);
  std::smatch url_match;
  if (std::regex_match(secret_url, url_match, url_regex)) {
    std::ifstream jsonFile(url_match[1], std::ifstream::in);
    if (jsonFile.fail()) throw std::runtime_error("Secret retrieve failed to open "s + secret_url);
    json j;
    jsonFile >> j;
    return ParseJson(j);
  } else {
    return std::nullopt;
  }
}

std::shared_ptr<httplib::Response> GetWithRetry(httplib::Client &cli, const std::string &path) {
  std::shared_ptr<httplib::Response> httpres;
  size_t retries = 0;
  while (true) {
    LOG_INFO(logger, "Secrets API GET " << path);
    httpres = cli.Get(path.c_str());
    if (!httpres || httpres->status != 200) {
      if (++retries > MAX_RETRIES) {
        throw std::runtime_error("Secret retrieve Rest API call failed!");
      }
      LOG_INFO(logger, "Secrets API GET " << path << " failed! Retrying...");
      sleep(RETRY_DELAY);
      continue;
    }
    return httpres;
  }
}

std::optional<SecretData> RemoteSecretRetrieve(const std::string &secret_url) {
  std::regex url_regex(R"(^(https|http)://([a-zA-Z0-9\.-]+)(:([0-9]+))?(.*))", std::regex::extended);
  std::smatch url_match;
  if (std::regex_match(secret_url, url_match, url_regex)) {
    std::shared_ptr<httplib::Response> httpres;
    const std::string host = url_match[2];
    const std::string port = url_match[4];
    const std::string path = url_match[5];

    if ("http"s.compare(url_match[1]) == 0) {
      int p = 80;
      if (!port.empty()) p = stoi(port);
      httplib::Client cli(host, p);
      httpres = GetWithRetry(cli, path);
    } else {
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
      int p = 443;
      if (!port.empty()) p = stoi(port);
      httplib::SSLClient cli(host, p);
      cli.enable_server_certificate_verification(false);
      httpres = GetWithRetry(cli, path);
#else
      throw std::runtime_error("Secret retrieve Rest API call failed! Build without HTTPS support!");
#endif  // CPPHTTPLIB_OPENSSL_SUPPORT
    }

    if (!httpres) throw std::runtime_error("Secret retrieve Rest API call failed!");
    if (httpres->status != 200)
      throw std::runtime_error("Secret retrieve Rest API call failed "s + std::to_string(httpres->status));

    return ParseJson(json::parse(httpres->body));
  } else {
    return std::nullopt;
  }
}

SecretData SecretRetrieve(const std::string &secret_url) {
  {
    auto res = FileSecretRetrieve(secret_url);
    if (res) return res.value();
  }
  {
    auto res = RemoteSecretRetrieve(secret_url);
    if (res) return res.value();
  }
  throw std::runtime_error("URL is invalid.");
}

}  // namespace concord::secretsretriever