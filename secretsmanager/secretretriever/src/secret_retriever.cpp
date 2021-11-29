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
#include "httplib.h"

using json = nlohmann::json;
using namespace std::string_literals;
using namespace concord::secretsmanager;

namespace concord::secretsmanager::secretretriever {

static const int MAX_RETRIES = 60;
static const int RETRY_DELAY = 1;

auto logger = logging::getLogger("concord.secretretriever");

SecretData parseJson(json j) {
  auto additional_info = j["additional_info"].is_string() ? j["additional_info"].get<std::string>() : "";

  try {
    auto key = j["key"].get<std::string>();
    auto iv = j["iv"].get<std::string>();
    auto algorithm = j["algorithm"].get<std::string>();
    auto key_length = j["key_length"].get<uint32_t>();

    return SecretData{key, iv, algorithm, key_length, additional_info};

  } catch (const std::exception &e) {
    LOG_ERROR(logger, "Failed to read JSON content '" << additional_info << "'");
    throw;
  }
}

std::optional<SecretData> retrieveFileSecret(const std::string &secret_url) {
  std::regex url_regex(R"(^file://(.*))", std::regex::extended);
  std::smatch url_match;
  if (std::regex_match(secret_url, url_match, url_regex)) {
    std::ifstream jsonFile(url_match[1], std::ifstream::in);
    if (jsonFile.fail()) throw std::runtime_error("Secret retrieve failed to open "s + secret_url);
    json j;
    jsonFile >> j;
    return parseJson(j);
  } else {
    return std::nullopt;
  }
}

httplib::Result getWithRetry(httplib::ClientImpl &cli, const std::string &path) {
  httplib::Result httpres(nullptr, httplib::Error::Unknown);
  size_t retries = 0;
  while (true) {
    LOG_INFO(logger, "Secrets API GET " << path);
    httpres = cli.Get(path.c_str());
    if (!httpres || httpres.error() != httplib::Error::Success) {
      if (++retries > MAX_RETRIES) {
        throw std::runtime_error("Secret retrieve Rest API call failed!");
      }
      LOG_INFO(logger,
               "Secrets API GET " << path << " failed with the error: " << httplib::to_string(httpres.error())
                                  << "! Retrying...");
      sleep(RETRY_DELAY);
      continue;
    }
    return httpres;
  }
}

std::optional<SecretData> retrieveSecretRemote(const std::string &secret_url) {
  std::regex url_regex(R"(^(https|http)://([a-zA-Z0-9\.-]+)(:([0-9]+))?(.*))", std::regex::extended);
  std::smatch url_match;
  if (std::regex_match(secret_url, url_match, url_regex)) {
    httplib::Result httpres(nullptr, httplib::Error::Unknown);
    const std::string host = url_match[2];
    const std::string port = url_match[4];
    const std::string path = url_match[5];

    if ("http"s.compare(url_match[1]) == 0) {
      int p = 80;
      if (!port.empty()) p = stoi(port);
      httplib::ClientImpl cli(host, p);
      httpres = getWithRetry(cli, path);
    } else {
      int p = 443;
      if (!port.empty()) p = stoi(port);
      httplib::SSLClient cli(host, p);
      cli.enable_server_certificate_verification(false);
      httpres = getWithRetry(cli, path);
    }

    if (!httpres) throw std::runtime_error("Secret retrieve Rest API call failed!");
    if (httpres->status != 200)
      throw std::runtime_error("Secret retrieve Rest API call failed "s + std::to_string(httpres->status));

    return parseJson(json::parse(httpres->body));
  } else {
    return std::nullopt;
  }
}

SecretData retrieveSecret(const std::string &secret_url) {
  {
    auto res = retrieveFileSecret(secret_url);
    if (res) return res.value();
  }
  {
    auto res = retrieveSecretRemote(secret_url);
    if (res) return res.value();
  }
  throw std::runtime_error("URL is invalid.");
}

}  // namespace concord::secretsmanager::secretretriever
