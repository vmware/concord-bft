
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

// This file describes the wire protocol for the diagnostics server. The protocol is an ASCII based
// protocol as it is expected to be interacted with over the command line. For security purposes we
// expect to only run the protocol over a local unix domain socket.

#pragma once

#include <array>
#include <variant>

#include "diagnostics.h"

namespace concord::diagnostics {

inline std::string usage() {
  // TODO: Make the name of the program dynamic
  std::string usage = "Usage: concord-ctl <SUBJECT> <COMMAND> [ARGS]\n\n";
  usage += "  status <COMMAND> [ARGS]\n\n";
  usage += "    status get <KEY1> [KEY2]..[KEY_N]\n";
  usage += "        Get the status of the given key(s).\n\n";
  usage += "    status describe [KEY1] [KEY2]..[KEY_N]\n";
  usage += "        Get the description of the given keys, or all keys if none is given.\n\n";
  usage += "    status list\n";
  usage += "        List all status keys.\n\n";
  usage += "  perf <COMMAND> [ARGS]\n\n";
  usage += "    perf get <COMPONENT> [HISTOGRAM]\n";
  usage += "        Get all histograms for <COMPONENT> or a specific [HISTOGRAM]\n\n";
  usage += "    perf list [COMPONENT]\n";
  usage += "        List all components or all histograms for a given [COMPONENT]\n\n";
  usage += "    perf snapshot <COMPONENT1> [COMPONENT2]...[COMPONENT_N]\n";
  usage += "        Snapshot all histograms for the given components.";
  return usage;
}

template <typename Iterator, typename Fun>
std::string accumulate(Iterator begin, Iterator end, Fun f) {
  std::string output;
  for (auto it = begin; it != end; it++) {
    output += f(*it) + "\n";
  }
  return output;
}

// Take protocol input as a split string, along with a registrar and return diagnostics or a usage string.
inline std::string run(const std::vector<std::string>& tokens, Registrar& registrar) {
  if (tokens.size() < 2) return usage();

  auto& subject = tokens[0];
  auto& command = tokens[1];
  if (subject == "status") {
    if (command == "describe") {
      if (tokens.size() == 2) {
        return registrar.status.describe();
      } else {
        return diagnostics::accumulate(tokens.begin() + 2, tokens.end(), [&registrar](const auto& key) -> std::string {
          return registrar.status.describe(key);
        });
      }
    }

    if (command == "get") {
      if (tokens.size() < 3) return usage();
      return diagnostics::accumulate(tokens.begin() + 2, tokens.end(), [&registrar](const auto& key) -> std::string {
        return registrar.status.get(key);
      });
    }

    if (command == "list") {
      if (tokens.size() != 2) return usage();
      return registrar.status.listKeys();
    }
  }

  if (subject == "perf") {
    try {
      if (command == "get") {
        if (tokens.size() == 3) {
          return registrar.perf.toString(registrar.perf.get(tokens[2]));
        }
        if (tokens.size() == 4) {
          return registrar.perf.toString(registrar.perf.get(tokens[2], tokens[3]));
        }
        return usage();
      }
      if (command == "list") {
        if (tokens.size() == 2) {
          return registrar.perf.list();
        }
        if (tokens.size() == 3) {
          return registrar.perf.list(tokens[2]);
        }
        return usage();
      }
      if (command == "snapshot") {
        if (tokens.size() < 3) return usage();
        // Eliminate duplicate components. Otherwise snapshots will occur multiple times, probably
        // with no samples in between. This will almost always be due to a typo, and is a fairly
        // useless behavior, so eliminate the possibility.
        std::vector<std::string> components(tokens.begin() + 2, tokens.end());
        std::sort(components.begin(), components.end());
        components.erase(std::unique(components.begin(), components.end()), components.end());
        for (auto it = components.begin(); it != components.end(); it++) {
          registrar.perf.snapshot(*it);
        }
        return "";
      }
    } catch (const std::exception& e) {
      return e.what();
    }
  }

  return usage();
}

}  // namespace concord::diagnostics
