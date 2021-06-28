// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "IReservedPages.hpp"

#include <algorithm>
#include <cstring>
#include <map>
#include <string>

namespace bftEngine::test {

template <typename ReservedPagesClient, uint32_t kSizeOfReservedPage = 4096>
class ReservedPagesMock : public bftEngine::IReservedPages {
 public:
  uint32_t numberOfReservedPages() const override { return ReservedPagesClient::getNumResPages(); }

  uint32_t sizeOfReservedPage() const override { return kSizeOfReservedPage; }

  bool loadReservedPage(uint32_t page_id, uint32_t size, char* data) const override {
    auto it = pages_.find(page_id);
    if (it != pages_.cend()) {
      std::memcpy(data, it->second.data(), std::min<std::size_t>(size, it->second.size()));
      return true;
    }
    return false;
  }

  void saveReservedPage(uint32_t page_id, uint32_t size, const char* data) override {
    pages_[page_id].assign(data, std::min<std::size_t>(size, sizeOfReservedPage()));
  }

  void zeroReservedPage(uint32_t page_id) override {
    auto it = pages_.find(page_id);
    if (it != pages_.cend()) {
      for (auto i = 0ul; i < it->second.size(); ++i) {
        it->second[i] = '\0';
      }
    }
  }

  bool isReservedPageZeroed(uint32_t page_id) const {
    auto it = pages_.find(ReservedPagesClient::my_offset() + page_id);
    if (it == pages_.cend()) {
      return false;
    }
    return (it->second.find_first_not_of('\0') == std::string::npos);
  }

  const std::map<uint32_t, std::string>& pages() const { return pages_; }

 private:
  // reserved page ID -> contents
  std::map<uint32_t, std::string> pages_;
};

}  // namespace bftEngine::test
