// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "IReservedPages.hpp"
#include <cstdint>
#include <string>
#include <typeindex>
#include <algorithm>
#include <map>
#include <iostream>
#include "Logger.hpp"

namespace bftEngine {

namespace test {
template <typename, uint32_t>
class ReservedPagesMock;
}

class ReservedPagesClientBase {
 public:
  static uint32_t totalNumberOfPages() {
    uint32_t numPages{0};
    for (auto& elt : registry()) numPages += elt.second;

    return numPages;
  }
  static void setReservedPages(IReservedPages* rp) { res_pages_ = rp; }

 protected:
  // [type_index -> numberOfPages]
  typedef std::map<std::type_index, std::uint32_t> Registry;

  static Registry& registry() {
    static Registry registry_;
    return registry_;
  }

  static IReservedPages* res_pages_;
};
/**
 * Facility for registering Reserved Pages clients
 *
 * Become a Reserved Pages client by sub-classing:
 * class MyClass: public ResPagesClient<MyClass, requiredNumberOfPages> {};
 * If requiredNumberOfPages is not known at compile time call setNumResPages(requiredNumberOfPages).
 */
template <typename T, uint32_t NumPages = 0>
class ResPagesClient : public ReservedPagesClientBase, public IReservedPages {
 public:
  ResPagesClient() { (void)registered_; }

  static bool registerT() {
    Registry& reg = registry();
    if (auto it = reg.find(std::type_index(typeid(T))); it != reg.end()) {
      std::cerr << __PRETTY_FUNCTION__ << " BUG: already registered." << std::endl;
      std::terminate();
    }
    reg[std::type_index(typeid(T))] = NumPages;
    //    std::cout << __PRETTY_FUNCTION__ << " hash: " << std::type_index(typeid(T)).hash_code() << " pages: " <<
    //    NumPages
    //              << std::endl;
    return true;
  }
  /**
   * Should be called at initialization if number of pages is not known at compile time
   */
  static void setNumResPages(const uint32_t numPages) {
    Registry& reg = registry();
    if (auto it = reg.find(std::type_index(typeid(T))); it != reg.end()) {
      it->second = numPages;
    } else {
      std::cerr << __PRETTY_FUNCTION__ << " BUG: not registered" << std::endl;
      std::terminate();
    }
  }

  static uint32_t numberOfReservedPagesForClient() {
    auto& reg = registry();
    if (auto it = reg.find(std::type_index(typeid(T))); it != reg.end()) {
      return it->second;
    } else {
      std::cerr << __PRETTY_FUNCTION__ << " BUG: not registered" << std::endl;
      std::terminate();
    }
  }

  uint32_t numberOfReservedPages() const override { return res_pages_->numberOfReservedPages(); }
  uint32_t sizeOfReservedPage() const override { return res_pages_->sizeOfReservedPage(); }
  bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const override {
    return res_pages_->loadReservedPage(my_offset() + reservedPageId, copyLength, outReservedPage);
  }
  void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) override {
    LOG_TRACE(logging::getLogger("bftengine.res_page"),
              typeid(T).name() << " page: " << reservedPageId << " offset: " << my_offset()
                               << " abs page: " << my_offset() + reservedPageId);
    res_pages_->saveReservedPage(my_offset() + reservedPageId, copyLength, inReservedPage);
  }
  void zeroReservedPage(uint32_t reservedPageId) override {
    res_pages_->zeroReservedPage(my_offset() + reservedPageId);
  }

 private:
  template <typename, uint32_t>
  friend class bftEngine::test::ReservedPagesMock;

  /** is called when calculating absolute pageId */
  static uint32_t my_offset() {
    static uint32_t offset_ = calc_my_offset();
    return offset_;
  }

  // is done once per client
  static uint32_t calc_my_offset() {
    uint32_t offset = 0;
    for (auto& it : registry()) {
      if (it.first == std::type_index(typeid(T))) {
        std::cout << __PRETTY_FUNCTION__ << " offset: " << offset << std::endl;
        return offset;
      }
      offset += it.second;
    }
    std::cerr << __PRETTY_FUNCTION__ << " BUG: not registered" << std::endl;
    std::terminate();
    return 0;
  }
  static bool registered_;
};
// static registration
template <typename T, uint32_t NumPages>
bool ResPagesClient<T, NumPages>::registered_ = ResPagesClient<T, NumPages>::registerT();

}  // namespace bftEngine
