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
#include <tuple>
#include <vector>
#include <iostream>
#include "demangle.hpp"

namespace bftEngine {

class ReservedPagesClientBase {
 public:
  static uint32_t totalNumberOfPages() {
    uint32_t numPages{0};
    for (auto& elt : registry()) numPages += std::get<2>(elt);

    return numPages;
  }
  static void setReservedPages(IReservedPages* rp) { res_pages_ = rp; }

 protected:
  // [idx, typeid, numberOfPages]
  typedef std::vector<std::tuple<std::uint8_t, std::type_index, std::uint32_t>> Registry;

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
 * class MyClass: public ResPagesClient<MyClass, idx, requiredNumberOfPages> {};
 * where idx is a unique integer used for dividing a reserved pages space into sub-spaces.
 * If requiredNumberOfPages is not known at compile time call setNumResPages(requiredNumberOfPages).
 */
template <typename T, uint8_t Idx, uint32_t NumPages = 0>
class ResPagesClient : public ReservedPagesClientBase, public IReservedPages {
 public:
  ResPagesClient() { (void)registered_; }

  static bool registerT() {
    Registry& reg = registry();
    auto it = std::find_if(reg.begin(), reg.end(), [](const Registry::value_type& v) { return std::get<0>(v) == Idx; });
    if (it != reg.end()) {
      std::cerr << "BUG: ResPagesClient<" << demangler::demangle<T>() << ", " << (int)Idx << ">: index is used by "
                << demangler::demangle(std::get<1>(*it).name());
      std::terminate();
    }
    reg.push_back(Registry::value_type(Idx, std::type_index(typeid(T)), NumPages));
    // sorting by clients' index
    std::sort(reg.begin(), reg.end(), [](const Registry::value_type& v1, const Registry::value_type& v2) {
      return std::get<0>(v1) < std::get<0>(v2);
    });

    return true;
  }
  /**
   * Should be called at initialization if number of pages is not known at compile time
   */
  static void setNumResPages(const uint32_t numPages) {
    Registry& reg = registry();
    auto it = std::find_if(reg.begin(), reg.end(), [](const Registry::value_type& v) {
      return std::get<1>(v) == std::type_index(typeid(T));
    });
    if (it == reg.end()) {
      std::cerr << __PRETTY_FUNCTION__ << " BUG: not registered: " << demangler::demangle<T>() << std::endl;
      std::terminate();
    }
    std::get<2>(*it) = numPages;
  }

  uint32_t numberOfReservedPages() const override { return res_pages_->numberOfReservedPages(); }
  uint32_t sizeOfReservedPage() const override { return res_pages_->sizeOfReservedPage(); }
  bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const override {
    return res_pages_->loadReservedPage(my_offset() + reservedPageId, copyLength, outReservedPage);
  }
  void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) override {
    res_pages_->saveReservedPage(my_offset() + reservedPageId, copyLength, inReservedPage);
  }
  void zeroReservedPage(uint32_t reservedPageId) override {
    res_pages_->zeroReservedPage(my_offset() + reservedPageId);
  }

 private:
  /** is called when calculating absolute pageId */
  uint32_t my_offset() const {
    static uint32_t offset_ = calc_my_offset();
    return offset_;
  }
  // is done once per client
  uint32_t calc_my_offset() const {
    uint32_t offset = 0;
    for (auto& elt : registry()) {
      if (std::get<1>(elt) == std::type_index(typeid(T))) return offset;
      offset += std::get<2>(elt);
    }
    std::cerr << __PRETTY_FUNCTION__ << " BUG: not registered: " << demangler::demangle<T>() << std::endl;
    std::terminate();
    return 0;
  }
  static bool registered_;
};
// static registration
template <typename T, uint8_t Idx, uint32_t NumPages>
bool ResPagesClient<T, Idx, NumPages>::registered_ = ResPagesClient<T, Idx, NumPages>::registerT();

}  // namespace bftEngine
