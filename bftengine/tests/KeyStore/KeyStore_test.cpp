#include "KeyStore.h"
#include "gtest/gtest.h"

using namespace bftEngine;
using namespace bftEngine::impl;

struct ReservedPagesMock : public IReservedPages {
  int size{4096};
  std::vector<char*> resPages;
  ReservedPagesMock(uint32_t num_pages, bool clean) {
    for (uint32_t i = 0; i < num_pages; ++i) {
      auto c = new char[size]();
      resPages.push_back(c);
      if (!clean) continue;
      memset(c, 0, size);
    }
    ReservedPagesClientBase::setReservedPages(this);
  }
  ~ReservedPagesMock() {
    for (auto c : resPages) {
      delete[] c;
    }
  }
  virtual uint32_t numberOfReservedPages() const { return resPages.size(); };
  virtual uint32_t sizeOfReservedPage() const { return size; };
  virtual bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const {
    memcpy(outReservedPage, resPages[reservedPageId], copyLength);
    return true;
  };
  virtual void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) {
    memcpy(resPages[reservedPageId], inReservedPage, copyLength);
  };
  virtual void zeroReservedPage(uint32_t reservedPageId) { memset(resPages[reservedPageId], 0, size); };
};

TEST(checkAndSetState_empty, ClientKeyStore) {
  ReservedPagesMock rpm{5, true};
  std::string keys{"id1:HX56AB"};
  ClientKeyStore cks{keys};
  cks.checkAndSetState(keys);
  ASSERT_FALSE(cks.published_);
}

TEST(checkAndSetState_set_true, ClientKeyStore) {
  ReservedPagesMock rpm{5, true};
  std::string keys{"id1:HX56AB"};
  ClientKeyStore cks{keys};
  ASSERT_FALSE(cks.published_);
  cks.saveToReservedPages(keys);
  cks.checkAndSetState(keys);
  ASSERT_TRUE(cks.published_);
}

TEST(checkAndSetState_set_false, ClientKeyStore) {
  ReservedPagesMock rpm{5, true};
  std::string keys{"id1:HX56AB"};
  {
    ClientKeyStore cks{keys};
    cks.saveToReservedPages(keys);
  }
  ClientKeyStore cks{keys};
  ASSERT_TRUE(cks.published_);

  {
    std::string keys{"id2:894894"};
    cks.saveToReservedPages(keys);
  }

  cks.checkAndSetState(keys);
  ASSERT_FALSE(cks.published_);
}

TEST(compare_agaisnt_reserved_pages, ClientKeyStore) {
  ReservedPagesMock rpm{5, true};
  std::string keys{"id1:HX56AB"};
  ClientKeyStore cks{keys};
  cks.saveToReservedPages(keys);
  ASSERT_FALSE(cks.compareAgainstReservedPage(""));
  ASSERT_FALSE(cks.compareAgainstReservedPage("djkjdksjdkjskd"));
  ASSERT_FALSE(cks.compareAgainstReservedPage("id1:HX56Ab"));
  ASSERT_TRUE(cks.compareAgainstReservedPage(keys));
}

TEST(construct_with_same_keys, ClientKeyStore) {
  ReservedPagesMock rpm{5, true};
  std::string keys{"id1:HX56AB"};
  {
    ClientKeyStore cks{keys};
    cks.saveToReservedPages(keys);
  }
  ClientKeyStore cks{keys};
  ASSERT_TRUE(cks.published_);
}

TEST(construct_with_diff_keys, ClientKeyStore) {
  ReservedPagesMock rpm{5, true};
  std::string keys{"id1:HX56AB"};
  std::string new_keys{"id2:ABC123"};
  {
    ClientKeyStore cks{keys};
    cks.saveToReservedPages(keys);
  }
  ClientKeyStore cks{new_keys};
  ASSERT_FALSE(cks.published_);
}
