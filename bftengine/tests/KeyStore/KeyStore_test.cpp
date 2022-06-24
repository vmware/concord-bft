#include "KeyStore.h"
#include "gtest/gtest.h"
#include "keys_and_signatures.cmf.hpp"

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
  ClientKeyStore cks(true);
  ASSERT_FALSE(cks.published());
}

TEST(checkAndSetState_set_true, ClientKeyStore) {
  ReservedPagesMock rpm{5, true};
  concord::messages::keys_and_signatures::ClientsPublicKeys cpk;
  concord::messages::keys_and_signatures::PublicKey entry{"1234", 2};
  cpk.ids_to_keys[0] = entry;
  std::vector<uint8_t> ser_keys;
  concord::messages::keys_and_signatures::serialize(ser_keys, cpk);
  std::string str_ser(ser_keys.begin(), ser_keys.end());
  ClientKeyStore cks(false);
  cks.save(str_ser);
  ASSERT_TRUE(cks.published());
  auto loaded_keys = cks.load();
  auto hashed_keys = concord::util::SHA3_256().digest(str_ser.c_str(), str_ser.size());
  auto strHashed_keys = std::string(hashed_keys.begin(), hashed_keys.end());
  ASSERT_EQ(loaded_keys, strHashed_keys);
}

TEST(checkAndSetState_start_true, ClientKeyStore) {
  ReservedPagesMock rpm{5, true};
  concord::messages::keys_and_signatures::ClientsPublicKeys cpk;
  concord::messages::keys_and_signatures::PublicKey entry{"1234", 2};
  cpk.ids_to_keys[0] = entry;
  std::vector<uint8_t> ser_keys;
  concord::messages::keys_and_signatures::serialize(ser_keys, cpk);
  std::string str_ser(ser_keys.begin(), ser_keys.end());
  {
    ClientKeyStore cks(true);
    ASSERT_FALSE(cks.published());
    cks.save(str_ser);
    ASSERT_TRUE(cks.published());
  }
  {
    ClientKeyStore cks(false);
    ASSERT_TRUE(cks.published());
  }
}
