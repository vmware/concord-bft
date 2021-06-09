// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include <chrono>
#include <thread>
#include "../../src/bftengine/KeyExchangeManager.h"

using namespace std;

namespace bftEngine::impl {
class TestKeyManager {
 public:
  TestKeyManager(KeyExchangeManager::InitData* id) : km_(id), a(new concordMetrics::Aggregator()) {
    km_.initMetrics(a, std::chrono::seconds(600));
  }
  KeyExchangeManager km_;
  std::shared_ptr<concordMetrics::Aggregator> a;
  uint64_t getKeyExchangedCounter() { return km_.metrics_->keyExchangedCounter.Get().Get(); }
  uint64_t getKeyExchangedOnStartCounter() { return km_.metrics_->keyExchangedOnStartCounter.Get().Get(); }
  uint64_t getPublicKeyRotated() { return km_.metrics_->publicKeyRotated.Get().Get(); }
};

struct DummyKeyGen : public IMultiSigKeyGenerator, public IKeyExchanger {
  std::pair<std::string, std::string> generateMultisigKeyPair() { return std::make_pair(prv, pub); }
  void onPrivateKeyExchange(const std::string& secretKey, const std::string& verificationKey) {
    selfpub = verificationKey;
    selfprv = secretKey;
  }
  void onPublicKeyExchange(const std::string& verificationKey, const std::uint16_t& signerIndex) {
    pubs[signerIndex] = verificationKey;
  }
  DummyKeyGen(uint32_t cs) : pubs{cs, ""} {}
  std::string prv;
  std::string pub;
  std::vector<std::string> pubs;
  std::string selfpub;
  std::string selfprv;
};

class DummyLoaderSaver : public ISecureStore {
 public:
  std::string cache;
  void save(const std::string& s) { cache = s; };
  std::string load() { return cache; }
};

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

TEST(Messages, ser_der) {
  KeyExchangeMsg kem;
  kem.key = "qwerty";
  kem.signature = "123456";
  kem.repID = 6;

  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, kem);
  auto strMsg = ss.str();

  auto deskem = KeyExchangeMsg::deserializeMsg(strMsg.c_str(), strMsg.size());

  ASSERT_EQ(deskem.key, kem.key);
  ASSERT_EQ(deskem.signature, kem.signature);
  ASSERT_EQ(deskem.repID, kem.repID);

  // Test constuctor with move of KeyExchangeMsg
  ReplicaKeyStore::ReplicaKey rk(kem, 123);
  ASSERT_EQ(rk.msg.key, "qwerty");
  ASSERT_EQ(rk.msg.signature, "123456");
  ASSERT_EQ(rk.msg.repID, 6);
  ASSERT_EQ(rk.seqnum, 123);

  ss.str("");
  concord::serialize::Serializable::serialize(ss, rk);
  auto rkStr = ss.str();

  ReplicaKeyStore::ReplicaKey der_rk;
  ss.str("");
  KeyExchangeMsg ke;
  ss.write(rkStr.c_str(), std::streamsize(rkStr.size()));
  concord::serialize::Serializable::deserialize(ss, der_rk);

  ASSERT_EQ(rk.msg.key, der_rk.msg.key);
  ASSERT_EQ(rk.msg.signature, der_rk.msg.signature);
  ASSERT_EQ(rk.msg.repID, der_rk.msg.repID);
  ASSERT_EQ(rk.seqnum, der_rk.seqnum);
}

TEST(ReplicaKeyStore, ser_der) {
  ReplicaKeyStore rks;
  KeyExchangeMsg kem;
  kem.key = "qwerty";
  kem.signature = "123456";
  kem.repID = 6;
  KeyExchangeMsg kem2;
  kem.key = "ytrewq";
  kem.signature = "098765";
  kem.repID = 2;
  kem2.repID = 2;
  rks.push(kem, 34);
  rks.push(kem2, 222);

  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, rks);
  auto strRks = ss.str();

  ReplicaKeyStore der_rks;
  ss.str("");
  ss.write(strRks.c_str(), std::streamsize(strRks.size()));
  concord::serialize::Serializable::deserialize(ss, der_rks);

  ASSERT_EQ(der_rks.numKeys(), rks.numKeys());
  ASSERT_EQ(der_rks.current().msg.key, rks.current().msg.key);
  ASSERT_EQ(der_rks.current().msg.signature, rks.current().msg.signature);
  ASSERT_EQ(der_rks.current().msg.repID, rks.current().msg.repID);
  ASSERT_EQ(der_rks.current().seqnum, rks.current().seqnum);
  der_rks.pop();
  rks.pop();
  ASSERT_EQ(der_rks.current().msg.key, rks.current().msg.key);
  ASSERT_EQ(der_rks.current().msg.signature, rks.current().msg.signature);
  ASSERT_EQ(der_rks.current().msg.repID, rks.current().msg.repID);
  ASSERT_EQ(der_rks.current().seqnum, rks.current().seqnum);
}

TEST(ReplicaKeyStore, key_limit) {
  ReplicaKeyStore rks;
  rks.setKeysLimit(3);
  ASSERT_EQ(rks.numKeys(), 0);

  KeyExchangeMsg kem;
  kem.key = "qwerty";
  kem.signature = "123456";
  kem.repID = 6;
  KeyExchangeMsg kem2;
  kem2.key = "ytrewq";
  kem2.signature = "098765";
  kem2.repID = 2;
  rks.push(kem, 34);
  rks.push(kem2, 222);
  ASSERT_EQ(rks.numKeys(), 2);
  ASSERT_EQ(rks.current().msg.key, "qwerty");

  KeyExchangeMsg kem3;
  kem3.key = "qwerty3";
  kem3.signature = "1234563";
  kem3.repID = 3;
  KeyExchangeMsg kem4;
  kem4.key = "ytrewq4";
  kem4.signature = "0987654";
  kem4.repID = 4;
  rks.push(kem3, 34);
  ASSERT_EQ(rks.numKeys(), 3);
  auto ok = rks.push(kem4, 222);
  ASSERT_EQ(ok, false);
  ASSERT_EQ(rks.numKeys(), 3);
  rks.pop();
  ASSERT_EQ(rks.numKeys(), 2);
  ASSERT_EQ(rks.current().msg.key, "ytrewq");
}

TEST(ReplicaKeyStore, rotate) {
  ReplicaKeyStore rks;
  KeyExchangeMsg kem;
  kem.key = "qwerty";
  kem.signature = "123456";
  kem.repID = 6;
  rks.push(kem, 34);
  auto ok = rks.rotate(2);
  // Rotate won't be performed, only one key.
  ASSERT_EQ(ok, false);
  KeyExchangeMsg kem2;
  kem2.key = "ytrewq";
  kem2.signature = "098765";
  kem2.repID = 2;
  rks.push(kem2, 35);
  ASSERT_EQ(rks.numKeys(), 2);
  ok = rks.rotate(1);
  // Rotate won't be performed, checkpoint is too close.
  ASSERT_EQ(ok, false);
  ok = rks.rotate(2);
  ASSERT_EQ(ok, true);
  ASSERT_EQ(rks.numKeys(), 1);
  ASSERT_EQ(rks.current().msg.key, "ytrewq");
  KeyExchangeMsg kem3;
  kem3.key = "dsdsdsd";
  kem3.signature = "098765";
  kem3.repID = 2;
  // edge case, where key exchange is on checkpoint
  rks.push(kem3, 450);
  ok = rks.rotate(3);
  // Rotate won't be performed, checkpoint is too close.
  ASSERT_EQ(ok, false);
  ok = rks.rotate(4);
  ASSERT_EQ(ok, true);
}

// TEST(ReplicaKeyStore, rotate_death_if_checkpoint_is_less) {
//   ReplicaKeyStore rks;
//   KeyExchangeMsg kem;
//   kem.key = "a";
//   kem.signature = "1";
//   kem.repID = 6;
//   rks.push(kem, 1);
//   KeyExchangeMsg kem2;
//   kem2.key = "qwerty";
//   kem2.signature = "123456";
//   kem2.repID = 6;
//   rks.push(kem2, 340);
//   // Checkpoint can't be less than key exchange seq num
//   ASSERT_DEATH(rks.rotate(2), ".*");
// }

// TEST(ReplicaKeyStore, rotate_death_if_checkpoint_is_more) {
//   ReplicaKeyStore rks;
//   KeyExchangeMsg kem;
//   kem.key = "a";
//   kem.signature = "1";
//   kem.repID = 6;
//   rks.push(kem, 1);
//   KeyExchangeMsg kem2;
//   kem2.key = "qwerty";
//   kem2.signature = "123456";
//   kem2.repID = 6;
//   rks.push(kem2, 340);

//   ASSERT_DEATH(rks.rotate(5), ".*");
// }

TEST(ClusterKeyStore, push) {
  uint32_t clustersize{7};
  ReservedPagesMock rpm(ReservedPagesClientBase::totalNumberOfPages() + clustersize, true);
  ClusterKeyStore cks{7, 4094};
  {
    KeyExchangeMsg kem;
    kem.key = "a";
    kem.signature = "1";
    kem.repID = 8;
    // replica id out of range
    ASSERT_EQ(cks.push(kem, 2), false);
  }

  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 1;
  auto ok = cks.push(kem, 2);
  ASSERT_EQ(ok, true);

  KeyExchangeMsg kem2;
  kem2.key = "c";
  kem2.signature = "3";
  kem2.repID = 3;
  cks.push(kem2, 3);

  KeyExchangeMsg kem3;
  kem3.key = "d";
  kem3.signature = "4";
  kem3.repID = 3;
  cks.push(kem3, 3);

  KeyExchangeMsg kem4;
  kem4.key = "e";
  kem4.signature = "4w";
  kem4.repID = 3;
  // limit excceds
  ASSERT_EQ(cks.push(kem4, 2), false);

  auto msg = cks.getReplicaPublicKey(3);
  ASSERT_EQ(msg.key, "c");
  ASSERT_EQ(msg.signature, "3");
}

TEST(ClusterKeyStore, rotate) {
  uint32_t clustersize{7};
  ReservedPagesMock rpm(ReservedPagesClientBase::totalNumberOfPages() + clustersize, true);
  ClusterKeyStore cks{7, 4094};

  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 1;
  cks.push(kem, 2);

  KeyExchangeMsg kem2;
  kem2.key = "c";
  kem2.signature = "3";
  kem2.repID = 3;
  cks.push(kem2, 3);

  KeyExchangeMsg kem3;
  kem3.key = "d";
  kem3.signature = "4";
  kem3.repID = 3;
  cks.push(kem3, 30);

  KeyExchangeMsg kem4;
  kem4.key = "e";
  kem4.signature = "3";
  kem4.repID = 4;
  cks.push(kem4, 45);

  KeyExchangeMsg kem5;
  kem5.key = "f";
  kem5.signature = "4";
  kem5.repID = 4;
  cks.push(kem5, 80);

  KeyExchangeMsg kem6;
  kem6.key = "f";
  kem6.signature = "4";
  kem6.repID = 5;
  cks.push(kem6, 155);

  // Checkpoint too close
  ASSERT_EQ(cks.rotate(1).empty(), true);
  // Should trigger two rotations at 3 and 4
  ASSERT_EQ(cks.rotate(2).empty(), false);
}

struct DummyClient : public IInternalBFTClient {
  inline NodeIdType getClientId() const { return 1; };
  void sendRquest(uint8_t flags, uint32_t requestLength, const char* request, const std::string& cid) {}
  uint32_t numOfConnectedReplicas(uint32_t clusterSize) { return clusterSize; }
  bool isUdp() { return false; }
};

TEST(KeyExchangeManager, initialKeyExchange) {
  uint32_t clusterSize = 4;
  std::shared_ptr<IInternalBFTClient> dc(new DummyClient());
  DummyKeyGen dkg{clusterSize};
  dkg.prv = "private";
  dkg.pub = "public";
  ReservedPagesMock rpm(ReservedPagesClientBase::totalNumberOfPages() + clusterSize, true);
  concordUtil::Timers timers;

  KeyExchangeManager::InitData id{};
  id.cl = dc;
  id.id = 3;
  id.clusterSize = clusterSize;
  id.reservedPages = &rpm;
  id.sizeOfReservedPage = 4096;
  id.timers = &timers;
  id.kg = &dkg;
  id.ke = &dkg;
  id.sec = std::shared_ptr<ISecureStore>(new DummyLoaderSaver());
  TestKeyManager test{&id};
  // get the pub and prv keys from the key handlr and set them to be rotated.
  test.km_.sendInitialKey();
  test.km_.futureRet.get();
  // set public of replica 0
  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 0;
  test.km_.onKeyExchange(kem, 2);

  // set public of replica 1
  KeyExchangeMsg kem2;
  kem2.key = "b";
  kem2.signature = "11";
  kem2.repID = 1;
  test.km_.onKeyExchange(kem2, 3);

  // set public of replica 3 and promote its private
  KeyExchangeMsg kem4;
  kem4.key = "public";
  kem4.signature = "1";
  kem4.repID = 3;
  test.km_.onKeyExchange(kem4, 5);

  // will return true to slow path and will ignore this msg
  KeyExchangeMsg kem3;
  // set to fast path
  kem3.key = "c";
  kem3.signature = "w1";
  kem3.repID = 2;
  test.km_.onKeyExchange(kem3, 5);
  ASSERT_EQ(test.getKeyExchangedOnStartCounter(), 4);
  ASSERT_EQ(test.km_.keysExchanged, true);
  // check notification
  ASSERT_EQ(dkg.selfprv, "private");
  ASSERT_EQ(dkg.selfpub, "public");
  ASSERT_EQ(dkg.pubs[0], "a");
  ASSERT_EQ(dkg.pubs[1], "b");
  ASSERT_EQ(dkg.pubs[2], "c");
}

TEST(KeyExchangeManager, endToEnd) {
  uint32_t clustersize{4};
  std::shared_ptr<IInternalBFTClient> dc(new DummyClient());
  DummyKeyGen dkg{clustersize};
  dkg.prv = "private";
  dkg.pub = "public";
  ReservedPagesMock rpm(ReservedPagesClientBase::totalNumberOfPages() + clustersize, true);
  concordUtil::Timers timers;

  KeyExchangeManager::InitData id{};
  id.cl = dc;
  id.id = 2;
  id.clusterSize = clustersize;
  id.reservedPages = &rpm;
  id.sizeOfReservedPage = 4096;
  id.timers = &timers;
  id.kg = &dkg;
  id.ke = &dkg;
  id.sec = std::shared_ptr<ISecureStore>(new DummyLoaderSaver());
  TestKeyManager test{&id};

  // set published private key of replica 2
  test.km_.sendInitialKey();
  test.km_.futureRet.get();

  // set public of replica 0
  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 0;
  test.km_.onKeyExchange(kem, 2);

  // set public of replica 1
  KeyExchangeMsg kem2;
  kem2.key = "b";
  kem2.signature = "11";
  kem2.repID = 1;
  test.km_.onKeyExchange(kem2, 3);

  // set public of replica 2 and promote private
  KeyExchangeMsg kem3;
  kem3.key = "public";
  kem3.signature = "w1";
  kem3.repID = 2;
  test.km_.onKeyExchange(kem3, 4);

  // set public of replica 3
  KeyExchangeMsg kem4;
  kem4.key = "d";
  kem4.signature = "1";
  kem4.repID = 3;
  ASSERT_EQ(test.km_.keysExchanged, false);
  test.km_.onKeyExchange(kem4, 5);
  ASSERT_EQ(test.km_.keysExchanged, true);

  ASSERT_EQ(test.km_.getReplicaPublicKey(0).key, "a");
  ASSERT_EQ(test.km_.getReplicaPublicKey(1).key, "b");
  ASSERT_EQ(test.km_.getReplicaPublicKey(2).key, "public");
  ASSERT_EQ(test.km_.getReplicaPublicKey(3).key, "d");

  // check notification
  ASSERT_EQ(dkg.selfprv, "private");
  ASSERT_EQ(dkg.selfpub, "public");
  ASSERT_EQ(dkg.pubs[0], "a");
  ASSERT_EQ(dkg.pubs[1], "b");
  ASSERT_EQ(dkg.pubs[3], "d");
  // Should not rotate any key
  test.km_.onCheckpoint(2);
  ASSERT_EQ(test.getPublicKeyRotated(), 0);
  ASSERT_EQ(test.km_.getReplicaPublicKey(0).key, "a");
  ASSERT_EQ(test.km_.getReplicaPublicKey(1).key, "b");
  ASSERT_EQ(test.km_.getReplicaPublicKey(2).key, "public");
  ASSERT_EQ(test.km_.getReplicaPublicKey(3).key, "d");
  dkg.prv = "private2";
  dkg.pub = "public2";
  // set published private key of replica 2
  test.km_.sendKeyExchange();

  // rotation candidate for replica 0
  KeyExchangeMsg kem5;
  kem5.key = "aaaa";
  kem5.signature = "1";
  kem5.repID = 0;
  test.km_.onKeyExchange(kem5, 22);

  // rotation candidate for replica 1
  KeyExchangeMsg kem6;
  kem6.key = "bbbb";
  kem6.signature = "11";
  kem6.repID = 1;
  test.km_.onKeyExchange(kem6, 33);

  // rotation candidate for replica 2 promote private
  KeyExchangeMsg kem7;
  kem7.key = "public2";
  kem7.signature = "w1";
  kem7.repID = 2;
  test.km_.onKeyExchange(kem7, 34);
  // Checkpoint too close, should not rotate
  test.km_.onCheckpoint(1);
  ASSERT_EQ(test.getPublicKeyRotated(), 0);
  ASSERT_EQ(test.km_.getPrivateKey(), "private");
  ASSERT_EQ(test.km_.getReplicaPublicKey(0).key, "a");
  ASSERT_EQ(test.km_.getReplicaPublicKey(1).key, "b");
  ASSERT_EQ(test.km_.getReplicaPublicKey(2).key, "public");
  // check notification
  ASSERT_EQ(dkg.selfprv, "private");
  ASSERT_EQ(dkg.selfpub, "public");
  ASSERT_EQ(dkg.pubs[0], "a");
  ASSERT_EQ(dkg.pubs[1], "b");
  ASSERT_EQ(dkg.pubs[3], "d");

  // rotation candidate for replica 3
  KeyExchangeMsg kem8;
  kem8.key = "ddddd";
  kem8.signature = "w1";
  kem8.repID = 3;
  test.km_.onKeyExchange(kem8, 180);

  // now should rotate 0,1,2
  test.km_.onCheckpoint(2);
  ASSERT_EQ(test.getPublicKeyRotated(), 3);
  ASSERT_EQ(test.km_.getPrivateKey(), "private2");
  ASSERT_EQ(test.km_.getReplicaPublicKey(0).key, "aaaa");
  ASSERT_EQ(test.km_.getReplicaPublicKey(1).key, "bbbb");
  ASSERT_EQ(test.km_.getReplicaPublicKey(2).key, "public2");
  ASSERT_EQ(test.km_.getReplicaPublicKey(3).key, "d");
  ASSERT_EQ(dkg.selfprv, "private2");
  ASSERT_EQ(dkg.selfpub, "public2");
  ASSERT_EQ(dkg.pubs[0], "aaaa");
  ASSERT_EQ(dkg.pubs[1], "bbbb");
  ASSERT_EQ(dkg.pubs[3], "d");
}

TEST(ClusterKeyStore, dirty_first_load) {
  uint32_t clustersize{4};
  ReservedPagesMock rpm(ReservedPagesClientBase::totalNumberOfPages() + clustersize, false);
  ClusterKeyStore cks{4, 4094};

  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(cks.numKeys(i), 0);
  }
}

TEST(ClusterKeyStore, dirty_first_load_save_keys_and_reload) {
  uint32_t clustersize{4};
  ReservedPagesMock rpm(ReservedPagesClientBase::totalNumberOfPages() + clustersize, false);
  ClusterKeyStore cks{4, 4094};

  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 0;
  cks.push(kem, 2);

  KeyExchangeMsg kem2;
  kem2.key = "b";
  kem2.signature = "12";
  kem2.repID = 1;
  cks.push(kem2, 2);

  KeyExchangeMsg kem3;
  kem3.key = "c";
  kem3.signature = "13";
  kem3.repID = 2;
  cks.push(kem3, 2);

  KeyExchangeMsg kem4;
  kem4.key = "d";
  kem4.signature = "14";
  kem4.repID = 3;
  cks.push(kem4, 2);

  KeyExchangeMsg kem5;
  kem5.key = "aa";
  kem5.signature = "15";
  kem5.repID = 0;
  cks.push(kem5, 2);

  ClusterKeyStore reloadCks{4, 4094};
  for (int i = 0; i < 4; i++) {
    if (i == 0) {
      ASSERT_EQ(reloadCks.numKeys(i), 2);
      continue;
    }
    ASSERT_EQ(reloadCks.numKeys(i), 1);
  }
}

TEST(ClusterKeyStore, clean_first_load_save_keys_rotate_and_reload) {
  uint32_t clustersize = 4;
  ReservedPagesMock rpm(ReservedPagesClientBase::totalNumberOfPages() + clustersize, false);
  ClusterKeyStore cks{4, 4094};

  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 0;
  cks.push(kem, 2);

  KeyExchangeMsg kem2;
  kem2.key = "b";
  kem2.signature = "12";
  kem2.repID = 1;
  cks.push(kem2, 2);

  KeyExchangeMsg kem3;
  kem3.key = "c";
  kem3.signature = "13";
  kem3.repID = 2;
  cks.push(kem3, 2);

  KeyExchangeMsg kem4;
  kem4.key = "d";
  kem4.signature = "14";
  kem4.repID = 3;
  cks.push(kem4, 2);

  KeyExchangeMsg kem5;
  kem5.key = "aa";
  kem5.signature = "15";
  kem5.repID = 0;
  cks.push(kem5, 3);

  KeyExchangeMsg kem6;
  kem6.key = "bb";
  kem6.signature = "15";
  kem6.repID = 1;
  cks.push(kem6, 190);

  cks.rotate(2);

  ClusterKeyStore reloadCks{4, 4094};
  ASSERT_EQ(reloadCks.exchangedReplicas.size(), 4);
  for (int i = 0; i < 4; i++) {
    if (i == 0) {
      ASSERT_EQ(reloadCks.getReplicaPublicKey(i).key, "aa");
      ASSERT_EQ(reloadCks.getReplicaPublicKey(i).signature, "15");
    }
    if (i == 1) {
      ASSERT_EQ(reloadCks.numKeys(i), 2);
      ASSERT_EQ(reloadCks.getReplicaPublicKey(i).key, "b");
      continue;
    }
    ASSERT_EQ(reloadCks.numKeys(i), 1);
  }
}
TEST(PrivateKeys, ser_der) {
  std::shared_ptr<ISecureStore> dls(new DummyLoaderSaver());
  KeyExchangeManager::KeysView keys{dls, nullptr, 4};
  keys.data.generatedPrivateKey = "publish";
  keys.data.outstandingPrivateKey = "outstandingPrivateKey";
  keys.data.privateKey = "privateKey";

  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, keys.data);
  auto strMsg = ss.str();

  KeyExchangeManager::KeysView keys2{dls, nullptr, 4};

  ss.write(strMsg.c_str(), std::streamsize(strMsg.size()));
  concord::serialize::Serializable::deserialize(ss, keys2.data);

  ASSERT_EQ(keys.data.generatedPrivateKey, keys2.data.generatedPrivateKey);
  ASSERT_EQ(keys.data.outstandingPrivateKey, keys2.data.outstandingPrivateKey);
  ASSERT_EQ(keys.data.privateKey, keys2.data.privateKey);
}

TEST(PrivateKeys, SaveLoad) {
  std::shared_ptr<ISecureStore> dls(new DummyLoaderSaver());
  KeyExchangeManager::KeysView keys{dls, nullptr, 4};
  keys.data.generatedPrivateKey = "publish";
  keys.data.outstandingPrivateKey = "outstandingPrivateKey";
  keys.data.privateKey = "privateKey";

  keys.save();
  keys.data.generatedPrivateKey = "";
  keys.data.outstandingPrivateKey = "";
  keys.data.privateKey = "";

  keys.load();

  ASSERT_EQ(keys.data.generatedPrivateKey, "publish");
  ASSERT_EQ(keys.data.outstandingPrivateKey, "outstandingPrivateKey");
  ASSERT_EQ(keys.data.privateKey, "privateKey");
}

TEST(KeyExchangeManager, reserved_pages) {}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace bftEngine::impl
