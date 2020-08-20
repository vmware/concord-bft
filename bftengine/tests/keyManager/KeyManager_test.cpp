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

#include "KeyManager.h"
#include "gtest/gtest.h"
#include <chrono>
#include <thread>

using namespace std;
using namespace bftEngine;

struct ExchangerMock : public IKeyExchanger {
 public:
  std::vector<KeyExchangeMsg> vmsg;
  std::vector<KeyExchangeMsg> pushvmsg;
  virtual void onExchange(const KeyExchangeMsg& m) { vmsg.push_back(m); }
  virtual void onNewKey(const KeyExchangeMsg& m) { pushvmsg.push_back(m); }
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

TEST(ReplicaKeyStore, rotate_death_if_checkpoint_is_less) {
  ReplicaKeyStore rks;
  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 6;
  rks.push(kem, 1);
  KeyExchangeMsg kem2;
  kem2.key = "qwerty";
  kem2.signature = "123456";
  kem2.repID = 6;
  rks.push(kem2, 340);
  // Checkpoint can't be less than key exchange seq num
  ASSERT_DEATH(rks.rotate(2), ".*");
}

TEST(ReplicaKeyStore, rotate_death_if_checkpoint_is_more) {
  ReplicaKeyStore rks;
  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 6;
  rks.push(kem, 1);
  KeyExchangeMsg kem2;
  kem2.key = "qwerty";
  kem2.signature = "123456";
  kem2.repID = 6;
  rks.push(kem2, 340);

  ASSERT_DEATH(rks.rotate(5), ".*");
}

TEST(ClusterKeyStore, push) {
  ExchangerMock em;
  std::vector<IKeyExchanger*> v;
  v.push_back(&em);
  ClusterKeyStore cks{7};
  {
    KeyExchangeMsg kem;
    kem.key = "a";
    kem.signature = "1";
    kem.repID = 8;
    // replica id out of range
    ASSERT_EQ(cks.push(kem, 2, v), false);
    ASSERT_EQ(em.pushvmsg.size(), 0);
  }

  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 1;
  auto ok = cks.push(kem, 2, v);
  ASSERT_EQ(ok, true);
  ASSERT_EQ(em.pushvmsg.size(), 1);
  ASSERT_EQ(em.pushvmsg[0].key, "a");

  KeyExchangeMsg kem2;
  kem2.key = "c";
  kem2.signature = "3";
  kem2.repID = 3;
  cks.push(kem2, 3, v);
  ASSERT_EQ(em.pushvmsg.size(), 2);

  KeyExchangeMsg kem3;
  kem3.key = "d";
  kem3.signature = "4";
  kem3.repID = 3;
  cks.push(kem3, 3, v);
  ASSERT_EQ(em.pushvmsg.size(), 3);

  KeyExchangeMsg kem4;
  kem4.key = "e";
  kem4.signature = "4w";
  kem4.repID = 3;
  // limit excceds
  ASSERT_EQ(cks.push(kem4, 2, v), false);
  ASSERT_EQ(em.pushvmsg.size(), 3);

  auto msg = cks.replicaKey(3);
  ASSERT_EQ(msg.key, "c");
  ASSERT_EQ(msg.signature, "3");
}

TEST(ClusterKeyStore, rotate) {
  ClusterKeyStore cks{7};
  ExchangerMock em;
  std::vector<IKeyExchanger*> v;
  v.push_back(&em);
  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 1;
  cks.push(kem, 2, v);

  KeyExchangeMsg kem2;
  kem2.key = "c";
  kem2.signature = "3";
  kem2.repID = 3;
  cks.push(kem2, 3, v);

  KeyExchangeMsg kem3;
  kem3.key = "d";
  kem3.signature = "4";
  kem3.repID = 3;
  cks.push(kem3, 30, v);

  KeyExchangeMsg kem4;
  kem4.key = "e";
  kem4.signature = "3";
  kem4.repID = 4;
  cks.push(kem4, 45, v);

  KeyExchangeMsg kem5;
  kem5.key = "f";
  kem5.signature = "4";
  kem5.repID = 4;
  cks.push(kem5, 80, v);

  KeyExchangeMsg kem6;
  kem6.key = "f";
  kem6.signature = "4";
  kem6.repID = 5;
  cks.push(kem6, 155, v);

  // Checkpoint too close
  ASSERT_EQ(cks.rotate(1, v), false);
  // Should trigger two torations at 3 and 4
  ASSERT_EQ(cks.rotate(2, v), true);
  ASSERT_EQ(((ExchangerMock*)v[0])->vmsg.size(), 2);
  ASSERT_EQ(((ExchangerMock*)v[0])->vmsg[0].key, cks.replicaKey(3).key);
  ASSERT_EQ(((ExchangerMock*)v[0])->vmsg[1].key, cks.replicaKey(4).key);
}

TEST(KeyManager, endToEnd) {
  ExchangerMock em;
  KeyManager::get(nullptr, 3, 4);
  KeyManager::get().registerForNotification(&em);
  KeyExchangeMsg kem;
  kem.key = "a";
  kem.signature = "1";
  kem.repID = 0;
  KeyManager::get().onKeyExchange(kem, 2);
  KeyExchangeMsg kem2;
  kem2.key = "b";
  kem2.signature = "11";
  kem2.repID = 1;
  KeyManager::get().onKeyExchange(kem2, 3);
  KeyExchangeMsg kem3;
  kem3.key = "c";
  kem3.signature = "w1";
  kem3.repID = 2;
  KeyManager::get().onKeyExchange(kem3, 4);
  KeyExchangeMsg kem4;
  kem4.key = "d";
  kem4.signature = "1";
  kem4.repID = 3;
  KeyManager::get().onKeyExchange(kem4, 5);
  ASSERT_EQ(KeyManager::get().replicaKey(0).key, "a");
  ASSERT_EQ(KeyManager::get().replicaKey(1).key, "b");
  ASSERT_EQ(KeyManager::get().replicaKey(2).key, "c");
  ASSERT_EQ(KeyManager::get().replicaKey(3).key, "d");
  // Should not rotate any key
  KeyManager::get().onCheckpoint(2);
  ASSERT_EQ(KeyManager::get().replicaKey(0).key, "a");
  ASSERT_EQ(KeyManager::get().replicaKey(1).key, "b");
  ASSERT_EQ(KeyManager::get().replicaKey(2).key, "c");
  ASSERT_EQ(KeyManager::get().replicaKey(3).key, "d");
  ASSERT_EQ(em.vmsg.size(), 0);

  KeyExchangeMsg kem5;
  kem5.key = "aaaa";
  kem5.signature = "1";
  kem5.repID = 0;
  KeyManager::get().onKeyExchange(kem5, 22);
  KeyExchangeMsg kem6;
  kem6.key = "bbbb";
  kem6.signature = "11";
  kem6.repID = 1;
  KeyManager::get().onKeyExchange(kem6, 33);
  KeyExchangeMsg kem7;
  kem7.key = "ccccc";
  kem7.signature = "w1";
  kem7.repID = 2;
  KeyManager::get().onKeyExchange(kem7, 34);
  // Checkpoint too close, should not rotate
  KeyManager::get().onCheckpoint(1);
  ASSERT_EQ(KeyManager::get().replicaKey(0).key, "a");
  ASSERT_EQ(KeyManager::get().replicaKey(1).key, "b");
  ASSERT_EQ(KeyManager::get().replicaKey(2).key, "c");
  ASSERT_EQ(KeyManager::get().replicaKey(3).key, "d");
  ASSERT_EQ(em.vmsg.size(), 0);
  // now should rotate 0,1,2
  KeyManager::get().onCheckpoint(2);
  ASSERT_EQ(KeyManager::get().replicaKey(0).key, "aaaa");
  ASSERT_EQ(KeyManager::get().replicaKey(1).key, "bbbb");
  ASSERT_EQ(KeyManager::get().replicaKey(2).key, "ccccc");
  ASSERT_EQ(KeyManager::get().replicaKey(3).key, "d");
  ASSERT_EQ(em.vmsg.size(), 3);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}