// Copyright 2019 VMware, all rights reserved
//
// Unit tests for ECS S3 object store replication
// IMPORTANT: the test assume that the S3 storage is up.

#include "gtest/gtest.h"
#include "storage/test/s3_test_common.h"

namespace concord::storage::s3::test {

/** @brief put and get regular string object */
TEST_F(S3Test, PutGetStringObject) {
  Sliver key("unit_test_key");
  Sliver value("unit_test_object");
  Status s = state.client->put(key, value);
  ASSERT_EQ(s, Status::OK());

  Sliver rvalue;
  s = state.client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_EQ(value, rvalue);
}

/** @brief put and get several regular string objects in batch */
TEST_F(S3Test, multiPut) {
  SetOfKeyValuePairs pairs;
  for (int i = 0; i < 10; ++i) pairs["multiPutKey" + std::to_string(i)] = "multiPutValue" + std::to_string(i);

  ASSERT_EQ(state.client->multiPut(pairs), Status::OK());

  KeysVector keys;
  for (int i = 0; i < 10; ++i) keys.push_back("multiPutKey" + std::to_string(i));

  ValuesVector values(10);
  ASSERT_EQ(state.client->multiGet(keys, values), Status::OK());
  for (int i = 0; i < 10; ++i)
    ASSERT_STRCASEEQ(values[i].toString().c_str(), std::string("multiPutValue" + std::to_string(i)).c_str());

  ASSERT_EQ(state.client->multiDel(keys), Status::OK());
}

/** @brief get a non-existing object */
TEST_F(S3Test, GetObjectNotExists) {
  Sliver key("unit_test_key12345");
  Sliver value;
  Status s = state.client->get(key, value);
  ASSERT_TRUE(s.isNotFound());
  ASSERT_EQ(value.length(), 0);
}

/** @brief put and get binary object */
TEST_F(S3Test, PutGetBinaryObject) {
  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[250];
  for (int i = 0; i < 250; i++) _value[i] = i;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 250);
  Status s = state.client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  Sliver rvalue;
  s = state.client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_EQ(wvalue, rvalue);
}

/** @brief Put and get large binary object when 25000 is maximum preallocated memory for sending the data */
TEST_F(S3Test, PutGetLargeBinaryObject) {
  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[25000];
  for (int i = 0; i < 25000; i++) _value[i] = 'a' + 0;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 25000);
  Status s = state.client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  Sliver rvalue;
  s = state.client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_EQ(wvalue, rvalue);
}

/** @brief Put and get large binary object when 25000 is maximum preallocated
 *  memory for sending the data expecting the buffer to grow exactly 4 times
 */
TEST_F(S3Test, PutGetLargeBinaryObjectWithResize) {
  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[100000];
  for (int i = 0; i < 100000; i++) _value[i] = 'a' + 0;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 100000);
  Status s = state.client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  Sliver rvalue;
  s = state.client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_TRUE(wvalue == rvalue);
}

/** @brief Check retrieving object headers functionality */
TEST_F(S3Test, ObjectExists) {
  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[100000];
  for (int i = 0; i < 100000; i++) _value[i] = 'a' + 0;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 100000);
  Status s = state.client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  s = state.client->has(key);
  ASSERT_TRUE(s.isOK());
}

/** @brief Check retrieving object headers functionality */
TEST_F(S3Test, ObjectNotExists) {
  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[100000];
  for (int i = 0; i < 100000; i++) _value[i] = 'a' + 0;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 100000);
  Status s = state.client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  Sliver key2("non_existing_key");
  s = state.client->has(key2);
  ASSERT_TRUE(s.isNotFound());
}

/** @brief Check retrieving object headers functionality */
TEST_F(S3Test, TestBucketExists) {
  Status s = state.client->test_bucket();
  ASSERT_TRUE(s.isOK());
}

/** @brief Check the iterator functionality
 *  Note: S3 iterator returns md5(value) and not a value itself.
 *  In order to obtain a value, the get operation must be issued.
 *
 */
TEST_F(S3Test, Iterator) {
  SetOfKeyValuePairs pairs;
  for (int i = 0; i < 10; ++i) {
    std::string key("prefix/multiPutKey" + std::to_string(i));
    std::string val("multiPutValue" + std::to_string(i));
    pairs[Sliver::copy(key.data(), key.length())] = Sliver::copy(val.data(), val.length());
  }
  ASSERT_EQ(state.client->multiPut(pairs), Status::OK());

  auto it = state.client->getIteratorGuard();
  auto res = it->seekAtLeast(std::string("prefix"));
  while (!it->isEnd()) {
    LOG_INFO(GL, res.first.toString() << ": " << res.second.toString());
    Sliver value;
    ASSERT_EQ(state.client->get(res.first, value), Status::OK());
    ASSERT_EQ(pairs[res.first], value);
    res = it->next();
  }
}
}  // namespace concord::storage::s3::test
