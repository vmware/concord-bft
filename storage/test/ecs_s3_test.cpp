// Copyright 2019 VMware, all rights reserved
//
// Unit tests for ECS S3 object store replication
// IMPORTANT: the test assume that the S3 storage is up.

#include <memory>
#include "gtest/gtest.h"
#include "object_store/object_store_client.hpp"

using namespace concord::storage;

namespace {

class EcsS3Test : public ::testing::Test {
 protected:
  EcsS3Test() {
    config.bucketName = "blockchain-dev-asx";
    config.accessKey = "blockchain-dev";
    config.protocol = "HTTP";
    config.url = "10.70.30.244:9020";
    config.secretKey = "Rz0mdbUNGJBxqdzprn5XGSXPr2AfkgcQsYS4y698";
    config.maxWaitTime = 10000;
  }

  void SetClient(ObjectStoreBehavior b = ObjectStoreBehavior(false, false)) {
    client = std::make_shared<ObjectStoreClient>(config, b);
    client->init();
  }

  void SetBucketName(std::string name) { config.bucketName = name; }

  S3_StoreConfig config;
  std::shared_ptr<ObjectStoreClient> client = nullptr;
};

/**
 * @brief put and get regular string object
 *
 */
TEST_F(EcsS3Test, PutGetStringObject) {
  SetClient();

  Sliver key("unit_test_key");
  Sliver wvalue("unit_test_object");
  Status s = client->put(key, wvalue);

  ASSERT_EQ(s, Status::OK());
  Sliver rvalue;
  s = client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_EQ(wvalue, rvalue);
}

/**
 * @brief put and get regular string object with retry
 *
 */
TEST_F(EcsS3Test, PutGetStringObjectRetryOK) {
  ObjectStoreBehavior b(false, true);
  b.set_do_net_failure(true, config.maxWaitTime / 2);
  SetClient();

  Sliver key("unit_test_key");
  Sliver wvalue("unit_test_object");

  Status s = client->put(key, wvalue);

  ASSERT_EQ(s, Status::OK());
  Sliver rvalue;
  s = client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_EQ(wvalue, rvalue);
}

/**
 * @brief put and get regular string object with retry
 *
 */
TEST_F(EcsS3Test, PutGetStringObjectRetryFail) {
  ObjectStoreBehavior b{false, true};
  b.set_do_net_failure(true, config.maxWaitTime * 2);
  SetClient(b);

  Sliver key("unit_test_key");
  Sliver wvalue("unit_test_object");
  Status s = client->put(key, wvalue);

  ASSERT_EQ(s, Status::GeneralError("Network failure simulated"));
}

/**
 * @brief try to put object into non existing bucket
 *
 */
TEST_F(EcsS3Test, PutObjectBucketNotExists) {
  SetBucketName("non_existing_bucket");
  SetClient();

  Sliver key("unit_test_key");
  Sliver wvalue("unit_test_object");
  Status s = client->put(key, wvalue);
  ASSERT_TRUE(s.isNotFound());
}

/**
 * @brief try to get object from non existing bucket
 *
 */
TEST_F(EcsS3Test, GetObjectBucketNotExists) {
  SetBucketName("non_existing_bucket");
  SetClient();

  Sliver key("unit_test_key");
  Sliver v;
  Status s = client->get(key, v);
  ASSERT_TRUE(s.isNotFound());
  ASSERT_EQ(v.length(), 0);
}

TEST_F(EcsS3Test, GetObjectNotExists) {
  SetClient();

  Sliver key("unit_test_key12345");
  Sliver value;
  Status s = client->get(key, value);
  ASSERT_TRUE(s.isNotFound());
  ASSERT_EQ(value.length(), 0);
}

/**
 * @brief put and get binary object
 *
 */
TEST_F(EcsS3Test, PutGetBinaryObject) {
  SetClient();

  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[250];
  for (int i = 0; i < 250; i++) _value[i] = i;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 250);
  Status s = client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  Sliver rvalue;
  s = client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_EQ(wvalue, rvalue);
}

/**
 * @brief Put and get large binary object when 25000 is maximum preallocated
 * memory for sending the data
 *
 */
TEST_F(EcsS3Test, PutGetLargeBinaryObject) {
  SetClient();

  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[25000];
  for (int i = 0; i < 25000; i++) _value[i] = 'a' + 0;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 25000);
  Status s = client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  Sliver rvalue;
  s = client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_EQ(wvalue, rvalue);
}

/**
 * @brief Put and get large binary object when 25000 is maximum preallocated
 * memory for sending the data expecting the buffer to grow exactly 4 times
 *
 */
TEST_F(EcsS3Test, PutGetLargeBinaryObjectWithResize) {
  SetClient();

  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[100000];
  for (int i = 0; i < 100000; i++) _value[i] = 'a' + 0;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 100000);
  Status s = client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  Sliver rvalue;
  s = client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_TRUE(wvalue == rvalue);
}

/**
 * @brief Check retrieving object headers functionality
 *
 */
TEST_F(EcsS3Test, ObjectExists) {
  SetClient();

  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[100000];
  for (int i = 0; i < 100000; i++) _value[i] = 'a' + 0;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 100000);
  Status s = client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  s = client->object_exists(key);
  ASSERT_TRUE(s.isOK());
}

/**
 * @brief Check retrieving object headers functionality
 *
 */
TEST_F(EcsS3Test, ObjectNotExists) {
  SetClient();

  uint8_t *_key = new uint8_t[30];
  for (int i = 0; i < 30; i++) _key[i] = i + 1;

  uint8_t *_value = new uint8_t[100000];
  for (int i = 0; i < 100000; i++) _value[i] = 'a' + 0;

  Sliver key((const char *)_key, 30);
  Sliver wvalue((const char *)_value, 100000);
  Status s = client->put(key, wvalue);
  ASSERT_TRUE(s.isOK());
  Sliver key2("non_existing_key");
  s = client->object_exists(key2);
  ASSERT_TRUE(s.isNotFound());
}

/**
 * @brief Check retrieving object headers functionality
 *
 */
TEST_F(EcsS3Test, TestBucketExists) {
  SetClient();
  Status s = client->test_bucket();
  ASSERT_TRUE(s.isOK());
}

TEST_F(EcsS3Test, TestBucketExistsRetryOK) {
  ObjectStoreBehavior b{false, true};
  b.set_do_net_failure(true, config.maxWaitTime / 2);
  SetClient(b);
  Status s = client->test_bucket();
  ASSERT_EQ(s, Status::OK());
}

TEST_F(EcsS3Test, TestBucketExistsRetryFail) {
  ObjectStoreBehavior b{false, true};
  b.set_do_net_failure(true, config.maxWaitTime * 2);
  SetClient(b);
  Status s = client->test_bucket();
  ASSERT_EQ(s, Status::GeneralError("Network failure simulated"));
}

TEST_F(EcsS3Test, TestBucketNotExists) {
  SetBucketName("non_existing_bucket");
  SetClient();
  Status s = client->test_bucket();
  ASSERT_TRUE(s.isNotFound());
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto exitCode = RUN_ALL_TESTS();
  return exitCode;
}
