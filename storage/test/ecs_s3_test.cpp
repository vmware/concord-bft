// Copyright 2019 VMware, all rights reserved
//
// Unit tests for ECS S3 object store replication
// IMPORTANT: the test assume that the S3 storage is up.

#include <memory>
#include "gtest/gtest.h"
#include "object_store/object_store_client.hpp"
#include "s3/client.hpp"

using namespace concord::storage;

namespace concord::storage::test {

/**
 * This class is used to tell the ObjectStore client impl to simulate network failure for predefined
 * ammount of time. Also it can be used for controlling deletion of objects (useful for tests)
 */
class ObjectStoreBehavior {
 private:
  bool allowNetworkFailure_ = false;
  std::chrono::time_point<std::chrono::steady_clock> startTimeForNetworkFailure_;
  bool doNetworkFailure_ = false;
  uint64_t doNetworkFailureDurMilli_ = 0;

 public:
  ObjectStoreBehavior(bool canSimulateNetFailure) : allowNetworkFailure_{canSimulateNetFailure} {}
  // set network failure flag and duration
  void set_do_net_failure(bool value, uint64_t durationMilli) {
    doNetworkFailure_ = value;
    doNetworkFailureDurMilli_ = durationMilli;
    startTimeForNetworkFailure_ = std::chrono::steady_clock::now();
  }
  bool get_do_net_failure() const { return doNetworkFailure_; }
  // used to assert whether its possible to simulate network failure
  bool can_simulate_net_failure() const { return allowNetworkFailure_; }
  // returns whether the network failure period has been elapsed
  bool has_finished() const {
    std::chrono::time_point<std::chrono::steady_clock> time = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(time - startTimeForNetworkFailure_).count();
    return diff > 0 && (uint64_t)diff > doNetworkFailureDurMilli_;
  }
};
/**
 * Ecs S3 test client
 */
class EcsS3TestClient : public concord::storage::s3::Client {
 public:
  EcsS3TestClient(const S3_StoreConfig &config, const ObjectStoreBehavior &b)
      : concord::storage::s3::Client(config), bh_{b} {}

  Status del(const Sliver &key) override {
    if (should_net_fail()) return Status::GeneralError("Network failure simulated");
    return concord::storage::s3::Client::del(key);
  }

  Status get_internal(const Sliver &_key, OUT Sliver &_outValue) const override {
    if (should_net_fail()) return Status::GeneralError("Network failure simulated");
    return concord::storage::s3::Client::get_internal(_key, _outValue);
  }

  Status put_internal(const Sliver &_key, const Sliver &_value) override {
    if (this->should_net_fail()) return Status::GeneralError("Network failure simulated");
    return concord::storage::s3::Client::put_internal(_key, _value);
  }

  Status object_exists_internal(const Sliver &key) const override {
    if (should_net_fail()) return Status::GeneralError("Network failure simulated");
    return concord::storage::s3::Client::object_exists_internal(key);
  }
  Status test_bucket_internal() override {
    if (should_net_fail()) return Status::GeneralError("Network failure simulated");
    return concord::storage::s3::Client::test_bucket_internal();
  }

 protected:
  bool should_net_fail() const {
    bool b1 = bh_.can_simulate_net_failure();
    if (!b1) return false;
    bool b3 = bh_.get_do_net_failure();
    if (!b3) return false;
    bool b2 = bh_.has_finished();
    if (b2) return false;
    return true;
  }

 protected:
  ObjectStoreBehavior bh_;
};
/**
 * object store test client
 */
class ObjectStoreTestClient : public concord::storage::ObjectStoreClient {
 public:
  ObjectStoreTestClient(IDBClient *impl) : concord::storage::ObjectStoreClient(impl) {}
  Status test_bucket() { return dynamic_pointer_cast<EcsS3TestClient>(pImpl_)->test_bucket(); }

 protected:
};

/**
 * Ecs test fixture
 */
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

  void SetClient(ObjectStoreBehavior b = ObjectStoreBehavior(false)) {
    client.reset(new ObjectStoreTestClient(new EcsS3TestClient(config, b)));
    client->init();
  }

  void SetBucketName(std::string name) { config.bucketName = name; }

  S3_StoreConfig config;
  std::shared_ptr<ObjectStoreTestClient> client = nullptr;
};

/**
 * @brief put and get regular string object
 *
 */
TEST_F(EcsS3Test, PutGetStringObject) {
  SetClient();

  Sliver key("unit_test_key");
  Sliver value("unit_test_object");
  Status s = client->put(key, value);

  ASSERT_EQ(s, Status::OK());
  Sliver rvalue;
  s = client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_EQ(value, rvalue);
}

/**
 * @brief put and get several regular string objects in batch
 *
 */
TEST_F(EcsS3Test, multiPut) {
  SetClient();
  SetOfKeyValuePairs pairs;
  for (int i = 0; i < 10; ++i) pairs["multiPutKey" + std::to_string(i)] = "multiPutValue" + std::to_string(i);

  ASSERT_EQ(client->multiPut(pairs), Status::OK());

  KeysVector keys;
  for (int i = 0; i < 10; ++i) keys.push_back("multiPutKey" + std::to_string(i));

  ValuesVector values(10);
  ASSERT_EQ(client->multiGet(keys, values), Status::OK());
  for (int i = 0; i < 10; ++i)
    ASSERT_STRCASEEQ(values[i].toString().c_str(), std::string("multiPutValue" + std::to_string(i)).c_str());

  ASSERT_EQ(client->multiDel(keys), Status::OK());
}

/**
 * @brief put and get regular string object with retry
 *
 */
TEST_F(EcsS3Test, PutGetStringObjectRetryOK) {
  ObjectStoreBehavior b(true);
  b.set_do_net_failure(true, config.maxWaitTime / 2);
  SetClient(b);

  Sliver key("unit_test_key");
  Sliver value("unit_test_object");

  Status s = client->put(key, value);

  ASSERT_EQ(s, Status::OK());
  Sliver rvalue;
  s = client->get(key, rvalue);
  ASSERT_TRUE(s.isOK());
  ASSERT_EQ(value, rvalue);
}

/**
 * @brief try to put object into non existing bucket
 *
 */
TEST_F(EcsS3Test, PutObjectBucketNotExists) {
  SetBucketName("non_existing_bucket");
  SetClient();

  Sliver key("unit_test_key");
  Sliver value("unit_test_object");
  Status s = client->put(key, value);
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
  s = client->has(key);
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
  s = client->has(key2);
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
  ObjectStoreBehavior b{true};
  b.set_do_net_failure(true, config.maxWaitTime / 2);
  SetClient(b);
  Status s = client->test_bucket();
  ASSERT_EQ(s, Status::OK());
}

TEST_F(EcsS3Test, TestBucketNotExists) {
  SetBucketName("non_existing_bucket");
  SetClient();
  Status s = client->test_bucket();
  ASSERT_TRUE(s.isNotFound());
}

}  // namespace concord::storage::test
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto exitCode = RUN_ALL_TESTS();
  return exitCode;
}
