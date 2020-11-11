#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "storage/db_interface.h"
#include "rocksdb/native_client.h"
#include "sliver.hpp"
#include "storage/test/storage_test_common.h"

#include <string_view>
#include <utility>

namespace {

using namespace concord::storage;
using namespace concord::storage::rocksdb;
using namespace ::testing;
using namespace std::literals;

class native_rocksdb_test : public ::testing::Test {
  void SetUp() override {
    cleanup();
    db = TestRocksDb::createNative();
  }
  void TearDown() override { cleanup(); }

 protected:
  std::shared_ptr<NativeClient> db;
  const std::string key{"key"};
  const std::string value{"value"};
  const std::string key1{"key1"};
  const std::string value1{"value1"};
  const std::string key2{"key2"};
  const std::string value2{"value2"};
  const std::string key3{"key3"};
  const std::string value3{"value3"};

  template <typename T>
  static Sliver toSliver(const T &v) {
    return Sliver::copy(v.data(), v.size());
  }
};

TEST_F(native_rocksdb_test, empty_db_has_default_family_only) {
  const auto families = db->columnFamilies();
  ASSERT_EQ(families.size(), 1);
  ASSERT_EQ(*families.begin(), NativeClient::defaultColumnFamily());
}

TEST_F(native_rocksdb_test, create_families) {
  db->createColumnFamily("cf1");
  db->createColumnFamily("cf2");
  db->createColumnFamily("cf3");
  ASSERT_THAT(db->columnFamilies(),
              ContainerEq(std::unordered_set<std::string>{db->defaultColumnFamily(), "cf1", "cf2", "cf3"}));
}

TEST_F(native_rocksdb_test, creating_a_family_twice_is_not_an_error) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  ASSERT_NO_THROW(db->createColumnFamily(cf));
  ASSERT_THAT(db->columnFamilies(), ContainerEq(std::unordered_set<std::string>{db->defaultColumnFamily(), "cf"}));
}

TEST_F(native_rocksdb_test, drop_family_and_list) {
  db->createColumnFamily("cf1");
  db->createColumnFamily("cf2");
  db->dropColumnFamily("cf2");
  ASSERT_THAT(db->columnFamilies(), ContainerEq(std::unordered_set<std::string>{db->defaultColumnFamily(), "cf1"}));
}

TEST_F(native_rocksdb_test, dropping_a_family_twice_is_not_an_error) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->dropColumnFamily(cf);
  ASSERT_NO_THROW(db->dropColumnFamily(cf));
  ASSERT_THAT(db->columnFamilies(), ContainerEq(std::unordered_set<std::string>{db->defaultColumnFamily()}));
}

TEST_F(native_rocksdb_test, single_key_api_throws_on_non_existent_family) {
  const auto cf = "cf"s;
  ASSERT_THROW(db->get(cf, key), RocksDBException);
  ASSERT_THROW(db->put(cf, key, value), RocksDBException);
  ASSERT_THROW(db->del(cf, key), RocksDBException);
}

TEST_F(native_rocksdb_test, put_in_default_family) {
  db->put(key, value);

  // Ensure key is in the default family via the non-family interface.
  {
    const auto dbValue = db->get(key);
    ASSERT_TRUE(dbValue.has_value());
    ASSERT_EQ(*dbValue, value);
  }

  // Ensure key is in the default family via the family interface.
  {
    const auto dbValue = db->get(db->defaultColumnFamily(), key);
    ASSERT_TRUE(dbValue.has_value());
    ASSERT_EQ(*dbValue, value);
  }
}

TEST_F(native_rocksdb_test, put_with_different_key_and_value_types) {
  db->put("key"sv, "value"s);
  const auto dbValue = db->get("key"s);
  ASSERT_TRUE(dbValue.has_value());
  ASSERT_EQ(*dbValue, "value");
}

TEST_F(native_rocksdb_test, del_from_default_family) {
  db->put(key, value);
  db->del(key);
  ASSERT_FALSE(db->get(key).has_value());
}

TEST_F(native_rocksdb_test, put_in_created_family) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(cf, key, value);
  const auto dbValue = db->get(cf, key);
  ASSERT_TRUE(dbValue.has_value());
  ASSERT_EQ(*dbValue, value);
  // Ensure the key is not the default family.
  ASSERT_FALSE(db->get(key).has_value());
}

TEST_F(native_rocksdb_test, del_from_created_family) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  // Put in both the default and the created one.
  db->put(key, value);
  db->put(cf, key, value);
  db->del(cf, key);
  const auto defaultValue = db->get(key);
  ASSERT_TRUE(defaultValue.has_value());
  ASSERT_EQ(*defaultValue, value);
  ASSERT_FALSE(db->get(cf, key).has_value());
}

TEST_F(native_rocksdb_test, del_non_existent_key_is_not_an_error) { ASSERT_NO_THROW(db->del(key)); }

TEST_F(native_rocksdb_test, drop_family_and_get) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(cf, key, value);
  db->dropColumnFamily(cf);
  ASSERT_THROW(db->get(cf, key), RocksDBException);
}

TEST_F(native_rocksdb_test, drop_family_then_create_and_ensure_empty) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(cf, key, value);
  db->dropColumnFamily(cf);
  db->createColumnFamily(cf);
  ASSERT_FALSE(db->get(cf, key).has_value());
}

TEST_F(native_rocksdb_test, put_in_batch_in_default_family) {
  auto batch = db->getBatch();
  batch.put(key1, value1);
  batch.put(key2, value2);
  db->write(std::move(batch));
  const auto dbValue1 = db->get(key1);
  const auto dbValue2 = db->get(key2);
  ASSERT_TRUE(dbValue1.has_value());
  ASSERT_EQ(*dbValue1, value1);
  ASSERT_TRUE(dbValue2.has_value());
  ASSERT_EQ(*dbValue2, value2);
}

TEST_F(native_rocksdb_test, put_in_batch_in_2_families) {
  const auto cf1 = "cf1"s;
  const auto cf2 = "cf2"s;
  db->createColumnFamily(cf1);
  db->createColumnFamily(cf2);
  auto batch = db->getBatch();
  db->put(cf1, key1, value1);
  db->put(cf1, key2, value2);
  db->put(cf2, key2, value2);
  db->put(cf2, key3, value3);
  db->write(std::move(batch));

  // cf1
  {
    const auto dbValue1 = db->get(cf1, key1);
    const auto dbValue2 = db->get(cf1, key2);
    ASSERT_TRUE(dbValue1.has_value());
    ASSERT_EQ(*dbValue1, value1);
    ASSERT_TRUE(dbValue2.has_value());
    ASSERT_EQ(*dbValue2, value2);
  }

  // cf2
  {
    const auto dbValue2 = db->get(cf2, key2);
    const auto dbValue3 = db->get(cf2, key3);
    ASSERT_TRUE(dbValue2.has_value());
    ASSERT_EQ(*dbValue2, value2);
    ASSERT_TRUE(dbValue3.has_value());
    ASSERT_EQ(*dbValue3, value3);
  }
}

TEST_F(native_rocksdb_test, del_non_existent_key_in_batch_is_not_an_error) {
  auto batch = db->getBatch();
  batch.del(key);
  ASSERT_NO_THROW(db->write(std::move(batch)));
}

TEST_F(native_rocksdb_test, multiple_deletes_for_same_key_in_batch) {
  db->put(key, value);
  auto batch = db->getBatch();
  batch.del(key);
  batch.del(key);
  ASSERT_NO_THROW(db->write(std::move(batch)));
  ASSERT_FALSE(db->get(key).has_value());
}

TEST_F(native_rocksdb_test, batch_operations_honor_order) {
  // put -> delete
  {
    auto batch = db->getBatch();
    batch.put(key, value);
    batch.del(key);
    db->write(std::move(batch));
    ASSERT_FALSE(db->get(key).has_value());
  }

  // put -> delete -> put
  {
    auto batch = db->getBatch();
    batch.put(key, value);
    batch.del(key);
    batch.put(key, value2);
    db->write(std::move(batch));
    const auto dbValue = db->get(key);
    ASSERT_TRUE(dbValue.has_value());
    ASSERT_EQ(*dbValue, value2);
  }
}

TEST_F(native_rocksdb_test, put_container_in_batch_in_default_family) {
  const auto kvSet = SetOfKeyValuePairs{std::make_pair(toSliver(key1), toSliver(value1)),
                                        std::make_pair(toSliver(key2), toSliver(value2))};
  auto batch = db->getBatch();
  putInBatch(batch, kvSet);
  db->write(std::move(batch));
  const auto dbValue1 = db->get(key1);
  ASSERT_TRUE(dbValue1.has_value());
  ASSERT_EQ(*dbValue1, value1);
  const auto dbValue2 = db->get(key2);
  ASSERT_TRUE(dbValue2.has_value());
  ASSERT_EQ(*dbValue2, value2);
}

TEST_F(native_rocksdb_test, put_container_in_batch_in_a_family) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  const auto kvSet = SetOfKeyValuePairs{std::make_pair(toSliver(key1), toSliver(value1)),
                                        std::make_pair(toSliver(key2), toSliver(value2))};
  auto batch = db->getBatch();
  putInBatch(batch, cf, kvSet);
  db->write(std::move(batch));
  const auto dbValue1 = db->get(cf, key1);
  ASSERT_TRUE(dbValue1.has_value());
  ASSERT_EQ(*dbValue1, value1);
  const auto dbValue2 = db->get(cf, key2);
  ASSERT_TRUE(dbValue2.has_value());
  ASSERT_EQ(*dbValue2, value2);
}

TEST_F(native_rocksdb_test, del_container_in_batch_in_default_family) {
  db->put(key1, value1);
  db->put(key2, value2);
  const auto kvVec = KeysVector{toSliver(key1), toSliver(key2)};
  auto batch = db->getBatch();
  delInBatch(batch, kvVec);
  db->write(std::move(batch));
  ASSERT_FALSE(db->get(key1).has_value());
  ASSERT_FALSE(db->get(key2).has_value());
}

TEST_F(native_rocksdb_test, del_container_in_batch_in_a_family) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(cf, key1, value1);
  db->put(cf, key2, value2);
  const auto kvVec = KeysVector{toSliver(key1), toSliver(key2)};
  auto batch = db->getBatch();
  delInBatch(batch, cf, kvVec);
  db->write(std::move(batch));
  ASSERT_FALSE(db->get(cf, key1).has_value());
  ASSERT_FALSE(db->get(cf, key2).has_value());
}

TEST_F(native_rocksdb_test, batch_del_range_in_default_family) {
  db->put(key1, value1);
  db->put(key2, value2);
  db->put(key3, value3);
  auto batch = db->getBatch();
  batch.delRange(key1, key3);
  db->write(std::move(batch));
  ASSERT_FALSE(db->get(key1).has_value());
  ASSERT_FALSE(db->get(key2).has_value());
  const auto dbValue3 = db->get(key3);
  ASSERT_TRUE(dbValue3.has_value());
  ASSERT_EQ(*dbValue3, value3);
}

TEST_F(native_rocksdb_test, batch_del_range_in_a_family) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(cf, key1, value1);
  db->put(cf, key2, value2);
  db->put(cf, key3, value3);
  auto batch = db->getBatch();
  batch.delRange(cf, key1, key3);
  db->write(std::move(batch));
  ASSERT_FALSE(db->get(cf, key1).has_value());
  ASSERT_FALSE(db->get(cf, key2).has_value());
  const auto dbValue3 = db->get(cf, key3);
  ASSERT_TRUE(dbValue3.has_value());
  ASSERT_EQ(*dbValue3, value3);
}

TEST_F(native_rocksdb_test, batch_del_invalid_range_in_default_family) {
  db->put(key1, value1);
  db->put(key2, value2);
  auto batch = db->getBatch();
  batch.delRange(key3, key1);
  db->write(std::move(batch));
  const auto dbValue1 = db->get(key1);
  ASSERT_TRUE(dbValue1.has_value());
  ASSERT_EQ(*dbValue1, value1);
  const auto dbValue2 = db->get(key2);
  ASSERT_TRUE(dbValue2.has_value());
  ASSERT_EQ(*dbValue2, value2);
}

TEST_F(native_rocksdb_test, batch_del_invalid_range_in_a_family) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(cf, key1, value1);
  db->put(cf, key2, value2);
  auto batch = db->getBatch();
  batch.delRange(cf, key3, key1);
  db->write(std::move(batch));
  const auto dbValue1 = db->get(cf, key1);
  ASSERT_TRUE(dbValue1.has_value());
  ASSERT_EQ(*dbValue1, value1);
  const auto dbValue2 = db->get(cf, key2);
  ASSERT_TRUE(dbValue2.has_value());
  ASSERT_EQ(*dbValue2, value2);
}

TEST_F(native_rocksdb_test, batch_for_non_existent_family_throws) {
  const auto cf = "cf"s;

  {
    auto batch = db->getBatch();
    ASSERT_THROW(batch.put(cf, key, value), RocksDBException);
    ASSERT_THROW(batch.del(cf, key), RocksDBException);
    ASSERT_THROW(batch.delRange(cf, key1, key2), RocksDBException);
  }
}

TEST_F(native_rocksdb_test, iterator_is_invalid_on_creation) {
  {
    auto it = db->getIterator();
    ASSERT_FALSE(it);
  }

  const auto cf1 = "cf1"s;
  {
    db->createColumnFamily(cf1);
    auto it = db->getIterator(cf1);
    ASSERT_FALSE(it);
  }

  const auto cf2 = "cf2"s;
  {
    db->createColumnFamily(cf2);
    auto iters = db->getIterators({cf1, cf2});
    ASSERT_EQ(iters.size(), 2);
    ASSERT_FALSE(iters[0]);
    ASSERT_FALSE(iters[1]);
  }
}

TEST_F(native_rocksdb_test, iterator_get_throws_for_non_existent_column_families) {
  const auto cf1 = "cf1"s;
  const auto cf2 = "cf2"s;

  ASSERT_THROW(db->getIterator(cf1), RocksDBException);
  ASSERT_THROW(db->getIterators({cf1, cf2}), RocksDBException);
}

TEST_F(native_rocksdb_test, iterate_all_keys_in_default_family) {
  db->put(key1, value1);
  db->put(key2, value2);
  auto it = db->getIterator();
  it.first();
  ASSERT_EQ(it.keyView(), key1);
  ASSERT_EQ(it.valueView(), value1);
  it.next();
  ASSERT_EQ(it.keyView(), key2);
  ASSERT_EQ(it.valueView(), value2);
}

TEST_F(native_rocksdb_test, iterate_all_keys_in_a_family) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(cf, key1, value1);
  db->put(cf, key2, value2);
  auto it = db->getIterator(cf);
  it.first();
  ASSERT_EQ(it.keyView(), key1);
  ASSERT_EQ(it.valueView(), value1);
  it.next();
  ASSERT_EQ(it.keyView(), key2);
  ASSERT_EQ(it.valueView(), value2);
}

TEST_F(native_rocksdb_test, iterator_going_past_last_is_invalid) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(key1, value1);
  db->put(cf, key2, value2);

  {
    auto it = db->getIterator();
    it.first();
    ASSERT_NO_THROW(it.next());
    ASSERT_FALSE(it);
  }

  {
    auto it = db->getIterator(cf);
    it.first();
    ASSERT_NO_THROW(it.next());
    ASSERT_FALSE(it);
  }
}

TEST_F(native_rocksdb_test, iterator_going_before_first_is_invalid) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(key1, value1);
  db->put(cf, key2, value2);

  {
    auto it = db->getIterator();
    it.first();
    ASSERT_NO_THROW(it.prev());
    ASSERT_FALSE(it);
  }

  {
    auto it = db->getIterator(cf);
    it.first();
    ASSERT_NO_THROW(it.prev());
    ASSERT_FALSE(it);
  }
}

TEST_F(native_rocksdb_test, iterator_key_values_and_views_are_equal) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(key1, value1);
  db->put(cf, key2, value2);

  {
    auto it = db->getIterator();
    it.first();
    ASSERT_EQ(it.key(), key1);
    ASSERT_EQ(it.value(), value1);
    ASSERT_EQ(it.key(), it.keyView());
    ASSERT_EQ(it.value(), it.valueView());
  }

  {
    auto it = db->getIterator(cf);
    it.first();
    ASSERT_EQ(it.key(), key2);
    ASSERT_EQ(it.value(), value2);
    ASSERT_EQ(it.key(), it.keyView());
    ASSERT_EQ(it.value(), it.valueView());
  }
}

TEST_F(native_rocksdb_test, iterator_seek_at_least_in_default_family) {
  db->put(key1, value1);
  db->put(key3, value3);
  auto it = db->getIterator();
  it.seekAtLeast(key2);
  ASSERT_TRUE(it);
  ASSERT_EQ(it.key(), key3);
  ASSERT_EQ(it.value(), value3);
}

TEST_F(native_rocksdb_test, iterator_seek_at_least_in_a_family) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(cf, key1, value1);
  db->put(cf, key3, value3);
  auto it = db->getIterator(cf);
  it.seekAtLeast(key2);
  ASSERT_TRUE(it);
  ASSERT_EQ(it.key(), key3);
  ASSERT_EQ(it.value(), value3);
}

TEST_F(native_rocksdb_test, iterator_seek_at_most_in_default_family) {
  db->put(key1, value1);
  db->put(key3, value3);
  auto it = db->getIterator();
  it.seekAtMost(key2);
  ASSERT_TRUE(it);
  ASSERT_EQ(it.key(), key1);
  ASSERT_EQ(it.value(), value1);
}

TEST_F(native_rocksdb_test, iterator_seek_at_most_in_a_family) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(cf, key1, value1);
  db->put(cf, key3, value3);
  auto it = db->getIterator(cf);
  it.seekAtMost(key2);
  ASSERT_TRUE(it);
  ASSERT_EQ(it.key(), key1);
  ASSERT_EQ(it.value(), value1);
}

TEST_F(native_rocksdb_test, iterator_seek_success_after_unsuccessful_seeks) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);
  db->put(key2, value);
  db->put(cf, key2, value);

  {
    auto it = db->getIterator();
    it.seekAtLeast(key3);
    ASSERT_FALSE(it);

    it.seekAtMost(key1);
    ASSERT_FALSE(it);

    it.seekAtLeast(key2);
    ASSERT_TRUE(it);
  }

  {
    auto it = db->getIterator(cf);
    it.seekAtLeast(key3);
    ASSERT_FALSE(it);

    it.seekAtMost(key1);
    ASSERT_FALSE(it);

    it.seekAtLeast(key2);
    ASSERT_TRUE(it);
  }
}

TEST_F(native_rocksdb_test, iterator_key_value_prev_next_throw_when_invalid) {
  const auto cf = "cf"s;
  db->createColumnFamily(cf);

  {
    auto it = db->getIterator();
    ASSERT_FALSE(it);
    ASSERT_THROW(it.key(), RocksDBException);
    ASSERT_THROW(it.value(), RocksDBException);
    ASSERT_THROW(it.keyView(), RocksDBException);
    ASSERT_THROW(it.valueView(), RocksDBException);
    ASSERT_THROW(it.prev(), RocksDBException);
    ASSERT_THROW(it.next(), RocksDBException);
  }

  {
    auto it = db->getIterator(cf);
    ASSERT_FALSE(it);
    ASSERT_THROW(it.key(), RocksDBException);
    ASSERT_THROW(it.value(), RocksDBException);
    ASSERT_THROW(it.keyView(), RocksDBException);
    ASSERT_THROW(it.valueView(), RocksDBException);
    ASSERT_THROW(it.prev(), RocksDBException);
    ASSERT_THROW(it.next(), RocksDBException);
  }
}

TEST_F(native_rocksdb_test, get_iterators) {
  const auto cf1 = "cf1"s;
  const auto cf2 = "cf2"s;
  db->createColumnFamily(cf1);
  db->createColumnFamily(cf2);
  db->put(cf1, key1, value1);
  db->put(cf2, key2, value2);

  auto iters = db->getIterators({cf1, cf2});

  iters[0].seekAtLeast(key1);
  ASSERT_TRUE(iters[0]);
  ASSERT_EQ(iters[0].key(), key1);
  ASSERT_EQ(iters[0].value(), value1);

  iters[1].seekAtLeast(key2);
  ASSERT_TRUE(iters[1]);
  ASSERT_EQ(iters[1].key(), key2);
  ASSERT_EQ(iters[1].value(), value2);
}

}  // namespace

int main(int argc, char *argv[]) {
  ::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
