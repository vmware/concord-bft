// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"

#include "status.hpp"
#include "storage/test/storage_test_common.h"

#include <iterator>
#include <string>

// NOTE: All tests assume lexicographical key ordering.
namespace {

using namespace concordUtils;
using namespace concord::storage;

template <typename T>
struct partition_test : public testing::Test {
  using Db = T;

  void SetUp() override { Db::cleanup(); }
  void TearDown() override { Db::cleanup(); }
};

using Types = testing::Types<TestMemoryDb>;
TYPED_TEST_CASE(partition_test, Types, );

TYPED_TEST(partition_test, has_default_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partitions = db->partitions();
  ASSERT_EQ(partitions.size(), 1);
  ASSERT_EQ(*std::cbegin(partitions), db->defaultPartition());
}

TYPED_TEST(partition_test, add_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition = db->defaultPartition() + "1";
  ASSERT_EQ(db->addPartition(partition), Status::OK());
  const auto partitions_after = db->partitions();
  ASSERT_EQ(partitions_after.size(), 2);
  ASSERT_NE(partitions_after.find(partition), std::cend(partitions_after));
  ASSERT_NE(partitions_after.find(db->defaultPartition()), std::cend(partitions_after));
}

TYPED_TEST(partition_test, cannot_add_existing_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition = db->defaultPartition() + "1";
  ASSERT_EQ(db->addPartition(partition), Status::OK());
  ASSERT_EQ(db->addPartition(partition), Status::InvalidArgument(""));
  // The default and the added one are present.
  ASSERT_EQ(db->partitions().size(), 2);
}

TYPED_TEST(partition_test, drop_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition = db->defaultPartition() + "1";
  ASSERT_EQ(db->addPartition(partition), Status::OK());
  ASSERT_EQ(db->dropPartition(partition), Status::OK());
  const auto partitions_after = db->partitions();
  ASSERT_EQ(partitions_after.size(), 1);
  ASSERT_EQ(partitions_after.find(partition), std::cend(partitions_after));
  ASSERT_NE(partitions_after.find(db->defaultPartition()), std::cend(partitions_after));
}

TYPED_TEST(partition_test, cannot_drop_default_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  ASSERT_EQ(db->dropPartition(db->defaultPartition()), Status::IllegalOperation(""));
  const auto partitions = db->partitions();
  ASSERT_EQ(partitions.size(), 1);
  ASSERT_NE(partitions.find(db->defaultPartition()), std::cend(partitions));
}

TYPED_TEST(partition_test, put_same_key_in_multiple_partitions) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition1 = db->defaultPartition() + "1";
  const auto partition2 = db->defaultPartition() + "2";
  ASSERT_EQ(db->addPartition(partition1), Status::OK());
  ASSERT_EQ(db->addPartition(partition2), Status::OK());

  const auto key = getSliverOfSize(2);
  const auto value_in_default = getSliverOfSize(10, 'd');
  const auto value_in_1 = getSliverOfSize(10, '1');
  const auto value_in_2 = getSliverOfSize(10, '2');
  ASSERT_EQ(db->put(db->defaultPartition(), key, value_in_default), Status::OK());
  ASSERT_EQ(db->put(partition1, key, value_in_1), Status::OK());
  ASSERT_EQ(db->put(partition2, key, value_in_2), Status::OK());

  auto out = Sliver{};
  ASSERT_EQ(db->has(db->defaultPartition(), key), Status::OK());
  ASSERT_EQ(db->get(db->defaultPartition(), key, out), Status::OK());
  ASSERT_EQ(out, value_in_default);
  ASSERT_EQ(db->has(partition1, key), Status::OK());
  ASSERT_EQ(db->get(partition1, key, out), Status::OK());
  ASSERT_EQ(out, value_in_1);
  ASSERT_EQ(db->has(partition2, key), Status::OK());
  ASSERT_EQ(db->get(partition2, key, out), Status::OK());
  ASSERT_EQ(out, value_in_2);
}

TYPED_TEST(partition_test, delete_key_in_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition1 = db->defaultPartition() + "1";
  const auto partition2 = db->defaultPartition() + "2";
  ASSERT_EQ(db->addPartition(partition1), Status::OK());
  ASSERT_EQ(db->addPartition(partition2), Status::OK());

  const auto key = getSliverOfSize(2);
  const auto value_in_1 = getSliverOfSize(10, '1');
  const auto value_in_2 = getSliverOfSize(10, '2');
  ASSERT_EQ(db->put(partition1, key, value_in_1), Status::OK());
  ASSERT_EQ(db->put(partition2, key, value_in_2), Status::OK());

  auto out = Sliver{};
  ASSERT_EQ(db->del(partition1, key), Status::OK());
  ASSERT_EQ(db->get(partition1, key, out), Status::NotFound(""));
  ASSERT_EQ(db->get(partition2, key, out), Status::OK());
  ASSERT_EQ(out, value_in_2);
}

TYPED_TEST(partition_test, range_delete_in_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition1 = db->defaultPartition() + "1";
  const auto partition2 = db->defaultPartition() + "2";
  ASSERT_EQ(db->addPartition(partition1), Status::OK());
  ASSERT_EQ(db->addPartition(partition2), Status::OK());

  const auto a = getSliverOfSize(1, 'a');
  const auto b = getSliverOfSize(1, 'b');
  const auto c = getSliverOfSize(1, 'c');
  const auto in_value = getSliverOfSize(2, 'v');

  ASSERT_EQ(db->put(partition1, a, in_value), Status::OK());
  ASSERT_EQ(db->put(partition1, b, in_value), Status::OK());
  ASSERT_EQ(db->put(partition1, c, in_value), Status::OK());
  ASSERT_EQ(db->put(partition2, a, in_value), Status::OK());
  ASSERT_EQ(db->put(partition2, b, in_value), Status::OK());
  ASSERT_EQ(db->put(partition2, c, in_value), Status::OK());

  // Delete the [a, c) range in partition1.
  ASSERT_EQ(db->rangeDel(partition1, a, c), Status::OK());
  ASSERT_EQ(db->has(partition1, a), Status::NotFound(""));
  ASSERT_EQ(db->has(partition1, b), Status::NotFound(""));
  ASSERT_EQ(db->has(partition1, c), Status::OK());
  ASSERT_EQ(db->has(partition2, a), Status::OK());
  ASSERT_EQ(db->has(partition2, b), Status::OK());
  ASSERT_EQ(db->has(partition2, c), Status::OK());
}

TYPED_TEST(partition_test, drop_partition_destroys_data_in_dropped_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition1 = db->defaultPartition() + "1";
  const auto partition2 = db->defaultPartition() + "2";
  ASSERT_EQ(db->addPartition(partition1), Status::OK());
  ASSERT_EQ(db->addPartition(partition2), Status::OK());

  const auto key = getSliverOfSize(2);
  const auto value_in_1 = getSliverOfSize(10, '1');
  const auto value_in_2 = getSliverOfSize(10, '2');
  ASSERT_EQ(db->put(partition1, key, value_in_1), Status::OK());
  ASSERT_EQ(db->put(partition2, key, value_in_2), Status::OK());

  // Drop and add the partition.
  ASSERT_EQ(db->dropPartition(partition1), Status::OK());
  ASSERT_EQ(db->addPartition(partition1), Status::OK());

  auto out = Sliver{};
  ASSERT_EQ(db->get(partition1, key, out), Status::NotFound(""));
  ASSERT_EQ(db->get(partition2, key, out), Status::OK());
  ASSERT_EQ(out, value_in_2);
}

TYPED_TEST(partition_test, non_existing_partition_returns_invalid_argument) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition = db->defaultPartition() + "1";
  const auto key = getSliverOfSize(1);
  auto out_value = Sliver{};
  const auto keys = KeysVector{key};
  auto out_values = ValuesVector{};
  const auto kvs = SetOfKeyValuePairs{std::make_pair(key, Sliver{})};

  ASSERT_EQ(db->get(partition, key, out_value), Status::InvalidArgument(""));
  ASSERT_EQ(db->has(partition, key), Status::InvalidArgument(""));
  ASSERT_EQ(db->del(partition, key), Status::InvalidArgument(""));
  // Test multi calls with empty and non-empty key vectors.
  ASSERT_EQ(db->multiGet(partition, keys, out_values), Status::InvalidArgument(""));
  ASSERT_EQ(db->multiGet(partition, KeysVector{}, out_values), Status::InvalidArgument(""));
  ASSERT_EQ(db->multiPut(partition, kvs), Status::InvalidArgument(""));
  ASSERT_EQ(db->multiPut(partition, SetOfKeyValuePairs{}), Status::InvalidArgument(""));
  ASSERT_EQ(db->multiDel(partition, keys), Status::InvalidArgument(""));
  ASSERT_EQ(db->multiDel(partition, KeysVector{}), Status::InvalidArgument(""));
  ASSERT_EQ(db->rangeDel(partition, getSliverOfSize(1, 'a'), getSliverOfSize(1, 'b')), Status::InvalidArgument(""));
  ASSERT_EQ(db->getIterator(partition), nullptr);
  ASSERT_EQ(db->getPartitionClient(partition), nullptr);
}

TYPED_TEST(partition_test, iterate_a_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition = db->defaultPartition() + "1";
  ASSERT_EQ(db->addPartition(partition), Status::OK());
  const auto kva = std::make_pair(getSliverOfSize(1, 'a'), getSliverOfSize(2, 'x'));
  const auto kvb = std::make_pair(getSliverOfSize(1, 'b'), getSliverOfSize(2, 'y'));
  const auto kvc = std::make_pair(getSliverOfSize(1, 'c'), getSliverOfSize(2, 'z'));
  const auto empty = std::make_pair(Sliver{}, Sliver{});
  const auto kvs = SetOfKeyValuePairs{kva, kvb, kvc};
  ASSERT_EQ(db->multiPut(partition, kvs), Status::OK());

  auto iter = db->getIterator(partition);
  ASSERT_NE(iter, nullptr);

  // Iterate forward.
  ASSERT_FALSE(iter->valid());

  ASSERT_EQ(iter->first(), kva);
  ASSERT_TRUE(iter->valid());
  ASSERT_EQ(iter->getCurrent(), kva);

  ASSERT_EQ(iter->next(), kvb);
  ASSERT_TRUE(iter->valid());
  ASSERT_EQ(iter->getCurrent(), kvb);
  ;

  ASSERT_EQ(iter->next(), kvc);
  ASSERT_TRUE(iter->valid());
  ASSERT_EQ(iter->getCurrent(), kvc);

  ASSERT_EQ(iter->next(), empty);
  ASSERT_FALSE(iter->valid());
  ASSERT_EQ(iter->getCurrent(), empty);

  // Iterate backward.
  ASSERT_EQ(iter->last(), kvc);
  ASSERT_TRUE(iter->valid());
  ASSERT_EQ(iter->getCurrent(), kvc);

  ASSERT_EQ(iter->previous(), kvb);
  ASSERT_TRUE(iter->valid());
  ASSERT_EQ(iter->getCurrent(), kvb);

  ASSERT_EQ(iter->previous(), kva);
  ASSERT_TRUE(iter->valid());
  ASSERT_EQ(iter->getCurrent(), kva);

  ASSERT_EQ(iter->previous(), empty);
  ASSERT_FALSE(iter->valid());
  ASSERT_EQ(iter->getCurrent(), empty);
}

TYPED_TEST(partition_test, seek_in_a_partition) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition = db->defaultPartition() + "1";
  ASSERT_EQ(db->addPartition(partition), Status::OK());
  const auto kva = std::make_pair(getSliverOfSize(1, 'a'), getSliverOfSize(2, 'x'));
  const auto kvb = std::make_pair(getSliverOfSize(1, 'b'), getSliverOfSize(2, 'y'));
  const auto kvc = std::make_pair(getSliverOfSize(1, 'c'), getSliverOfSize(2, 'y'));
  const auto kvd = std::make_pair(getSliverOfSize(1, 'd'), getSliverOfSize(2, 'z'));
  const auto empty = std::make_pair(Sliver{}, Sliver{});
  const auto kvs = SetOfKeyValuePairs{kva, kvb, kvd};
  ASSERT_EQ(db->multiPut(partition, kvs), Status::OK());

  auto iter = db->getIterator(partition);
  ASSERT_NE(iter, nullptr);

  // [a, b, d] seekAtLeast(c) returns d
  ASSERT_EQ(iter->seekAtLeast(kvc.first), kvd);
  ASSERT_TRUE(iter->valid());
  ASSERT_EQ(iter->next(), empty);
  ASSERT_FALSE(iter->valid());

  // [a, b, d] seekAtMost(c) returns b
  ASSERT_EQ(iter->seekAtMost(kvc.first), kvb);
  ASSERT_TRUE(iter->valid());
  ASSERT_EQ(iter->next(), kvd);
  ASSERT_TRUE(iter->valid());
  ASSERT_EQ(iter->next(), empty);
  ASSERT_FALSE(iter->valid());
}

TYPED_TEST(partition_test, iterator_first_and_last) {
  using Db = typename TestFixture::Db;

  auto db = Db::createPartitioned();
  const auto partition = db->defaultPartition() + "1";
  ASSERT_EQ(db->addPartition(partition), Status::OK());
  const auto kva = std::make_pair(getSliverOfSize(1, 'a'), getSliverOfSize(2, 'x'));
  const auto kvb = std::make_pair(getSliverOfSize(1, 'b'), getSliverOfSize(2, 'y'));
  const auto kvs = SetOfKeyValuePairs{kva, kvb};
  ASSERT_EQ(db->multiPut(partition, kvs), Status::OK());

  auto iter = db->getIterator(partition);
  ASSERT_NE(iter, nullptr);

  ASSERT_EQ(iter->first(), kva);
  ASSERT_EQ(iter->last(), kvb);
}

}  // namespace

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
