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
#include "direct_kv_storage_factory.h"
#include "direct_kv_db_adapter.h"
#include "s3/client.hpp"

TEST(s3, key_generation) {
  const auto pathPrefix{"s3_prefix"};
  const auto keyName{"test_key"};
  const auto keyHex{"746573745f6b6579"};  // hex representation of the key
  const auto blockId = concord::kvbc::BlockId{1};
  const auto delim = std::string{"/"};
  auto keygen = std::make_unique<concord::kvbc::v1DirectKeyValue::S3KeyGenerator>(pathPrefix);

  auto blockKey = keygen->blockKey(blockId);
  ASSERT_EQ(blockKey.toString(), pathPrefix + delim + std::to_string(blockId) + std::string{"/raw_block"});

  auto dataKey = keygen->dataKey(concord::kvbc::Key{keyName}, blockId);
  ASSERT_EQ(dataKey.toString(), pathPrefix + delim + std::to_string(blockId) + delim + keyHex);

  auto mdtKey = keygen->mdtKey(concord::kvbc::Key{keyName});
  ASSERT_EQ(mdtKey.toString(), pathPrefix + std::string{"/metadata/"} + keyName);
}

TEST(s3, empty_prefix_keygen) {
  // Path can't contain two slashes, so we have to handle empty prefix correctly
  const auto keyName{"test_key"};
  const auto keyHex{"746573745f6b6579"};  // hex representation of the key
  const auto blockId = concord::kvbc::BlockId{1};
  const auto delim = std::string{"/"};
  auto keygen = std::make_unique<concord::kvbc::v1DirectKeyValue::S3KeyGenerator>("");

  auto blockKey = keygen->blockKey(blockId);
  ASSERT_EQ(blockKey.toString(), std::to_string(blockId) + std::string{"/raw_block"});

  auto dataKey = keygen->dataKey(concord::kvbc::Key{keyName}, blockId);
  ASSERT_EQ(dataKey.toString(), std::to_string(blockId) + delim + keyHex);

  auto mdtKey = keygen->mdtKey(concord::kvbc::Key{keyName});
  ASSERT_EQ(mdtKey.toString(), std::string{"metadata/"} + keyName);
}