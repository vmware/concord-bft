// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

namespace concord {
namespace kvbc {
namespace sparse_merkle {

// Forward declarations
class BatchedInternalNode;
class InternalNodeKey;
struct LeafNode;
class LeafKey;

// This is a class used for accessing nodes from a database.
class IDBReader {
 public:
  virtual ~IDBReader() = default;

  // Return the latest root node in the system
  virtual BatchedInternalNode get_latest_root() const = 0;

  // Retrieve a BatchedInternalNode given an InternalNodeKey.
  //
  // Throws a std::out_of_range exception if the internal node does not exist.
  virtual BatchedInternalNode get_internal(const InternalNodeKey&) const = 0;
};

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
