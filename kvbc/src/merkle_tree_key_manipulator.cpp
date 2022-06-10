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

#include "merkle_tree_key_manipulator.h"

#include "assertUtils.hpp"
#include "endianness.hpp"
#include "hex_tools.h"
#include "merkle_tree_serialization.h"
#include "string.hpp"

namespace concord::kvbc::v2MerkleTree::detail {

using ::concord::util::toChar;

using ::concordUtils::fromBigEndianBuffer;
using ::concordUtils::HexPrintBuffer;
using ::concordUtils::Sliver;

using ::concord::storage::ObjectId;
using ::concord::storage::v2MerkleTree::detail::EDBKeyType;
using ::concord::storage::v2MerkleTree::detail::EKeySubtype;
using ::concord::storage::v2MerkleTree::detail::EBFTSubtype;

using sparse_merkle::Hash;
using sparse_merkle::Hasher;
using sparse_merkle::InternalNodeKey;
using sparse_merkle::LeafKey;
using sparse_merkle::Version;

Key DBKeyManipulator::genNonProvableDbKey(BlockId block_id, const Key &key) {
  return serialize(EKeySubtype::NonProvable) + key.toString() + serializeImp(block_id);
}

Key DBKeyManipulator::genBlockDbKey(BlockId version) { return serialize(EDBKeyType::Block, version); }

Key DBKeyManipulator::genDataDbKey(const LeafKey &key) { return serialize(EKeySubtype::Leaf, key); }

Key DBKeyManipulator::genDataDbKey(const Key &key, const Version &version) {
  auto hasher = Hasher{};
  return genDataDbKey(LeafKey{hasher.hash(key.data(), key.length()), version});
}

Key DBKeyManipulator::genInternalDbKey(const InternalNodeKey &key) { return serialize(EKeySubtype::Internal, key); }

Key DBKeyManipulator::genStaleDbKey(const InternalNodeKey &key, const Version &staleSinceVersion) {
  return serialize(EKeySubtype::ProvableStale, staleSinceVersion.value(), EKeySubtype::Internal, key);
}

Key DBKeyManipulator::genStaleDbKey(const LeafKey &key, const Version &staleSinceVersion) {
  return serialize(EKeySubtype::ProvableStale, staleSinceVersion.value(), EKeySubtype::Leaf, key);
}

Key DBKeyManipulator::genStaleDbKey(const Version &staleSinceVersion) {
  return serialize(EKeySubtype::ProvableStale, staleSinceVersion.value());
}

Key DBKeyManipulator::genNonProvableStaleDbKey(const Key &key, BlockId staleSinceBlock) {
  return serialize(EKeySubtype::NonProvableStale, staleSinceBlock) + key.toString();
}

Key DBKeyManipulator::generateSTTempBlockKey(BlockId blockId) { return serialize(EBFTSubtype::STTempBlock, blockId); }

BlockId DBKeyManipulator::extractBlockIdFromNonProvableKey(const Key &key) {
  ConcordAssert(key.length() >= sizeof(EDBKeyType::Key) + sizeof(EKeySubtype::NonProvable) + sizeof(BlockId));
  ConcordAssert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  ConcordAssert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::NonProvable);
  return fromBigEndianBuffer<BlockId>(key.data() + key.length() - sizeof(BlockId));
}

Key DBKeyManipulator::extractKeyFromNonProvableKey(const Key &key) {
  constexpr auto keyOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype);
  ConcordAssert(key.length() >= keyOffset + sizeof(BlockId));
  ConcordAssert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  ConcordAssert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::NonProvable);
  return Key{key, keyOffset, key.length() - keyOffset - sizeof(BlockId)};
}

BlockId DBKeyManipulator::extractBlockIdFromKey(const Key &key) {
  ConcordAssert(key.length() > sizeof(BlockId));

  const auto offset = key.length() - sizeof(BlockId);
  const auto id = fromBigEndianBuffer<BlockId>(key.data() + offset);

  LOG_TRACE(
      logger(),
      "Got block ID " << id << " from key " << (HexPrintBuffer{key.data(), key.length()}) << ", offset " << offset);
  return id;
}

Hash DBKeyManipulator::extractHashFromLeafKey(const Key &key) {
  constexpr auto keyTypeOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype);
  ConcordAssert(key.length() > keyTypeOffset + Hash::SIZE_IN_BYTES);
  ConcordAssert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  ConcordAssert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::Leaf);
  return Hash{reinterpret_cast<const uint8_t *>(key.data() + keyTypeOffset)};
}

Version DBKeyManipulator::extractVersionFromProvableStaleKey(const Key &key) {
  constexpr auto keyTypeOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype);
  ConcordAssert(key.length() >= keyTypeOffset + Version::SIZE_IN_BYTES);
  ConcordAssert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  ConcordAssert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::ProvableStale);
  return fromBigEndianBuffer<Version::Type>(key.data() + keyTypeOffset);
}

BlockId DBKeyManipulator::extractBlockIdFromNonProvableStaleKey(const Key &key) {
  constexpr auto keyTypeOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype);
  ConcordAssert(key.length() >= keyTypeOffset + sizeof(BlockId));
  ConcordAssert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  ConcordAssert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::NonProvableStale);
  return fromBigEndianBuffer<BlockId>(key.data() + keyTypeOffset);
}

Key DBKeyManipulator::extractKeyFromProvableStaleKey(const Key &key) {
  constexpr auto keyOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype) + Version::SIZE_IN_BYTES;
  ConcordAssert(key.length() > keyOffset);
  ConcordAssert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  ConcordAssert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::ProvableStale);
  return Key{key, keyOffset, key.length() - keyOffset};
}

Key DBKeyManipulator::extractKeyFromNonProvableStaleKey(const Key &key) {
  constexpr auto keyOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype) + sizeof(BlockId);
  ConcordAssert(key.length() > keyOffset);
  ConcordAssert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  ConcordAssert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::NonProvableStale);
  return Key{key, keyOffset, key.length() - keyOffset};
}

Version DBKeyManipulator::extractVersionFromInternalKey(const Key &key) {
  constexpr auto keyOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype);
  ConcordAssert(key.length() > keyOffset);
  return deserialize<InternalNodeKey>(Sliver{key, keyOffset, key.length() - keyOffset}).version();
}

// Undefined behavior if an incorrect type is read from the buffer.
EDBKeyType DBKeyManipulator::getDBKeyType(const Sliver &s) {
  ConcordAssert(!s.empty());

  switch (s[0]) {
    case toChar(EDBKeyType::Block):
      return EDBKeyType::Block;
    case toChar(EDBKeyType::Key):
      return EDBKeyType::Key;
    case toChar(EDBKeyType::BFT):
      return EDBKeyType::BFT;
    case toChar(EDBKeyType::Migration):
      return EDBKeyType::Migration;
  }
  LOG_FATAL(
      logger(),
      "Key type does not match, key(hex) " << concordUtils::bufferToHex(s.data(), s.size()) << " key " << s.toString());
  printCallStack();
  ConcordAssert(false);

  // Dummy return to silence the compiler.
  return EDBKeyType::Block;
}

// Undefined behavior if an incorrect type is read from the buffer.
EKeySubtype DBKeyManipulator::getKeySubtype(const Sliver &s) {
  ConcordAssert(s.length() > 1);

  switch (s[1]) {
    case toChar(EKeySubtype::Internal):
      return EKeySubtype::Internal;
    case toChar(EKeySubtype::ProvableStale):
      return EKeySubtype::ProvableStale;
    case toChar(EKeySubtype::NonProvableStale):
      return EKeySubtype::NonProvableStale;
    case toChar(EKeySubtype::Leaf):
      return EKeySubtype::Leaf;
    case toChar(EKeySubtype::NonProvable):
      return EKeySubtype::NonProvable;
  }
  ConcordAssert(false);

  // Dummy return to silence the compiler.
  return EKeySubtype::Internal;
}

// Undefined behavior if an incorrect type is read from the buffer.
EBFTSubtype DBKeyManipulator::getBftSubtype(const Sliver &s) {
  ConcordAssert(s.length() > 1);

  switch (s[1]) {
    case toChar(EBFTSubtype::Metadata):
      return EBFTSubtype::Metadata;
    case toChar(EBFTSubtype::ST):
      return EBFTSubtype::ST;
    case toChar(EBFTSubtype::STPendingPage):
      return EBFTSubtype::STPendingPage;
    case toChar(EBFTSubtype::STReservedPageDynamic):
      return EBFTSubtype::STReservedPageDynamic;
    case toChar(EBFTSubtype::STCheckpointDescriptor):
      return EBFTSubtype::STCheckpointDescriptor;
    case toChar(EBFTSubtype::STTempBlock):
      return EBFTSubtype::STTempBlock;
    case toChar(EBFTSubtype::PublicStateHashAtDbCheckpoint):
      return EBFTSubtype::PublicStateHashAtDbCheckpoint;
  }
  ConcordAssert(false);

  // Dummy return to silence the compiler.
  return EBFTSubtype::Metadata;
}

}  // namespace concord::kvbc::v2MerkleTree::detail
