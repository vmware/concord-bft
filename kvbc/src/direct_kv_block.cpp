#include "assertUtils.hpp"
#include "direct_kv_block.h"
#include "kv_types.hpp"
#include "Logger.hpp"

#include <cassert>
#include <cstring>

using concordUtils::Sliver;
using concord::kvbc::SetOfKeyValuePairs;
using concord::kvbc::OrderedSetOfKeyValuePairs;
using concord::kvbc::KeyValuePair;

namespace concord {
namespace kvbc {
namespace v1DirectKeyValue {
namespace block {
namespace detail {
Sliver create(const SetOfKeyValuePairs &updates,
              SetOfKeyValuePairs &outUpdatesInNewBlock,
              const BlockDigest &parentDigest,
              const void *userData,
              std::size_t userDataSize) {
  // TODO(GG): overflow handling ....
  // TODO(SG): How? Right now - will put empty block instead

  ConcordAssert(outUpdatesInNewBlock.size() == 0);

  std::uint32_t blockBodySize = 0;
  const std::uint16_t numOfElements = updates.size();
  for (const auto &elem : updates) {
    // body is all of the keys and values strung together
    blockBodySize += (elem.first.length() + elem.second.length());
  }

  const std::uint32_t metadataSize = sizeof(detail::Header) + sizeof(detail::Entry) * numOfElements;

  const std::uint32_t blockSize = metadataSize + blockBodySize + userDataSize;

  try {
    char *blockBuffer = new char[blockSize];
    std::memset(blockBuffer, 0, blockSize);
    Sliver blockSliver(blockBuffer, blockSize);

    auto header = (detail::Header *)blockBuffer;
    std::memcpy(header->parentDigest, parentDigest.data(), DIGEST_SIZE);
    header->parentDigestLength = DIGEST_SIZE;

    std::int16_t idx = 0;
    header->numberOfElements = numOfElements;
    std::int32_t currentOffset = metadataSize;
    auto *entries = (detail::Entry *)(blockBuffer + sizeof(detail::Header));
    // Serialize key/values in a deterministic order.
    const auto orderedUpdates = concord::kvbc::order<SetOfKeyValuePairs>(updates);
    for (const auto &kvPair : orderedUpdates) {
      // key
      entries[idx].keyOffset = currentOffset;
      entries[idx].keySize = kvPair.first.length();
      std::memcpy(blockBuffer + currentOffset, kvPair.first.data(), kvPair.first.length());
      const Sliver newKey(blockSliver, currentOffset, kvPair.first.length());

      currentOffset += kvPair.first.length();

      // value
      entries[idx].valOffset = currentOffset;
      entries[idx].valSize = kvPair.second.length();
      std::memcpy(blockBuffer + currentOffset, kvPair.second.data(), kvPair.second.length());
      const Sliver newVal(blockSliver, currentOffset, kvPair.second.length());

      currentOffset += kvPair.second.length();

      // add to outUpdatesInNewBlock
      const KeyValuePair newKVPair(newKey, newVal);
      outUpdatesInNewBlock.insert(newKVPair);

      idx++;
    }
    ConcordAssert(idx == numOfElements);

    if (userDataSize) {
      std::memcpy(blockBuffer + currentOffset, userData, userDataSize);
      currentOffset += userDataSize;
    }

    ConcordAssert((std::uint32_t)currentOffset == blockSize);

    return blockSliver;
  } catch (const std::bad_alloc &ba) {  // TODO: do we really want to mask this failure?
    LOG_ERROR(logging::getLogger("skvbc.replicaImp"), "Failed to alloc size " << blockSize << ", error: " << ba.what());
    char *emptyBlockBuffer = new char[1];
    std::memset(emptyBlockBuffer, 0, 1);
    return Sliver(emptyBlockBuffer, 1);
  }
}

Sliver create(const SetOfKeyValuePairs &updates,
              SetOfKeyValuePairs &outUpdatesInNewBlock,
              const BlockDigest &parentDigest) {
  return create(updates, outUpdatesInNewBlock, parentDigest, nullptr, 0);
}

SetOfKeyValuePairs getData(const Sliver &block) {
  SetOfKeyValuePairs retVal;

  if (block.length() > 0) {
    const auto numOfElements = ((detail::Header *)block.data())->numberOfElements;
    auto *entries = (const detail::Entry *)(block.data() + sizeof(detail::Header));
    for (size_t i = 0u; i < numOfElements; i++) {
      const Sliver keySliver(block, entries[i].keyOffset, entries[i].keySize);
      const Sliver valSliver(block, entries[i].valOffset, entries[i].valSize);
      retVal.insert(KeyValuePair(keySliver, valSliver));
    }
  }
  return retVal;
}

BlockDigest getParentDigest(const Sliver &block) {
  const auto *bh = reinterpret_cast<const detail::Header *>(block.data());
  ConcordAssert(DIGEST_SIZE == bh->parentDigestLength);
  BlockDigest digest;
  std::memcpy(digest.data(), bh->parentDigest, DIGEST_SIZE);
  return digest;
}

Sliver getUserData(const Sliver &block) {
  constexpr auto headerSize = sizeof(detail::Header);
  ConcordAssert(block.length() >= headerSize);
  const auto header = reinterpret_cast<const detail::Header *>(block.data());

  // If there are no elements, we only have a Header and, optionally, user data.
  if (header->numberOfElements == 0) {
    return Sliver{block, headerSize, block.length() - headerSize};
  }

  // Calculate the user data offset by adding the offset of the last value and its length.
  const auto entries = reinterpret_cast<const detail::Entry *>(block.data() + sizeof(detail::Header));
  const auto lastEntry = entries[header->numberOfElements - 1];
  const auto userDataOffset = lastEntry.valOffset + lastEntry.valSize;
  ConcordAssert(block.length() >= userDataOffset);
  return Sliver{block, userDataOffset, block.length() - userDataOffset};
}

}  // namespace detail
}  // namespace block
}  // namespace v1DirectKeyValue
}  // namespace kvbc
}  // namespace concord
