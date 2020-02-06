#include "blockchain/block.h"

#include "Logger.hpp"

#include <cassert>
#include <cstring>

using concordUtils::Sliver;
using concordUtils::SetOfKeyValuePairs;
using concordUtils::KeyValuePair;

namespace concord {
namespace storage {
namespace blockchain {
namespace block {
inline namespace v1DirectKeyValue {
concordUtils::Sliver create(const concordUtils::SetOfKeyValuePairs &updates,
                            concordUtils::SetOfKeyValuePairs &outUpdatesInNewBlock,
                            const void *parentDigest) {
  // TODO(GG): overflow handling ....
  // TODO(SG): How? Right now - will put empty block instead

  assert(outUpdatesInNewBlock.size() == 0);

  std::uint32_t blockBodySize = 0;
  const std::uint16_t numOfElements = updates.size();
  for (const auto &elem : updates) {
    // body is all of the keys and values strung together
    blockBodySize += (elem.first.length() + elem.second.length());
  }

  const std::uint32_t metadataSize = sizeof(detail::Header) + sizeof(detail::Entry) * numOfElements;

  const std::uint32_t blockSize = metadataSize + blockBodySize;

  try {
    char *blockBuffer = new char[blockSize];
    std::memset(blockBuffer, 0, blockSize);
    const Sliver blockSliver(blockBuffer, blockSize);

    auto header = (detail::Header *)blockBuffer;
    std::memcpy(header->parentDigest, parentDigest, BLOCK_DIGEST_SIZE);
    header->parentDigestLength = BLOCK_DIGEST_SIZE;

    std::int16_t idx = 0;
    header->numberOfElements = numOfElements;
    std::int32_t currentOffset = metadataSize;
    auto *entries = (detail::Entry *)(blockBuffer + sizeof(detail::Header));
    for (const auto &elem : updates) {
      const KeyValuePair &kvPair = elem;

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
    assert(idx == numOfElements);
    assert((std::uint32_t)currentOffset == blockSize);

    return blockSliver;
  } catch (const std::bad_alloc &ba) {  // TODO: do we really want to mask this failure?
    LOG_ERROR(concordlogger::Log::getLogger("skvbc.replicaImp"),
              "Failed to alloc size " << blockSize << ", error: " << ba.what());
    char *emptyBlockBuffer = new char[1];
    std::memset(emptyBlockBuffer, 0, 1);
    return Sliver(emptyBlockBuffer, 1);
  }
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

const void *getParentDigest(const concordUtils::Sliver &block) {
  const auto *bh = reinterpret_cast<const detail::Header *>(block.data());
  assert(BLOCK_DIGEST_SIZE == bh->parentDigestLength);
  return bh->parentDigest;
}

}  // namespace v1DirectKeyValue
}  // namespace block
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
