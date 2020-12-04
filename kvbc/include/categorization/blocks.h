#include "updates.h"
#include <utility>
#include <block_digest.h>
#include "details.h"
#include "merkle_tree_serialization.h"
#include "merkle_tree_key_manipulator.h"
#include "merkle_tree_db_adapter.h"
#include "sliver.hpp"

namespace concord::kvbc::categorization {

// Block is composed out of:
// - BlockID
// - Parent digest
// - Categories' updates_info where update info is essentially a list of keys, metadata on the keys and a hash of the
// category state.
struct Block {
  static const std::string CATEGORY_ID;
  Block() {
    data.block_id = 0;
    data.parent_digest.fill(0);
  }

  Block(const BlockId block_id) {
    data.block_id = block_id;
    data.parent_digest.fill(0);
  }

  void add(const std::string& category_id, MerkleUpdatesInfo&& updates_info) {
    data.categories_updates_info.emplace(category_id, std::move(updates_info));
  }

  void add(const std::string& category_id, KeyValueUpdatesInfo&& updates_info) {
    data.categories_updates_info.emplace(category_id, std::move(updates_info));
  }

  void add(SharedKeyValueUpdatesInfo&& updates_info) { data.shared_updates_info = std::move(updates_info); }

  void setParentHash(const BlockDigest& parent_hash) {
    std::copy(parent_hash.begin(), parent_hash.end(), data.parent_digest.begin());
  }
  inline BlockId id() const { return data.block_id; }

  static const detail::Buffer& serialize(const Block& block) {
    static thread_local detail::Buffer output;
    output.clear();
    concord::kvbc::categorization::serialize(output, block.data);
    return output;
  }

  static Block deserialize(const detail::Buffer& input) {
    Block output;
    concord::kvbc::categorization::deserialize(input, output.data);
    return output;
  }

  // Generate block key from block ID
  // Using CMF for big endian
  static const detail::Buffer& generateKey(const BlockId block_id) {
    static thread_local detail::Buffer output;
    output.clear();
    BlockKey key{block_id};
    // think about optimization
    concord::kvbc::categorization::serialize(output, key);
    return output;
  }

  BlockData data;
};

const std::string Block::CATEGORY_ID{"__blocks__"};

//////////////////////////////////// RAW BLOCKS//////////////////////////////////////

// Reconstructs the updates data as recieved from the user
// This set methods are overloaded in order to construct the appropriate updates

// Merkle updates reconstruction
RawBlockMerkleUpdates getRawUpdates(const std::string& category_id,
                                    const MerkleUpdatesInfo& update_info,
                                    const BlockId& block_id,
                                    const storage::rocksdb::NativeClient* native_client) {
  // For old serialization
  using namespace concord::kvbc::v2MerkleTree;
  RawBlockMerkleUpdates data;
  // Copy state hash (std::copy?)
  data.root_hash = update_info.root_hash;
  // Iterate over the keys:
  // if deleted, add to the deleted set.
  // else generate a db key, serialize it and
  // get the value from the corresponding column family
  for (auto& [key, flag] : update_info.keys) {
    if (flag.deleted) {
      data.updates.deletes.push_back(key);
      continue;
    }
    // E.L see how we can optimize the sliver temporary allocation
    auto db_key =
        v2MerkleTree::detail::DBKeyManipulator::genDataDbKey(Key(std::string(key)), update_info.state_root_version);
    auto val = native_client->get(category_id, db_key);
    if (!val.has_value()) {
      throw std::logic_error("Couldn't find value for key");
    }
    // E.L serializtion of the Merkle to CMF will be in later phase
    auto dbLeafVal = v2MerkleTree::detail::deserialize<v2MerkleTree::detail::DatabaseLeafValue>(
        concordUtils::Sliver{std::move(val.value())});
    ConcordAssert(dbLeafVal.addedInBlockId == block_id);

    data.updates.kv[key] = dbLeafVal.leafNode.value.toString();
  }

  return data;
}

// KeyValueUpdatesData updates reconstruction
RawBlockKeyValueUpdates getRawUpdates(const std::string& category_id,
                                      const KeyValueUpdatesInfo& update_info,
                                      const BlockId& block_id,
                                      const storage::rocksdb::NativeClient* native_client) {
  RawBlockKeyValueUpdates data;
  return data;
}

// shared updates reconstruction
RawBlockSharedUpdates getRawSharedUpdates(const SharedKeyValueUpdatesInfo& update_info,
                                          const BlockId& block_id,
                                          const storage::rocksdb::NativeClient* native_client) {
  RawBlockSharedUpdates data;
  return data;
}

// Raw block is the unit which we can transfer and reconstruct a block in a remote replica.
// It contains all the relevant updates as recieved from the client in the initial call to storage.
// It also contains:
// - parent digest
// - state hash per category (if exists) E.L check why do we pass it.
struct RawBlock {
  RawBlock(const Block& block, const storage::rocksdb::NativeClient* native_client) {
    // parent digest (std::copy?)
    data.parent_digest = block.data.parent_digest;
    // recontruct updates of categories
    for (auto& [cat_id, update_info] : block.data.categories_updates_info) {
      std::visit(
          [category_id = cat_id, &block, this, &native_client](const auto& update_info) {
            auto category_updates = getRawUpdates(category_id, update_info, block.id(), native_client);
            data.category_updates.emplace(category_id, std::move(category_updates));
          },
          update_info);
    }
    if (block.data.shared_updates_info.has_value()) {
      data.shared_update = getRawSharedUpdates(block.data.shared_updates_info.value(), block.id(), native_client);
    }
  }

  static const detail::Buffer& serialize(const RawBlockData& data) {
    static thread_local detail::Buffer out;
    out.clear();
    concord::kvbc::categorization::serialize(out, data);
    return out;
  }

  RawBlockData data;
};

}  // namespace concord::kvbc::categorization