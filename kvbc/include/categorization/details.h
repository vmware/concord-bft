#include <vector>
#include <string>
#include "categorized_kvbc_msgs.cmf.hpp"
#include "kv_types.hpp"
#include "sparse_merkle/base_types.h"
#include "evp_hash.hpp"

namespace concord::kvbc::categorization {

namespace serialization {
using buffer = std::vector<uint8_t>;
}  // namespace serialization

namespace hash {
using HashType = concord::util::SHA3_256;
using digest = HashType::Digest;
digest sha3_256(const void* buf, size_t size) { return HashType{}.digest(buf, size); }
}  // namespace hash

// DB key is a CMF stuct of hashed key and the version.
// On serialization it will be hashed_key || version(big endian)
DbKey genDbKey(const std::string& key, const BlockId& version) {
  DbKey db_key;
  auto hash_arr = hash::sha3_256(key.data(), key.length());
  db_key.hashed_key = hash_arr;
  db_key.version = version;
  return db_key;
}

}  // namespace concord::kvbc::categorization