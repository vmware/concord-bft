#include "gtest/gtest.h"
#include "s3/s3_metrics.hpp"
#include "direct_kv_storage_factory.h"
#include "direct_kv_db_adapter.h"

using namespace concord::kvbc;

namespace {

TEST(metrics_test, updateLastSavedBlockId) {
  auto keygen = std::make_unique<v1DirectKeyValue::S3KeyGenerator>("prefix");
  concord::storage::s3::Metrics m;
  const auto keyName{"test_key"};
  auto blockId = 1;

  // Create a data key - it should increase the metric
  auto dataKey = keygen->dataKey(concord::kvbc::Key{keyName}, blockId);
  m.updateLastSavedBlockId(dataKey.toString());
  ASSERT_EQ(m.getLastSavedBlockId(), 1);

  // Create metadata key - it should NOT increase the metric
  auto mdtKey = keygen->mdtKey(Key{keyName});
  m.updateLastSavedBlockId(mdtKey.toString());
  ASSERT_EQ(m.getLastSavedBlockId(), 1);

  // Create block key - it should increase the metric
  blockId++;
  auto blockKey = keygen->blockKey(blockId);
  m.updateLastSavedBlockId(blockKey.toString());
  ASSERT_EQ(m.getLastSavedBlockId(), 2);
}

}  // namespace
