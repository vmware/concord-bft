#include "FakeS3.hpp"
#include "direct_kv_db_adapter.h"
#include "direct_kv_storage_factory.h"
#include "object_store/object_store_client.hpp"
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
#include "storage_factory_interface.h"

namespace concord::test::ror_perf {

class StorageFactory : public concord::kvbc::IStorageFactory {
 public:
  StorageFactory(std::string dbPath, const std::chrono::milliseconds s3OpDuration)
      : dbPath_{dbPath}, s3Config_{}, s3Factory_{dbPath_, s3Config_}, s3OpDuration_{s3OpDuration} {}

  // This method is 90% copy-paste of S3StorageFactory::newDatabaseSet()
  // Don't try to call s3Factory_.newDatabaseSet() and to overwrite values.
  // S3 client is initialised on creation.
  IStorageFactory::DatabaseSet newDatabaseSet() const override {
    auto ret = IStorageFactory::DatabaseSet{};
    ret.metadataDBClient = std::make_shared<concord::storage::rocksdb::Client>(
        dbPath_,
        std::make_unique<concord::storage::rocksdb::KeyComparator>(
            new concord::kvbc::v1DirectKeyValue::DBKeyComparator{}));
    ret.metadataDBClient->init();
    ret.dataDBClient = std::make_shared<concord::storage::ObjectStoreClient>(new FakeS3{s3OpDuration_});
    ret.dataDBClient->init();

    auto dataKeyGenerator = std::make_unique<concord::kvbc::v1DirectKeyValue::S3KeyGenerator>(s3Config_.pathPrefix);
    ret.dbAdapter = std::make_unique<concord::kvbc::v1DirectKeyValue::DBAdapter>(
        ret.dataDBClient, std::move(dataKeyGenerator), true);

    return ret;
  }

  std::unique_ptr<storage::IMetadataKeyManipulator> newMetadataKeyManipulator() const override {
    return s3Factory_.newMetadataKeyManipulator();
  }

  std::unique_ptr<storage::ISTKeyManipulator> newSTKeyManipulator() const override {
    return s3Factory_.newSTKeyManipulator();
  }

  ~StorageFactory() {}

 private:
  const std::string dbPath_;
  concord::storage::s3::StoreConfig s3Config_;
  concord::kvbc::v1DirectKeyValue::S3StorageFactory s3Factory_;
  std::chrono::milliseconds s3OpDuration_;  // how much time should it take to write a single KV pair
};
}  // namespace concord::test::ror_perf