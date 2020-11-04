#include <exception>
#include <thread>

#include "storage/db_interface.h"

namespace concord::test::ror_perf {
class FakeS3 : public concord::storage::IDBClient {
 public:
  FakeS3(std::chrono::milliseconds opDelay) : delay{opDelay} {}

  void init(bool readOnly = false) override {}
  concordUtils::Status get(const concordUtils::Sliver& _key, OUT concordUtils::Sliver& _outValue) const override {
    std::this_thread::sleep_for(delay);
    return getValue(_key.toString(), _outValue);
  }

  concordUtils::Status get(const concordUtils::Sliver& _key,
                           OUT char*& buf,
                           uint32_t bufSize,
                           OUT uint32_t& _size) const override {
    throw std::runtime_error("get2");
    return concordUtils::Status::IllegalOperation("");
  }
  concordUtils::Status has(const concordUtils::Sliver& _key) const override {
    std::this_thread::sleep_for(delay);
    std::scoped_lock l(metadata_mut_);
    if (data_.find(_key) != data_.end()) {
      return concordUtils::Status::OK();
    } else {
      return concordUtils::Status::NotFound("");
    }
    // throw std::runtime_error("has");
  }
  concordUtils::Status put(const concordUtils::Sliver& _key, const concordUtils::Sliver& _value) override {
    std::this_thread::sleep_for(delay);
    std::scoped_lock l(metadata_mut_);
    data_.insert_or_assign(_key, _value);
    return concordUtils::Status::OK();
  }
  concordUtils::Status del(const concordUtils::Sliver& _key) override {
    throw std::runtime_error("del");
    return concordUtils::Status::IllegalOperation("");
  }
  concordUtils::Status multiGet(const concord::storage::KeysVector& _keysVec,
                                OUT concord::storage::ValuesVector& _valuesVec) override {
    throw std::runtime_error("multiGet");
    return concordUtils::Status::IllegalOperation("");
  }
  concordUtils::Status multiPut(const concord::storage::SetOfKeyValuePairs& _keyValueMap) override {
    std::this_thread::sleep_for(delay * _keyValueMap.size());
    std::scoped_lock l(metadata_mut_);
    data_.insert(_keyValueMap.begin(), _keyValueMap.end());
    return concordUtils::Status::OK();
  }
  concordUtils::Status multiDel(const concord::storage::KeysVector& _keysVec) override {
    throw std::runtime_error("multidel");
    return concordUtils::Status::IllegalOperation("");
  }
  concordUtils::Status rangeDel(const concordUtils::Sliver& _beginKey, const concordUtils::Sliver& _endKey) override {
    throw std::runtime_error("rangedel");
    return concordUtils::Status::IllegalOperation("");
  }
  bool isNew() override {
    throw std::runtime_error("isNew");
    return false;
  }

  // the caller is responsible for transaction object lifetime
  // possible options: ITransaction::Guard or std::shared_ptr
  concord::storage::ITransaction* beginTransaction() override {
    throw std::runtime_error("beginTransaction");
    return nullptr;
  }

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) override {}

  IDBClientIterator* getIterator() const override {
    throw std::runtime_error("getiterator");
    return nullptr;
  }
  concordUtils::Status freeIterator(IDBClientIterator* _iter) const override {
    throw std::runtime_error("freeIterator");
    return concordUtils::Status::IllegalOperation("");
  }

 private:
  std::unordered_map<concordUtils::Sliver, concordUtils::Sliver> data_;
  mutable std::mutex metadata_mut_;
  std::chrono::milliseconds delay;

  concordUtils::Status getValue(const concordUtils::Sliver& key, concordUtils::Sliver& result) const;
};
}  // namespace concord::test::ror_perf