#include "FakeS3.hpp"

using namespace concord::test::ror_perf;

concordUtils::Status FakeS3::getValue(const concordUtils::Sliver& key, concordUtils::Sliver& result) const {
  std::scoped_lock l(metadata_mut_);
  auto res = data_.find(key);
  if (res == data_.end()) {
    return concordUtils::Status::NotFound("");
  }

  result = res->second;
  return concordUtils::Status::OK();
}