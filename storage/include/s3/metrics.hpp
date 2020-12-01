#pragma once

#include <Metrics.hpp>

namespace concord::storage::s3 {

struct Metrics {
  Metrics()
      : metrics_component_{concordMetrics::Component("s3", std::make_shared<concordMetrics::Aggregator>())},
        numKeysTransferred{metrics_component_.RegisterCounter("keys_transferred")},
        bytesTransferred{
            metrics_component_.RegisterCounter("bytes_transferred"),
        },
        lastSavedBlockId{
            metrics_component_.RegisterStatus("last_saved_block_id", "0"),
        }

  {
    metrics_component_.Register();
  }

  void updateLastSavedBlockId(const std::string& key) {
    if (!IsBlockKey(key)) return;

    // tokenize the key
    std::vector<std::string> elems;
    std::istringstream key_stream(key);
    std::string e;
    while (std::getline(key_stream, e, '/')) {
      elems.push_back(e);
    }

    // There should be at least two elements - block id and key
    // the format is: "PREFIX/BLOCK_ID/KEY", where PREFIX is optional
    if (elems.size() < 2) return;

    lastSavedBlockId.Get().Set(elems[elems.size() - 2]);
  }

  std::string getLastSavedBlockId() { return lastSavedBlockId.Get().Get(); }

  concordMetrics::Component metrics_component_;

  concordMetrics::CounterHandle numKeysTransferred;
  concordMetrics::CounterHandle bytesTransferred;

 private:
  // This function "guesses" if metadata or block is being updated.
  // In the latter case it updates the metric
  bool IsBlockKey(std::string_view key) { return key.find("metadata") == std::string_view::npos; }

  concordMetrics::StatusHandle lastSavedBlockId;
};
}  // namespace concord::storage::s3