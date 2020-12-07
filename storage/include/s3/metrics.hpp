#pragma once

#include <Metrics.hpp>
#include <sliver.hpp>

namespace concord::storage::s3 {

struct Metrics {
  Metrics()
      : metrics_component_{concordMetrics::Component("s3", std::make_shared<concordMetrics::Aggregator>())},
        numKeysTransferred{metrics_component_.RegisterCounter("keys_transferred")},
        bytesTransferred{
            metrics_component_.RegisterCounter("bytes_transferred"),
        },
        lastSavedBlockId_{
            metrics_component_.RegisterStatus("last_saved_block_id", "0"),
        }

  {
    metrics_component_.Register();
  }

  void updateLastSavedBlockId(const concordUtils::Sliver& key) {
    if (!isBlockKey(key.string_view())) return;

    // tokenize the key
    std::vector<std::string> elems;
    std::istringstream key_stream(key.toString());
    std::string e;
    while (std::getline(key_stream, e, '/')) {
      elems.push_back(e);
    }

    // There should be at least two elements - block id and key
    // the format is: "PREFIX/BLOCK_ID/KEY", where PREFIX is optional
    if (elems.size() < 2) return;

    lastSavedBlockId_.Get().Set(elems[elems.size() - 2]);
  }

  std::string getLastSavedBlockId() { return lastSavedBlockId_.Get().Get(); }

  concordMetrics::Component metrics_component_;

  concordMetrics::CounterHandle numKeysTransferred;
  concordMetrics::CounterHandle bytesTransferred;

 private:
  // This function "guesses" if metadata or block is being updated.
  // In the latter case it updates the metric
  bool isBlockKey(std::string_view key) { return key.find("metadata") == std::string_view::npos; }

  concordMetrics::StatusHandle lastSavedBlockId_;
};
}  // namespace concord::storage::s3