#pragma once

#include "Metrics.hpp"
#include "sliver.hpp"
#include "Logger.hpp"
namespace concord::storage::s3 {

class Metrics {
 public:
  Metrics()
      : metrics_component{concordMetrics::Component("s3", std::make_shared<concordMetrics::Aggregator>())},
        num_keys_transferred{metrics_component.RegisterCounter("keys_transferred")},
        bytes_transferred{
            metrics_component.RegisterCounter("bytes_transferred"),
        },
        last_saved_block_id_{
            metrics_component.RegisterGauge("last_saved_block_id", 0),
        }

  {
    metrics_component.Register();
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

    uint64_t lastSavedBlockVal = 0;
    try {
      lastSavedBlockVal = stoull(elems[elems.size() - 2]);
    } catch (std::invalid_argument& e) {
      LOG_ERROR(logger_, "Can't convert lastSavedBlockId (" << elems[elems.size() - 2] << ") to numeric value.");
      return;
    } catch (std::out_of_range& e) {
      LOG_ERROR(logger_,
                "lastSavedBlockId value (" << elems[elems.size() - 2] << ") doesn't fit in unsigned 64bit integer.");
      return;
    } catch (std::exception& e) {
      LOG_ERROR(logger_,
                "Unexpected error occured while converting lastSavedBlockId (" << elems[elems.size() - 2]
                                                                               << ") to numeric value.");
      return;
    }
    last_saved_block_id_.Get().Set(lastSavedBlockVal);
  }

  uint64_t getLastSavedBlockId() { return last_saved_block_id_.Get().Get(); }

  concordMetrics::Component metrics_component;

  concordMetrics::CounterHandle num_keys_transferred;
  concordMetrics::CounterHandle bytes_transferred;

 private:
  // This function "guesses" if metadata or block is being updated.
  // In the latter case it updates the metric
  bool isBlockKey(std::string_view key) { return key.find("metadata") == std::string_view::npos; }

  concordMetrics::GaugeHandle last_saved_block_id_;

  logging::Logger logger_ = logging::getLogger("concord.storage.s3.metrics");
};
}  // namespace concord::storage::s3