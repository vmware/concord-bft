// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <stdexcept>

#include <hdr/hdr_interval_recorder.h>

#include "assertUtils.hpp"
#include "Logger.hpp"

namespace concord::diagnostics {

static logging::Logger DIAG_LOGGER = logging::getLogger("concord.diagnostics");

enum class Unit {
  NANOSECONDS,
  MICROSECONDS,
  MILLISECONDS,
  SECONDS,
  MINUTES,

  BYTES,
  KB,
  MB,
  GB,

  // Things like queue length, size of a map, etc..
  COUNT
};

// A recorder is a thread-safe type that should always be created in a shared pointer to ensure
// proper destruction. The recorder is used to add values to the histogram in the recording thread.
// Interval histograms can be extracted in other threads.
struct Recorder {
  // These values, (except for unit), come directly from hdr histogram.
  // https://github.com/HdrHistogram/HdrHistogram_c/blob/master/src/hdr_interval_recorder.h#L28-L32
  Recorder(int64_t lowest_trackable_value, int64_t highest_trackable_value, int significant_figures, Unit unit)
      : unit(unit) {
    ConcordAssert(lowest_trackable_value > 0);
    auto rv =
        hdr_interval_recorder_init_all(&recorder, lowest_trackable_value, highest_trackable_value, significant_figures);
    ConcordAssertEQ(0, rv);
  }

  ~Recorder() { hdr_interval_recorder_destroy(&recorder); }
  Recorder(const Recorder&) = delete;
  Recorder& operator=(const Recorder&) = delete;

  void record(int64_t val) {
    if (!hdr_interval_recorder_record_value(&(recorder), val)) {
      // We don't track the name in recorder, which we would do just for this, which is almost impossible to hit.
      LOG_WARN(DIAG_LOGGER, "Failed to record value: " << KVLOG(val, unit));
    }
  }

  hdr_interval_recorder recorder;
  Unit unit;
};

struct Histogram {
  Histogram(const std::shared_ptr<Recorder>& recorder)
      : recorder(recorder), start(std::chrono::system_clock::now()), snapshot_start(start), snapshot_end(start) {
    snapshot = hdr_interval_recorder_sample_and_recycle(&(recorder->recorder), snapshot);
    auto rv = hdr_init(
        snapshot->lowest_trackable_value, snapshot->highest_trackable_value, snapshot->significant_figures, &history);
    ConcordAssertEQ(0, rv);
  }

  ~Histogram() {
    hdr_close(snapshot);
    hdr_close(history);
    snapshot = nullptr;
    history = nullptr;
  }

  Histogram(const Histogram&) = delete;
  Histogram& operator=(const Histogram&) = delete;

  void takeSnapshot() {
    snapshot_start = snapshot_end;
    snapshot_end = std::chrono::system_clock::now();
    // Add the previous snapshot to the history
    if (int64_t discarded = hdr_add(history, snapshot) != 0) {
      // This should be impossible to hit, according to hdrHistogram docs, since the histograms have the same
      // trackable values.
      LOG_ERROR(DIAG_LOGGER,
                "Failed to update history: " << KVLOG(discarded,
                                                      snapshot->lowest_trackable_value,
                                                      snapshot->highest_trackable_value,
                                                      history->lowest_trackable_value,
                                                      history->highest_trackable_value));
    }
    snapshot = hdr_interval_recorder_sample_and_recycle(&(recorder->recorder), snapshot);
  }

  std::shared_ptr<Recorder> recorder;

  std::chrono::system_clock::time_point start;
  std::chrono::system_clock::time_point snapshot_start;
  std::chrono::system_clock::time_point snapshot_end;

  // The histogram interval starting from when the last snapshot was taken
  hdr_histogram* snapshot = nullptr;

  // History doesn't include the latest snapshot.
  hdr_histogram* history = nullptr;
};

using Name = std::string;
using Recorders = std::map<Name, std::shared_ptr<Recorder>>;
using Histograms = std::map<Name, Histogram>;

// Useful data extracted from hdr_histogram(s)
//
// By extracting this data we can return it in a thread-safe manner, without having to copy the
// entire histogram.
//
// We specifically don't return the average, as it's a completely meaningless metric.
struct HistogramValues {
  HistogramValues(hdr_histogram* h) : memory_used(hdr_get_memory_size(h)) {
    max = hdr_max(h);
    if (max == 0) return;  // Histogram is empty
    min = hdr_min(h);
    count = h->total_count;
    pct_10 = hdr_value_at_percentile(h, 10);
    pct_25 = hdr_value_at_percentile(h, 25);
    pct_50 = hdr_value_at_percentile(h, 50);
    pct_75 = hdr_value_at_percentile(h, 75);
    pct_90 = hdr_value_at_percentile(h, 90);
    pct_95 = hdr_value_at_percentile(h, 95);
    pct_99 = hdr_value_at_percentile(h, 99);
    pct_99_9 = hdr_value_at_percentile(h, 99.9);
    pct_99_99 = hdr_value_at_percentile(h, 99.99);
    pct_99_999 = hdr_value_at_percentile(h, 99.999);
  }

  bool operator==(const HistogramValues& other) const {
    return count == other.count && min == other.min && max == other.max && pct_10 == other.pct_10 &&
           pct_25 == other.pct_25 && pct_50 == other.pct_50 && pct_75 == other.pct_75 && pct_90 == other.pct_90 &&
           pct_95 == other.pct_95 && pct_99 == other.pct_99 && pct_99_9 == other.pct_99 &&
           pct_99_99 == other.pct_99_99 && pct_99_999 == other.pct_99_999;
  }
  bool operator!=(const HistogramValues& other) const { return !(*this == other); }

  size_t memory_used;
  int64_t count = 0;
  int64_t min = 0;
  int64_t max = 0;
  int64_t pct_10 = 0;
  int64_t pct_25 = 0;
  int64_t pct_50 = 0;
  int64_t pct_75 = 0;
  int64_t pct_90 = 0;
  int64_t pct_95 = 0;
  int64_t pct_99 = 0;
  int64_t pct_99_9 = 0;
  int64_t pct_99_99 = 0;
  int64_t pct_99_999 = 0;
};

struct HistogramData {
  HistogramData(const Histogram& h)
      : HistogramData(h.start,
                      h.snapshot_start,
                      h.snapshot_end,
                      h.recorder->unit,
                      HistogramValues(h.history),
                      HistogramValues(h.snapshot)) {}

  HistogramData(const std::chrono::system_clock::time_point& start,
                const std::chrono::system_clock::time_point& snapshot_start,
                const std::chrono::system_clock::time_point& snapshot_end,
                Unit unit,
                const HistogramValues& history,
                const HistogramValues& last_snapshot)
      : start(start),
        snapshot_start(snapshot_start),
        snapshot_end(snapshot_end),
        unit(unit),
        history(history),
        last_snapshot(last_snapshot) {}

  bool operator==(const HistogramData& other) const {
    return start == other.start && snapshot_start == other.snapshot_start && snapshot_end == other.snapshot_end &&
           unit == other.unit && history == other.history && last_snapshot == other.last_snapshot;
  }
  bool operator!=(const HistogramData& other) const { return !(*this == other); }

  std::chrono::system_clock::time_point start;
  std::chrono::system_clock::time_point snapshot_start;
  std::chrono::system_clock::time_point snapshot_end;
  Unit unit;
  HistogramValues history;
  HistogramValues last_snapshot;
};

class PerformanceHandler {
 public:
  void registerComponent(const std::string& name, const Recorders&);

  // List all components
  std::string list() const;

  // List all metrics for a given component
  std::string list(const std::string& component_name) const;

  std::map<Name, HistogramData> get(const std::string& component) const;
  HistogramData get(const std::string& component, const std::string& histogram) const;

  std::string toString(const std::map<Name, HistogramData>&) const;
  std::string toString(const HistogramData&) const;

  // Snapshot all histograms for the given component
  void snapshot(const std::string& component);

 private:
  Histograms& getHistograms(const std::string& component_name);
  Histogram& getHistogram(const std::string& component_name, const std::string& histogram_name);
  const Histograms& getHistograms(const std::string& component_name) const;
  const Histogram& getHistogram(const std::string& component_name, const std::string& histogram_name) const;

  std::map<std::string, Histograms> components_;
  mutable std::mutex mutex_;
};

std::ostream& operator<<(std::ostream& os, const HistogramValues&);
std::ostream& operator<<(std::ostream& os, const HistogramData&);
std::ostream& operator<<(std::ostream& os, const Unit&);

}  // namespace concord::diagnostics
