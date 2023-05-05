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
#include <chrono>
#include <optional>
#include <boost/histogram.hpp>

#include "log/logger.hpp"
#include "util/assertUtils.hpp"

namespace concord::diagnostics {

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

using axes_t = std::vector<boost::histogram::axis::regular<>>;

// A recorder is a thread-safe type that should always be created in a shared pointer to ensure
// proper destruction. The recorder is used to add values to the histogram in the recording thread.
// Interval histograms can be extracted in other threads.
struct Recorder {
  Recorder(const std::string& name, int64_t num_bins, int64_t highest_trackable_value, Unit unit)
      : Recorder(num_bins, highest_trackable_value, unit) {
    this->name = name;
  }

  Recorder(int64_t num_bins, int64_t highest_trackable_value, Unit unit) : unit(unit) {
    histogram_ =
        boost::histogram::make_histogram(boost::histogram::axis::regular<>(num_bins, 0, highest_trackable_value));
  }

  ~Recorder() {}
  Recorder(const Recorder&) = delete;
  Recorder& operator=(const Recorder&) = delete;

  void record(int64_t val) { histogram_(val); }

  boost::histogram::histogram<axes_t> histogram_;
  Unit unit;
  // Set during registration
  std::string name;
};

#define MAKE_SHARED_RECORDER(name, num_bins, highest, unit) \
  std::make_shared<concord::diagnostics::Recorder>(name, num_bins, highest, unit)

#define DEFINE_SHARED_RECORDER(name, num_bins, highest, unit) \
  std::shared_ptr<concord::diagnostics::Recorder> name = MAKE_SHARED_RECORDER(#name, num_bins, highest, unit)
// This class should be instantiated to measure a duration of a scope and add it to a histogram
// recorder. The measurement is taken and recorded in the destructor.

class TimeRecorder {
 public:
  TimeRecorder(Recorder& recorder) : start_(std::chrono::steady_clock::now()), recorder_(&recorder), record_(true) {}
  TimeRecorder() : start_(std::chrono::steady_clock::time_point::min()), recorder_(nullptr), record_(false) {}
  TimeRecorder(TimeRecorder&& rhs) : start_(rhs.start_), recorder_(rhs.recorder_), record_(rhs.record_) {
    rhs.recorder_ = nullptr;
    rhs.record_ = false;
  }
  TimeRecorder& operator=(TimeRecorder&& rhs) {
    start_ = rhs.start_;
    recorder_ = rhs.recorder_;
    record_ = rhs.record_;
    rhs.recorder_ = nullptr;
    rhs.record_ = false;
    return *this;
  }

  // In some cases we don't want to record on destruction.
  void doNotRecord() { record_ = false; }

  int64_t wrapUpRecording() {
    int64_t durationInNano = 0;
    if (!record_) return durationInNano;
    switch (recorder_->unit) {
      case Unit::NANOSECONDS: {
        auto interval = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_);
        recorder_->record(interval.count());
        durationInNano = interval.count();
      } break;
      case Unit::MICROSECONDS: {
        auto interval =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_);
        recorder_->record(interval.count());
        durationInNano = interval.count() * 1000;
      } break;
      case Unit::MILLISECONDS: {
        auto interval =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_);
        recorder_->record(interval.count());
        durationInNano = interval.count() * 1000000;
      } break;
      case Unit::SECONDS: {
        auto interval = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start_);
        recorder_->record(interval.count());
        durationInNano = interval.count() * 1000000000;
      } break;
      case Unit::MINUTES: {
        auto interval = std::chrono::duration_cast<std::chrono::minutes>(std::chrono::steady_clock::now() - start_);
        recorder_->record(interval.count());
        durationInNano = interval.count() * 1000000000000;
      } break;
      default:
        ConcordAssert(false);
    }
    return durationInNano;
  }

  ~TimeRecorder() { wrapUpRecording(); }

  TimeRecorder(const TimeRecorder&) = delete;
  TimeRecorder& operator=(const TimeRecorder&) = delete;

 private:
  std::chrono::steady_clock::time_point start_;
  Recorder* recorder_;
  bool record_;
};

// This is a wrapper around an unordered_map that records the durations of asynchronous actions.
// It's useful when the timing being recorded can't tracked in a single scope, and there are
// multiple outstanding requests that need timing, such as consensus slots.
// We allow atomic operations on the elements of the unordered_map, but the map itself is not thread safe.
template <typename Key>
class AsyncTimeRecorderMap {
 public:
  AsyncTimeRecorderMap(const std::shared_ptr<Recorder>& recorder) : recorder_(recorder) {}

  void start(Key key) { timers_.emplace(key, *recorder_); }
  void end(Key key) { timers_.erase(key); }
  void clear() {
    for (auto& t : timers_) {
      t.second.doNotRecord();
    }
    timers_.clear();
  }

 private:
  std::shared_ptr<Recorder> recorder_;
  std::unordered_map<Key, TimeRecorder> timers_;
};

// This allows starting and stopping a timer manually rather than using the destructor. It's useful
// for async operations without a linear control-flow.

class AsyncTimeRecorder {
 public:
  AsyncTimeRecorder(const std::shared_ptr<Recorder>& recorder) : recorder_(recorder) {}
  void start() {
    // If a timer was already started, it will record if start is called again before end.
    // This behavior can be prevented by explicitly calling clear().
    timer_.emplace(*recorder_);
  }
  void end() { timer_.reset(); }
  void clear() {
    if (timer_) {
      timer_->doNotRecord();
      timer_.reset();
    }
  }

 private:
  std::shared_ptr<Recorder> recorder_;
  std::optional<TimeRecorder> timer_;
};

struct Histogram {
  Histogram(const std::shared_ptr<Recorder>& recorder)
      : recorder(recorder), start(std::chrono::system_clock::now()), snapshot_start(start), snapshot_end(start) {
    snapshot = recorder->histogram_;
    history = snapshot;
  }

  ~Histogram() {}

  void takeSnapshot();

  std::shared_ptr<Recorder> recorder;

  std::chrono::system_clock::time_point start;
  std::chrono::system_clock::time_point snapshot_start;
  std::chrono::system_clock::time_point snapshot_end;

  // The histogram interval starting from when the last snapshot was taken
  boost::histogram::histogram<axes_t> snapshot;
  // History doesn't include the latest snapshot.
  boost::histogram::histogram<axes_t> history;
  std::mutex mutex_;
};

using Name = std::string;
using Histograms = std::map<Name, Histogram>;

// Useful data extracted from histogram(s)
//
// By extracting this data we can return it in a thread-safe manner, without having to copy the
// entire histogram.
//
// We specifically don't return the average, as it's a completely meaningless metric.
struct HistogramValues {
  HistogramValues(const boost::histogram::histogram<axes_t>& h) : memory_used(sizeof(h)) {
    max = get_max_value(h);
    if (max == 0) return;  // Histogram is empty
    min = get_min_value(h);
    count = get_total_element(h);
    pct_10 = get_value_at_percentile(h, 10);
    pct_25 = get_value_at_percentile(h, 25);
    pct_50 = get_value_at_percentile(h, 50);
    pct_75 = get_value_at_percentile(h, 75);
    pct_90 = get_value_at_percentile(h, 90);
    pct_95 = get_value_at_percentile(h, 95);
    pct_99 = get_value_at_percentile(h, 99);
    pct_99_9 = get_value_at_percentile(h, 99.9);
    pct_99_99 = get_value_at_percentile(h, 99.99);
    pct_99_999 = get_value_at_percentile(h, 99.999);
    pct_99_9999 = get_value_at_percentile(h, 99.9999);
    pct_99_99999 = get_value_at_percentile(h, 99.99999);
  }

  int64_t get_total_element(const boost::histogram::histogram<axes_t>& h) {
    // Compute the cumulative sum of bin heights
    int64_t total_elem = 0;
    int count = h.axis().size();
    for (auto i = 0; i < count; ++i) {
      total_elem += h.at(i);
    }
    return total_elem;
  }

  int64_t get_max_value(const boost::histogram::histogram<axes_t>& h) {
    int bin_max = 0;
    for (auto i = h.axis().size() - 1; i >= 0; i--) {
      if (h.at(i) > 0) {
        bin_max = h.axis().bin(i).center();
        break;
      }
    }
    return bin_max;
  }

  int64_t get_min_value(const boost::histogram::histogram<axes_t>& h) {
    int bin_min = 0;
    for (auto i = 0; i < h.axis().size(); i++) {
      if (h.at(i) > 0) {
        bin_min = h.axis().bin(i).center();
        break;
      }
    }
    return bin_min;
  }

  int64_t get_value_at_percentile(const boost::histogram::histogram<axes_t>& h, double percent) {
    // Compute the cumulative sum of bin heights
    double cumsum = 0.0;
    int count = h.axis().size();
    for (auto i = 0; i < count; ++i) {
      cumsum += h.at(i) * h.axis().bin(i).center();
    }

    // Find the bin value which is greater than the percentage of all values
    double threshold = percent / 100.0 * cumsum;
    int bin_value = 0;
    double sum = 0.0;
    for (auto i = 0; i < h.axis().size(); ++i) {
      sum += h.at(i) * h.axis().bin(i).center();
      if (sum >= threshold) {
        bin_value = h.axis().bin(i).center();
        break;
      }
    }
    return bin_value;
  }

  bool operator==(const HistogramValues& other) const {
    return count == other.count && min == other.min && max == other.max && pct_10 == other.pct_10 &&
           pct_25 == other.pct_25 && pct_50 == other.pct_50 && pct_75 == other.pct_75 && pct_90 == other.pct_90 &&
           pct_95 == other.pct_95 && pct_99 == other.pct_99 && pct_99_9 == other.pct_99 &&
           pct_99_99 == other.pct_99_99 && pct_99_999 == other.pct_99_999 && pct_99_9999 == other.pct_99_9999 &&
           pct_99_99999 == other.pct_99_99999;
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
  int64_t pct_99_9999 = 0;
  int64_t pct_99_99999 = 0;
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
  /*
  If a component is already registered: print a warning and do nothing. This is the API definition, there is nothing
  ill-defined here. For example std::vector::clear(), std::unique_ptr::reset() or std::set::insert() operates in the
  same way. Same idea for the unregistered part.
  */
  void registerComponent(const std::string& name, const std::vector<std::shared_ptr<Recorder>>&);
  // If a component is already not registered: print a warning and do nothing.
  void unRegisterComponent(const std::string& name);

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

  // DO NOT USE THIS IN PRODUCTION. THIS IS ONLY FOR TESTING, SO THAT WE CAN CLEAR THE SINGLETON AND REREGISTER.
  void clear() {
    std::lock_guard<std::mutex> guard(mutex_);
    components_.clear();
  }

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
