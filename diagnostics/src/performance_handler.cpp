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

#include <iomanip>

#include "performance_handler.h"

static logging::Logger DIAG_LOGGER = logging::getLogger("concord.diag.perf");

namespace concord::diagnostics {

void Recorder::record(int64_t val) {
  if (!hdr_interval_recorder_record_value(&(recorder), val)) {
    LOG_WARN(DIAG_LOGGER, "Failed to record value: " << KVLOG(name, val, unit));
  }
}

void Recorder::recordAtomic(int64_t val) {
  if (!hdr_interval_recorder_record_value_atomic(&(recorder), val)) {
    LOG_WARN(DIAG_LOGGER, "Failed to record value: " << KVLOG(name, val, unit));
  }
}

void PerformanceHandler::registerComponent(const std::string& name,
                                           const std::vector<std::shared_ptr<Recorder>>& recorders) {
  std::lock_guard<std::mutex> guard(mutex_);
  // Comment: since registerComponent might be called from multiple threads, checking if registered before calling
  // this function doesn't make sense. Do not terminate if component is already registered. just exit with warning.
  if (components_.count(name)) {
    LOG_WARN(DIAG_LOGGER, "Component already registered: " << name);
    return;
  }
  Histograms histograms;
  for (const auto& recorder : recorders) {
    histograms.emplace(recorder->name, recorder);
  }
  components_.insert({name, std::move(histograms)});
}

void PerformanceHandler::unRegisterComponent(const std::string& name) {
  std::lock_guard<std::mutex> guard(mutex_);

  // Comment: since unRegisterComponent might be called from multiple threads, checking if registered before calling
  // this function doesn't make sense. Do not terminate if component is not registered. just exit with warning.
  if (components_.count(name) == 0) {
    LOG_WARN(DIAG_LOGGER, "Component is not registered: " << name);
    return;
  }
  components_.erase(name);
}

std::string PerformanceHandler::list() const {
  std::lock_guard<std::mutex> guard(mutex_);
  std::string output;
  for (const auto& [name, _] : components_) {
    (void)_;  // unused variable hack
    output += name + "\n";
  }
  return output;
}

std::string PerformanceHandler::list(const std::string& component_name) const {
  std::lock_guard<std::mutex> guard(mutex_);
  std::string output;
  for (const auto& [name, _] : getHistograms(component_name)) {
    (void)_;  // unused variable hack
    output += name + "\n";
  }
  return output;
}

std::map<Name, HistogramData> PerformanceHandler::get(const std::string& name) const {
  std::lock_guard<std::mutex> guard(mutex_);
  std::map<Name, HistogramData> data;
  for (const auto& [name, histogram] : getHistograms(name)) {
    data.try_emplace(name,
                     histogram.start,
                     histogram.snapshot_start,
                     histogram.snapshot_end,
                     histogram.recorder->unit,
                     histogram.history,
                     histogram.snapshot);
  }
  return data;
}

HistogramData PerformanceHandler::get(const std::string& component_name, const std::string& histogram_name) const {
  std::lock_guard<std::mutex> guard(mutex_);
  return HistogramData(getHistogram(component_name, histogram_name));
}

void PerformanceHandler::snapshot(const std::string& name) {
  std::lock_guard<std::mutex> guard(mutex_);
  for (auto& h : getHistograms(name)) {
    h.second.takeSnapshot();
  }
}

Histograms& PerformanceHandler::getHistograms(const std::string& name) {
  try {
    return components_.at(name);
  } catch (...) {
    throw std::invalid_argument(std::string("Component Not Found: ") + name);
  }
}

Histogram& PerformanceHandler::getHistogram(const std::string& component_name, const std::string& histogram_name) {
  auto& histograms = getHistograms(component_name);
  try {
    return histograms.at(histogram_name);
  } catch (...) {
    throw std::invalid_argument(std::string("Histogram Not Found: ") + component_name + "/" + histogram_name);
  }
}

const Histograms& PerformanceHandler::getHistograms(const std::string& name) const {
  try {
    return components_.at(name);
  } catch (...) {
    throw std::invalid_argument(std::string("Component Not Found: ") + name);
  }
}

const Histogram& PerformanceHandler::getHistogram(const std::string& component_name,
                                                  const std::string& histogram_name) const {
  auto& histograms = getHistograms(component_name);
  try {
    return histograms.at(histogram_name);
  } catch (...) {
    throw std::invalid_argument(std::string("Histogram Not Found: ") + component_name + "/" + histogram_name);
  }
}

std::ostream& operator<<(std::ostream& os, const HistogramValues& values) {
  os << "Count: " << values.count << std::endl;
  os << "Max: " << values.max << std::endl;
  os << "Min: " << values.min << std::endl;
  os << "Percentiles: " << std::endl;
  os << "  10: " << values.pct_10 << std::endl;
  os << "  25: " << values.pct_25 << std::endl;
  os << "  50: " << values.pct_50 << std::endl;
  os << "  75: " << values.pct_75 << std::endl;
  os << "  90: " << values.pct_90 << std::endl;
  os << "  95: " << values.pct_95 << std::endl;
  os << "  99: " << values.pct_99 << std::endl;
  os << "  99.9: " << values.pct_99_9 << std::endl;
  os << "  99.99: " << values.pct_99_99 << std::endl;
  os << "  99.999: " << values.pct_99_999 << std::endl;
  os << "  99.9999: " << values.pct_99_9999 << std::endl;
  os << "  99.99999: " << values.pct_99_99999 << std::endl;
  return os;
}

using std::chrono::system_clock;

std::ostream& operator<<(std::ostream& os, const HistogramData& data) {
  auto start = system_clock::to_time_t(data.start);
  auto snapshot_start = system_clock::to_time_t(data.snapshot_start);
  auto snapshot_end = system_clock::to_time_t(data.snapshot_end);

  os << "Start time (UTC): " << std::put_time(std::gmtime(&start), "%F %T") << std::endl;
  os << "Snapshot Start time (UTC): " << std::put_time(std::gmtime(&snapshot_start), "%F %T") << std::endl;
  os << "Snapshot End time (UTC): " << std::put_time(std::gmtime(&snapshot_end), "%F %T") << std::endl;
  os << "Unit: " << data.unit << std::endl << std::endl;
  os << "History" << std::endl << "--------------------" << std::endl;
  os << data.history << std::endl;
  os << "Snapshot" << std::endl << "--------------------" << std::endl;
  os << data.last_snapshot;
  return os;
}

std::string PerformanceHandler::toString(const std::map<Name, HistogramData>& hist_data) const {
  std::ostringstream oss;
  for (const auto& [name, data] : hist_data) {
    oss << name << std::endl << "====================" << std::endl << data << std::endl;
  }
  return oss.str();
}

std::string PerformanceHandler::toString(const HistogramData& data) const {
  std::ostringstream oss;
  oss << data;
  return oss.str();
}

std::ostream& operator<<(std::ostream& os, const Unit& unit) {
  switch (unit) {
    case Unit::NANOSECONDS:
      os << "nanoseconds";
      break;
    case Unit::MICROSECONDS:
      os << "microseconds";
      break;
    case Unit::MILLISECONDS:
      os << "milliseconds";
      break;
    case Unit::SECONDS:
      os << "seconds";
      break;
    case Unit::MINUTES:
      os << "minutes";
      break;
    case Unit::BYTES:
      os << "bytes";
      break;
    case Unit::KB:
      os << "kb";
      break;
    case Unit::MB:
      os << "mb";
      break;
    case Unit::GB:
      os << "mb";
      break;
    case Unit::COUNT:
      os << "count";
      break;
  }
  return os;
}

void Histogram::takeSnapshot() {
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

}  // namespace concord::diagnostics
