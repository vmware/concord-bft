// UTT
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <map>
#include <unordered_map>
#include <mutex>
#include <functional>
namespace utt {
using validationCb = std::function<bool(uint16_t id, const std::vector<uint8_t>&)>;
struct SigJobEntry {
  uint64_t job_id{0};
  uint64_t start_time{0};
  uint64_t job_timeout_ms{1000};
  std::map<uint32_t, std::vector<uint8_t>> partial_sigs;
};

class SigJobEntriesMap {
 public:
  const SigJobEntry& get(uint64_t id) const {
    std::unique_lock lck(lock_);
    return jobs_.at(id);
  }

  bool hasPartialSig(uint64_t id, uint32_t sig_source) const {
    std::unique_lock lck(lock_);
    if (jobs_.find(id) == jobs_.end()) return false;
    return (jobs_.at(id).partial_sigs.find(sig_source) != jobs_.at(id).partial_sigs.end());
  }

  void addPartialSig(uint64_t id, uint32_t sig_source, const std::vector<uint8_t>& sig) {
    std::unique_lock lck(lock_);
    jobs_[id].partial_sigs.emplace(sig_source, sig);
  }

  void removePartialSig(uint64_t id, uint32_t sig_source) {
    std::unique_lock lck(lock_);
    jobs_[id].partial_sigs.erase(sig_source);
  }

  void registerEntry(uint64_t id, const validationCb& vcb, uint64_t job_timeout_ms) {
    std::unique_lock lck(lock_);
    validation_callbacks_[id] = vcb;
    jobs_[id].job_id = id;
    jobs_[id].start_time = std::chrono::system_clock::now().time_since_epoch().count();
    jobs_[id].job_timeout_ms = job_timeout_ms;
  }

  const std::unordered_map<uint64_t, SigJobEntry>& getEntries() {
    std::unique_lock lck(lock_);
    return jobs_;
  }
  const validationCb& getJobValidator(uint64_t job_id) const {
    std::unique_lock lck(lock_);
    return validation_callbacks_.at(job_id);
  }
  bool isRegistered(uint64_t job_id) const {
    std::unique_lock lck(lock_);
    return validation_callbacks_.find(job_id) != validation_callbacks_.end();
  }
  void erase(uint64_t job_id) {
    std::unique_lock lck(lock_);
    jobs_.erase(job_id);
    validation_callbacks_.erase(job_id);
  }

  uint64_t getNumOfPartialSigs(uint64_t job_id) const {
    std::unique_lock lck(lock_);
    if (jobs_.find(job_id) == jobs_.end()) return 0;
    return jobs_.at(job_id).partial_sigs.size();
  }

 private:
  std::unordered_map<uint64_t, SigJobEntry> jobs_;
  std::unordered_map<uint64_t, validationCb> validation_callbacks_;
  mutable std::mutex lock_;
};
}  // namespace utt