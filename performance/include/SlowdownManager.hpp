// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>
#include <iostream>
#include "Helper.hpp"
#include "../../kvbc/include/kv_types.hpp"
#include "../../util/include/sliver.hpp"

namespace concord::performance {

enum class SlowdownPhase : uint16_t {
  None,
  BftClientBeforeSendPrimary,
  PreProcessorAfterPreexecPrimary,
  PreProcessorAfterPreexecNonPrimary,
  ConsensusFullCommitMsgProcess,
  PostExecBeforeConflictResolution,
  PostExecAfterConflictResolution,
  StorageBeforeMerkleTree,
  StorageBeforeDbWrite
};

enum class SlowdownPolicyType { BusyWait, AddKeys, Sleep };

struct SlowDownResult {
  SlowdownPhase phase = SlowdownPhase::None;
  uint totalKeyCount = 0;
  uint totalValueSize = 0;
  uint totalWaitDuration = 0;
  uint totalSleepDuration = 0;

  friend std::ostream &operator<<(std::ostream &os, const SlowDownResult &r);
};

inline std::ostream &operator<<(std::ostream &os, const SlowDownResult &r) {
  os << "phase: " << (uint)r.phase << ", totalWaitDuration: " << r.totalWaitDuration
     << ", totalSleepDuration: " << r.totalSleepDuration << ", totalKeyCount: " << r.totalKeyCount
     << ",totalValueSize: " << r.totalValueSize;
  return os;
}

struct SlowdownPolicyConfig {
  SlowdownPolicyType type;
  virtual uint16_t GetSleepDuration() { return 0; }
  virtual uint16_t GetBusyWaitTime() { return 0; }
  virtual uint16_t GetItemCount() { return 0; }
  virtual uint16_t GetKeySize() { return 0; }
  virtual uint16_t GetValueSize() { return 0; }
  virtual ~SlowdownPolicyConfig() = default;

  explicit SlowdownPolicyConfig(SlowdownPolicyType t) : type{t} {}
};
using SlowdownConfiguration = std::unordered_map<SlowdownPhase, std::vector<std::shared_ptr<SlowdownPolicyConfig>>>;

struct SleepPolicyConfig : public SlowdownPolicyConfig {
  uint16_t sleep_duration_ms;

  uint16_t GetSleepDuration() override { return sleep_duration_ms; }

  explicit SleepPolicyConfig(uint16_t dur) : SlowdownPolicyConfig(SlowdownPolicyType::Sleep) {
    sleep_duration_ms = dur;
  }
  SleepPolicyConfig() = delete;
  virtual ~SleepPolicyConfig() = default;
};

struct BusyWaitPolicyConfig : public SleepPolicyConfig {
  uint16_t wait_duration_ms;
  uint16_t GetBusyWaitTime() override { return wait_duration_ms; }

  BusyWaitPolicyConfig() = delete;
  virtual ~BusyWaitPolicyConfig() = default;

  BusyWaitPolicyConfig(uint16_t waitDurMs, uint16_t sleepDurMs)
      : SleepPolicyConfig(sleepDurMs), wait_duration_ms{waitDurMs} {
    type = SlowdownPolicyType::BusyWait;
  }
};

struct AddKeysPolicyConfig : SlowdownPolicyConfig {
  uint16_t key_count;
  uint16_t key_size;
  uint16_t value_size;

  AddKeysPolicyConfig(uint16_t keys, uint16_t ksize, uint16_t vsize)
      : SlowdownPolicyConfig(SlowdownPolicyType::AddKeys), key_count{keys}, key_size{ksize}, value_size{vsize} {}

  AddKeysPolicyConfig() = delete;
  virtual ~AddKeysPolicyConfig() = default;

  uint16_t GetItemCount() override { return key_count; }
  uint16_t GetKeySize() override { return key_size; }
  uint16_t GetValueSize() override { return value_size; }
};

#ifdef USE_SLOWDOWN

template <SlowdownPhase e>
struct phase_tag {};
typedef phase_tag<SlowdownPhase::BftClientBeforeSendPrimary> bftclient_before_send_primary;
typedef phase_tag<SlowdownPhase::PreProcessorAfterPreexecPrimary> pre_processor_aferpe_primary;
typedef phase_tag<SlowdownPhase::PreProcessorAfterPreexecNonPrimary> pre_processor_aferpe_nonprimary;
typedef phase_tag<SlowdownPhase::ConsensusFullCommitMsgProcess> cons_process_fullcommit_msg;
typedef phase_tag<SlowdownPhase::StorageBeforeMerkleTree> storage_before_merkle_tree;
typedef phase_tag<SlowdownPhase::StorageBeforeDbWrite> storage_before_db_write;
typedef phase_tag<SlowdownPhase::PostExecAfterConflictResolution> post_exec_after_conflict_det;
typedef phase_tag<SlowdownPhase::PostExecBeforeConflictResolution> post_exec_before_conflict_det;

class BasePolicy {
 public:
  BasePolicy() = default;
  virtual void Slowdown(SlowDownResult &outRes) {}
  virtual void Slowdown(concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &outRes) {}
  virtual ~BasePolicy() = default;
};

class BusyWaitPolicy : public BasePolicy {
 public:
  explicit BusyWaitPolicy(SlowdownPolicyConfig *c)
      : sleepTime_{c->GetSleepDuration()}, waitTime_{c->GetBusyWaitTime()} {}

  void Slowdown(SlowDownResult &outRes) override {
    if (waitTime_ == 0) return;
    auto s = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - s).count() <
           waitTime_)
      std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime_));
    outRes.totalSleepDuration += sleepTime_;
    outRes.totalWaitDuration += waitTime_;
  }

  void Slowdown(concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &outRes) override { Slowdown(outRes); }

  virtual ~BusyWaitPolicy() = default;

 private:
  uint sleepTime_;
  uint waitTime_;
};

class SleepPolicy : public BasePolicy {
 public:
  explicit SleepPolicy(SlowdownPolicyConfig *c) : sleepDuration_{c->GetSleepDuration()} {}

  void Slowdown(SlowDownResult &outRes) override {
    if (sleepDuration_ == 0) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration_));
    outRes.totalSleepDuration += sleepDuration_;
  }

  void Slowdown(concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &outRes) override { Slowdown(outRes); }

  virtual ~SleepPolicy() = default;

 private:
  uint sleepDuration_;
};

class AddKeysPolicy : public BasePolicy {
 public:
  explicit AddKeysPolicy(SlowdownPolicyConfig *c)
      : keyCount_{c->GetItemCount()}, keySize_{c->GetKeySize()}, valueSize_{c->GetValueSize()} {
    prgState_.a = std::chrono::steady_clock::now().time_since_epoch().count();
    data_.reserve(keyCount_ * 10);
    for (uint i = 0; i < keyCount_ * 10; ++i) {
      std::vector<uint64_t> key;
      uint size = 0;
      while (size < c->GetKeySize()) {
        auto t = xorshift64(&prgState_);
        key.push_back(t);
        size += sizeof(uint64_t);
      }

      size = 0;
      std::vector<uint64_t> value;
      while (size < c->GetValueSize()) {
        auto t = xorshift64(&prgState_);
        value.push_back(t);
        size += sizeof(uint64_t);
      }
      totalValueSize_ += size;

      auto *key_data = new uint64_t[key.size()];
      std::copy(key.begin(), key.end(), key_data);
      concordUtils::Sliver k{(const char *)key_data, key.size() * sizeof(uint64_t)};

      if (valueSize_ > 0) {
        auto *value_data = new uint64_t[value.size()];
        std::copy(value.begin(), value.end(), value_data);
        concordUtils::Sliver v{(const char *)value_data, value.size() * sizeof(uint64_t)};
        data_.emplace_back(k, v);
      } else {
        data_.emplace_back(k, concordUtils::Sliver{});
      }
    }
  }

  void Slowdown(concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &outRes) override {
    for (uint i = 0; i < keyCount_;) {
      auto ind = std::rand() % data_.size();
      auto key = data_[ind].first;
      if (set.find(key) != set.end()) continue;
      ++i;
      set[key] = data_[ind].second;
      outRes.totalValueSize += data_[ind].second.size();
    }

    outRes.totalKeyCount += keyCount_;
  }

  virtual ~AddKeysPolicy() = default;

 private:
  uint keyCount_ = 0;
  uint keySize_ = 0;
  uint valueSize_ = 0;
  uint totalValueSize_ = 0;
  std::vector<concord::kvbc::KeyValuePair> data_;
  xorshift64_state prgState_{};
};

class SlowdownManager {
 private:
  std::vector<std::shared_ptr<BasePolicy>> GetPolicies(const SlowdownPhase &phase, SlowDownResult &outRes) {
    auto it = config_.find(phase);
    if (config_.end() == it) {
      outRes.phase = SlowdownPhase::None;
      return std::vector<std::shared_ptr<BasePolicy>>();
    }
    outRes.phase = phase;
    return it->second;
  }

  void DelayInternal(SlowdownPhase phase, SlowDownResult &res) {
    auto policies = GetPolicies(phase, res);
    for (auto &policy : policies) policy->Slowdown(res);
  }

  void DelayInternal(SlowdownPhase phase, concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &res) {
    auto policies = GetPolicies(phase, res);
    for (auto &policy : policies) policy->Slowdown(set, res);
  }

  void DelayImp(bftclient_before_send_primary, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::BftClientBeforeSendPrimary, res);
  }

  void DelayImp(pre_processor_aferpe_primary, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::PreProcessorAfterPreexecPrimary, res);
  }

  void DelayImp(pre_processor_aferpe_nonprimary, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::PreProcessorAfterPreexecNonPrimary, res);
  }

  void DelayImp(cons_process_fullcommit_msg, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::ConsensusFullCommitMsgProcess, res);
  }

  void DelayImp(storage_before_merkle_tree, concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::StorageBeforeMerkleTree, set, res);
  }

  void DelayImp(storage_before_merkle_tree, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::StorageBeforeMerkleTree, res);
  }

  void DelayImp(storage_before_db_write, concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::StorageBeforeDbWrite, set, res);
  }

  void DelayImp(storage_before_db_write, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::StorageBeforeDbWrite, res);
  }

  void DelayImp(post_exec_before_conflict_det, concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::PostExecBeforeConflictResolution, set, res);
  }

  void DelayImp(post_exec_before_conflict_det, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::PostExecBeforeConflictResolution, res);
  }

  void DelayImp(post_exec_after_conflict_det, concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::PostExecAfterConflictResolution, set, res);
  }

  void DelayImp(post_exec_after_conflict_det, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::PostExecAfterConflictResolution, res);
  }

 public:
  explicit SlowdownManager(std::shared_ptr<SlowdownConfiguration> &config) {
    using namespace std;
    if (!config) return;
    for (auto &it : *config) {
      vector<shared_ptr<BasePolicy>> policies;
      for (auto &p : it.second) {
        switch (p->type) {
          case SlowdownPolicyType::Sleep:
            policies.push_back(make_shared<SleepPolicy>(p.get()));
            break;
          case SlowdownPolicyType::BusyWait:
            policies.push_back(make_shared<BusyWaitPolicy>(p.get()));
            break;
          case SlowdownPolicyType::AddKeys:
            policies.push_back(make_shared<AddKeysPolicy>(p.get()));
            break;
          default:
            throw runtime_error("unsupported policy");
        }
      }
      config_[it.first] = policies;
    }
  }

  SlowdownManager() = default;

  template <SlowdownPhase T>
  SlowDownResult Delay(concord::kvbc::SetOfKeyValuePairs &set) {
    SlowDownResult res;
    auto t = phase_tag<T>();
    DelayImp(t, set, res);
    return res;
  }

  template <SlowdownPhase T>
  SlowDownResult Delay() {
    SlowDownResult res;
    auto t = phase_tag<T>();
    DelayImp(t, res);
    return res;
  }

  ~SlowdownManager() = default;

 private:
  std::unordered_map<SlowdownPhase, std::vector<std::shared_ptr<BasePolicy>>> config_;
};

#else

class SlowdownManager {
 public:
  SlowdownManager() {}

  SlowdownManager(std::shared_ptr<SlowdownConfiguration> &config) {}

  template <SlowdownPhase T>
  SlowDownResult Delay(concord::kvbc::SetOfKeyValuePairs &set) {
    return SlowDownResult();
  }

  template <SlowdownPhase T>
  SlowDownResult Delay() {
    return SlowDownResult();
  }

  ~SlowdownManager() {}
};

#endif

}  // namespace concord::performance
