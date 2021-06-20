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
#include <cstring>
#include "Helper.hpp"
#include "kv_types.hpp"
#include "sliver.hpp"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include "Logger.hpp"

namespace concord::performance {

enum class SlowdownPhase : uint16_t {
  None,
  BftClientBeforeSendPrimary,
  PreProcessorAfterPreexecPrimary,
  PreProcessorAfterPreexecNonPrimary,
  ConsensusFullCommitMsgProcess,
  PostExecBeforeConflictResolution,
  PostExecAfterConflictResolution,
  StorageBeforeKVBC,
  StorageBeforeDbWrite
};

enum class SlowdownPolicyType { BusyWait, AddKeys, Sleep, MessageDelay };

struct SlowDownResult {
  SlowdownPhase phase = SlowdownPhase::None;
  uint totalKeyCount = 0;
  uint totalValueSize = 0;
  uint totalWaitDuration = 0;
  uint totalSleepDuration = 0;
  uint totalMessageDelayDuration = 0;

  friend std::ostream &operator<<(std::ostream &os, const SlowDownResult &r);
};

inline std::ostream &operator<<(std::ostream &os, const SlowDownResult &r) {
  os << "phase: " << (uint)r.phase << ", totalWaitDuration: " << r.totalWaitDuration
     << ", totalSleepDuration: " << r.totalSleepDuration << ", totalKeyCount: " << r.totalKeyCount
     << ",totalValueSize: " << r.totalValueSize << ", totalMessageDelayDuration:" << r.totalMessageDelayDuration;
  return os;
}

struct SlowdownPolicyConfig {
  SlowdownPolicyType type;
  virtual uint16_t GetSleepDuration() { return 0; }
  virtual uint16_t GetBusyWaitTime() { return 0; }
  virtual uint16_t GetItemCount() { return 0; }
  virtual uint16_t GetKeySize() { return 0; }
  virtual uint16_t GetValueSize() { return 0; }
  virtual uint16_t GetMessageDelayDuration() { return 0; }
  virtual uint16_t GetMessageDelayPollTime() { return 0; }

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

struct MessageDelayPolicyConfig : SlowdownPolicyConfig {
  uint16_t delay_ms;
  uint16_t poll_ms;

  MessageDelayPolicyConfig(uint16_t delay_dur, uint16_t poll_milli)
      : SlowdownPolicyConfig(SlowdownPolicyType::MessageDelay), delay_ms{delay_dur}, poll_ms{poll_milli} {}

  uint16_t GetMessageDelayDuration() override { return delay_ms; }
  uint16_t GetMessageDelayPollTime() override { return poll_ms; }

  MessageDelayPolicyConfig() = delete;
  virtual ~MessageDelayPolicyConfig() = default;
};

#ifdef USE_SLOWDOWN

template <SlowdownPhase e>
struct phase_tag {};
typedef phase_tag<SlowdownPhase::BftClientBeforeSendPrimary> bftclient_before_send_primary;
typedef phase_tag<SlowdownPhase::PreProcessorAfterPreexecPrimary> pre_processor_aferpe_primary;
typedef phase_tag<SlowdownPhase::PreProcessorAfterPreexecNonPrimary> pre_processor_aferpe_nonprimary;
typedef phase_tag<SlowdownPhase::ConsensusFullCommitMsgProcess> cons_process_fullcommit_msg;
typedef phase_tag<SlowdownPhase::StorageBeforeKVBC> storage_before_merkle_tree;
typedef phase_tag<SlowdownPhase::StorageBeforeDbWrite> storage_before_db_write;
typedef phase_tag<SlowdownPhase::PostExecAfterConflictResolution> post_exec_after_conflict_det;
typedef phase_tag<SlowdownPhase::PostExecBeforeConflictResolution> post_exec_before_conflict_det;

class BasePolicy {
 public:
  explicit BasePolicy(logging::Logger &logger) : logger_{logger} {}
  virtual void Slowdown(SlowDownResult &outRes) {}
  virtual void Slowdown(concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &outRes) {}
  virtual void Slowdown(char *msg,
                        size_t &size,
                        std::function<void(char *, size_t &)> &&cback,
                        SlowDownResult &outRes) {}
  virtual ~BasePolicy() = default;

 protected:
  logging::Logger logger_;
};

class BusyWaitPolicy : public BasePolicy {
 public:
  explicit BusyWaitPolicy(SlowdownPolicyConfig *c, logging::Logger &logger)
      : BasePolicy(logger), sleepTime_{c->GetSleepDuration()}, waitTime_{c->GetBusyWaitTime()} {}

  void Slowdown(SlowDownResult &outRes) override {
    LOG_DEBUG(logger_, "BusyWait slowdown, duration: " << waitTime_ << ", sleepTime: " << sleepTime_);
    if (waitTime_ == 0) return;
    done_ = false;
    auto s = std::chrono::steady_clock::now();
    while (!done_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime_));
      outRes.totalSleepDuration += sleepTime_;
      done_ = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - s).count() >=
              waitTime_;
    }

    outRes.totalWaitDuration += waitTime_;
  }

  void Slowdown(concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &outRes) override { Slowdown(outRes); }

  virtual ~BusyWaitPolicy() = default;

 private:
  uint sleepTime_;
  uint waitTime_;
  volatile bool done_ = false;
};

class SleepPolicy : public BasePolicy {
 public:
  explicit SleepPolicy(SlowdownPolicyConfig *c, logging::Logger &logger)
      : BasePolicy(logger), sleepDuration_{c->GetSleepDuration()} {}

  void Slowdown(SlowDownResult &outRes) override {
    LOG_DEBUG(logger_, "Sleep slowdown, duration: " << sleepDuration_);
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
  explicit AddKeysPolicy(SlowdownPolicyConfig *c, logging::Logger &logger)
      : BasePolicy(logger), keyCount_{c->GetItemCount()}, keySize_{c->GetKeySize()}, valueSize_{c->GetValueSize()} {}

  void Slowdown(concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &outRes) override {
    LOG_DEBUG(logger_,
              "Addkeys slowdown, count: " << keyCount_ << ", ksize: " << keySize_ << ", vsize: " << valueSize_);
    for (uint i = 0; i < keyCount_; ++i) {
      char *key_data = nullptr;
      char *value_data = nullptr;
      if (keySize_) {
        key_data = new char[keySize_];
        uint64_t v = counter_.fetch_add(1);
        memcpy(key_data, &(v), sizeof(v));
        for (uint j = sizeof(v); j < keySize_; ++j) {
          key_data[j] = j;
        }
      }
      if (valueSize_) {
        value_data = new char[valueSize_];
        value_data[0] = i;
        for (uint j = 1; j < valueSize_; ++j) {
          value_data[j] = j;
        }
      }

      if (key_data && value_data) {
        concordUtils::Sliver k{key_data, keySize_};
        concordUtils::Sliver v{value_data, valueSize_};
        set.insert({k, v});
        outRes.totalValueSize += v.size();
      } else if (key_data) {
        concordUtils::Sliver k{key_data, keySize_};
        set.insert({k, concordUtils::Sliver{}});
      }
    }

    outRes.totalKeyCount += keyCount_;
  }

  virtual ~AddKeysPolicy() = default;

 private:
  std::atomic<uint64_t> counter_ = 0;
  uint keyCount_ = 0;
  uint keySize_ = 0;
  uint valueSize_ = 0;
};

class MessageDelayPolicy : public BasePolicy {
 private:
  struct DelayInfo {
    char *msg = nullptr;
    size_t msgSize = 0;
    std::chrono::steady_clock::time_point absReturnTime;  // absolute time in future to "end" the delay
    std::function<void(char *, size_t &)> callback = nullptr;

    DelayInfo(char *msgData,
              size_t &size,
              std::chrono::steady_clock::time_point &&absTime,
              std::function<void(char *, size_t &)> &cb)
        : msgSize{size}, absReturnTime{absTime}, callback{cb} {
      // must copy data since it is deleted by the end of the Replica handling
      msg = new char[size];
      memcpy((void *)msg, (const void *)msgData, size);
    };

    virtual ~DelayInfo() { delete[] msg; }

    DelayInfo(const DelayInfo &other) = delete;
    DelayInfo &operator=(const DelayInfo &other) = delete;

    DelayInfo &operator=(DelayInfo &&other) {
      if (this != &other) {
        delete[] msg;
        msg = other.msg;
        msgSize = other.msgSize;
        absReturnTime = other.absReturnTime;
        callback = other.callback;
        other.msg = nullptr;
        other.callback = nullptr;
        other.msgSize = 0;
      }
      return *this;
    }

    DelayInfo(DelayInfo &&other) { *this = std::move(other); }
  };

  class Compare {
   public:
    bool operator()(DelayInfo &a, DelayInfo &b) {
      if (a.absReturnTime > b.absReturnTime) return true;
      return false;
    }
  };

  void timer() {
    while (!done_) {
      std::unique_lock<std::mutex> ul(lock_);
      if (delayQueue_.empty()) {
        cv_.wait(ul, [&]() { return done_ || !delayQueue_.empty(); });
      }
      while (!delayQueue_.empty()) {
        // this is the only way to use move with priority queue since there are no iterators there and top() returns
        // const &
        auto delayedMsg = std::move(const_cast<DelayInfo &>(delayQueue_.top()));
        if (std::chrono::steady_clock::now() < delayedMsg.absReturnTime) {
          // since we move the top message for checking delay, the one in the Q is NULLed.
          // if this message is not ready to be re-delivered, return it to the Q and remove the invalid one from the top
          delayQueue_.pop();
          delayQueue_.push(std::move(delayedMsg));
          break;
        }
        if (delayedMsg.callback) {
          delayedMsg.callback(delayedMsg.msg, delayedMsg.msgSize);
        }
        delayQueue_.pop();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(poll_time_ms_));
    }
  }

  explicit MessageDelayPolicy(SlowdownPolicyConfig *c, logging::Logger &logger)
      : BasePolicy(logger), delay_dur_ms_{c->GetMessageDelayDuration()}, poll_time_ms_{c->GetMessageDelayPollTime()} {
    if (delay_dur_ms_) {
      pollingThread_ = std::make_shared<std::thread>(&MessageDelayPolicy::timer, this);
    }
  }

 public:
  MessageDelayPolicy(MessageDelayPolicy &other) = delete;
  MessageDelayPolicy &operator=(MessageDelayPolicy &other) = delete;

  void Slowdown(char *msg,
                size_t &size,
                std::function<void(char *, size_t &)> &&cback,
                SlowDownResult &outRes) override {
    using namespace std::chrono;
    if (delay_dur_ms_) {
      std::unique_lock<std::mutex> lg(lock_);
      delayQueue_.emplace(msg, size, steady_clock::now() + milliseconds(delay_dur_ms_), cback);
      cv_.notify_all();
    }
    outRes.totalMessageDelayDuration += delay_dur_ms_;
  }

  virtual ~MessageDelayPolicy() {
    done_ = true;
    cv_.notify_all();
    if (pollingThread_ && pollingThread_->joinable()) pollingThread_->join();
  }

  static std::shared_ptr<MessageDelayPolicy> instance(SlowdownPolicyConfig *c, logging::Logger &logger) {
    static std::shared_ptr<MessageDelayPolicy> inst =
        std::shared_ptr<MessageDelayPolicy>(new MessageDelayPolicy(c, logger));
    return inst;
  }

  MessageDelayPolicy(MessageDelayPolicy const &) = delete;
  void operator=(MessageDelayPolicy const &) = delete;

 private:
  uint delay_dur_ms_;
  uint poll_time_ms_;
  std::mutex lock_;
  std::condition_variable cv_;
  std::priority_queue<DelayInfo, std::vector<DelayInfo>, Compare> delayQueue_;
  std::shared_ptr<std::thread> pollingThread_;
  bool done_ = false;
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

  void DelayInternal(
      SlowdownPhase phase, char *msg, size_t size, std::function<void(char *, size_t &)> &&f, SlowDownResult &res) {
    auto policies = GetPolicies(phase, res);
    for (auto &policy : policies)
      policy->Slowdown(msg, size, std::forward<std::function<void(char *, size_t &)>>(f), res);
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

  void DelayImp(cons_process_fullcommit_msg,
                char *msg,
                size_t size,
                std::function<void(char *, size_t &)> &&f,
                SlowDownResult &res) {
    DelayInternal(SlowdownPhase::ConsensusFullCommitMsgProcess,
                  msg,
                  size,
                  std::forward<std::function<void(char *, size_t &)>>(f),
                  res);
  }

  void DelayImp(storage_before_merkle_tree, concord::kvbc::SetOfKeyValuePairs &set, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::StorageBeforeKVBC, set, res);
  }

  void DelayImp(storage_before_merkle_tree, SlowDownResult &res) {
    DelayInternal(SlowdownPhase::StorageBeforeKVBC, res);
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
            policies.push_back(make_shared<SleepPolicy>(p.get(), logger_));
            break;
          case SlowdownPolicyType::BusyWait:
            policies.push_back(make_shared<BusyWaitPolicy>(p.get(), logger_));
            break;
          case SlowdownPolicyType::AddKeys:
            policies.push_back(make_shared<AddKeysPolicy>(p.get(), logger_));
            break;
          case SlowdownPolicyType::MessageDelay:
            policies.push_back(MessageDelayPolicy::instance(p.get(), logger_));
            break;
          default:
            throw runtime_error("unsupported policy");
        }
      }
      config_[it.first] = policies;
    }

    LOG_DEBUG(logger_, "SlowdownManager initialized with " << config_.size() << " phases");
  }

  SlowdownManager() { LOG_DEBUG(logger_, "SlowdownManager initialized with no policies"); };

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

  template <SlowdownPhase T>
  SlowDownResult Delay(char *msg, size_t size, std::function<void(char *, size_t &)> &&f) {
    SlowDownResult res;
    auto t = phase_tag<T>();
    DelayImp(t, msg, size, std::forward<std::function<void(char *, size_t &)>>(f), res);
    return res;
  }

  ~SlowdownManager() = default;

  bool isEnabled() { return true; }

 private:
  std::unordered_map<SlowdownPhase, std::vector<std::shared_ptr<BasePolicy>>> config_;
  logging::Logger logger_ = logging::getLogger("concord.bft.slowdown");
};

#else

class SlowdownManager {
 public:
  SlowdownManager() {}

  SlowdownManager(std::shared_ptr<SlowdownConfiguration> &config) {}

  template <SlowdownPhase T>
  SlowDownResult Delay(concord::kvbc::SetOfKeyValuePairs &set) {
    LOG_DEBUG(logger_, "slowdown was not built !!!");
    return SlowDownResult();
  }

  template <SlowdownPhase T>
  SlowDownResult Delay() {
    LOG_DEBUG(logger_, "slowdown was not built !!!");
    return SlowDownResult();
  }

  template <SlowdownPhase T>
  SlowDownResult Delay(char *msg, size_t size, std::function<void(char *, size_t &)> &&f) {
    return SlowDownResult();
  }

  bool isEnabled() { return false; }

  ~SlowdownManager() {}

 private:
  logging::Logger logger_ = logging::getLogger("concord.bft.slowdown");
};

#endif

}  // namespace concord::performance
