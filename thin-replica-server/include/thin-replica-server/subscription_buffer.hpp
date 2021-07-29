// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#ifndef CONCORD_THIN_REPLICA_SUBSCRIPTION_BUFFER_HPP_
#define CONCORD_THIN_REPLICA_SUBSCRIPTION_BUFFER_HPP_

#include <categorization/updates.h>
#include <boost/lockfree/spsc_queue.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <tuple>
#include <unordered_set>
#include "Logger.hpp"
#include "assertUtils.hpp"
#include "block_update/block_update.hpp"
#include "block_update/event_group_update.hpp"
#include "kv_types.hpp"

namespace concord {
namespace thin_replica {

class ConsumerTooSlow : public std::runtime_error {
 public:
  ConsumerTooSlow() : std::runtime_error("Updates are not consumed fast enough"){};
};

typedef kvbc::BlockUpdate SubUpdate;
typedef kvbc::EventGroupUpdate SubEventGroupUpdate;

// Each subscriber creates its own spsc queue and puts it into the shared list
// of subscriber buffers. This is a thread-safe implementation around boost's
// spsc queue in order to use an additional wake-up mechanism. We expect a
// single producer (the commands handler) and a single consumer (the subscriber
// thread in the thin replica gRPC service).
class SubUpdateBuffer {
 public:
  explicit SubUpdateBuffer(size_t size)
      : logger_(logging::getLogger("concord.thin_replica.sub_buffer")),
        queue_(size),
        eg_queue_(size),
        too_slow_(false),
        eg_too_slow_(false),
        newest_block_id_(0),
        newest_event_group_id_(0) {}

  // Let's help ourselves and make sure we don't copy this buffer
  SubUpdateBuffer(const SubUpdateBuffer&) = delete;
  SubUpdateBuffer& operator=(const SubUpdateBuffer&) = delete;

  // Add an update to the queue and notify waiting subscribers
  void Push(const SubUpdate& update) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (!too_slow_ && !queue_.push(update)) {
        // If we fail to push a new update (because the queue is full) we
        // indicate that this queue is unusable and the reader should clean-up.
        // Not stopping the subscription will lead to a failure on the consumer
        // end (TRC) eventually. Therefore, let's stop it right here.
        too_slow_ = true;
        LOG_WARN(logger_, "Failed to add update. Consumer too slow.");
      } else {
        newest_block_id_ = update.block_id;
      }
    }
    cv_.notify_one();
  };

  // Add an update to the queue and notify waiting subscribers
  void PushEventGroup(const SubEventGroupUpdate& update) {
    {
      std::unique_lock<std::mutex> lock(eg_mutex_);
      if (!eg_too_slow_ && !eg_queue_.push(update)) {
        // If we fail to push a new update (because the queue is full) we
        // indicate that this queue is unusable and the reader should clean-up.
        // Not stopping the subscription will lead to a failure on the consumer
        // end (TRC) eventually. Therefore, let's stop it right here.
        eg_too_slow_ = true;
        LOG_WARN(logger_, "Failed to add update. Consumer too slow.");
      } else {
        newest_event_group_id_ = update.event_group_id;
      }
    }
    eg_cv_.notify_one();
  };

  // Return the oldest update (block if queue is empty)
  void Pop(SubUpdate& out) {
    std::unique_lock<std::mutex> lock(mutex_);
    // Boost's spsc queue is wait-free but we want to block here
    cv_.wait(lock, [this] { return too_slow_ || queue_.read_available(); });

    if (too_slow_) {
      // We throw an exception because we cannot handle the clean-up ourselves
      // and it doesn't make sense to continue pushing/popping updates.
      throw ConsumerTooSlow();
    }

    ConcordAssert(queue_.pop(out));
  };

  // Return the oldest update (event group if queue is empty)
  void PopEventGroup(SubEventGroupUpdate& out) {
    std::unique_lock<std::mutex> lock(eg_mutex_);
    // Boost's spsc queue is wait-free but we want to block here
    eg_cv_.wait(lock, [this] { return eg_too_slow_ || eg_queue_.read_available(); });

    if (eg_too_slow_) {
      // We throw an exception because we cannot handle the clean-up ourselves
      // and it doesn't make sense to continue pushing/popping updates.
      throw ConsumerTooSlow();
    }

    ConcordAssert(eg_queue_.pop(out));
  };

  void waitUntilNonEmpty() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return queue_.read_available(); });
  }

  template <typename RepT, typename PeriodT>
  [[nodiscard]] bool waitUntilNonEmpty(const std::chrono::duration<RepT, PeriodT>& duration) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, duration, [this] { return queue_.read_available(); });
  }

  void waitForEventGroupUntilNonEmpty() {
    std::unique_lock<std::mutex> lock(eg_mutex_);
    cv_.wait(lock, [this] { return queue_.read_available(); });
  }

  template <typename RepT, typename PeriodT>
  [[nodiscard]] bool waitForEventGroupUntilNonEmpty(const std::chrono::duration<RepT, PeriodT>& duration) {
    std::unique_lock<std::mutex> lock(eg_mutex_);
    return eg_cv_.wait_for(lock, duration, [this] { return eg_queue_.read_available(); });
  }

  // This is not thread-safe and the caller has to make sure that there is no
  // writer or reader active. This is a trade-off in order to stay lock-free.
  void removeAllUpdates() {
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.reset();
  }

  // This is not thread-safe and the caller has to make sure that there is no
  // writer or reader active. This is a trade-off in order to stay lock-free.
  void removeAllEventGroupUpdates() {
    std::unique_lock<std::mutex> lock(eg_mutex_);
    eg_queue_.reset();
  }

  // The caller needs to make sure that the queue is not empty when calling
  kvbc::BlockId newestBlockId() {
    std::unique_lock<std::mutex> lock(mutex_);
    // We cannot reach the newest element without consuming the others. Hence,
    // the counter is a workaround but it requires that there is at least one
    // element in the queue.
    ConcordAssertGT(queue_.read_available(), 0);
    return newest_block_id_;
  }

  // The caller needs to make sure that the queue is not empty when calling
  kvbc::EventGroupId newestEventGroupId() {
    std::unique_lock<std::mutex> lock(eg_mutex_);
    // We cannot reach the newest element without consuming the others. Hence,
    // the counter is a workaround but it requires that there is at least one
    // element in the queue.
    ConcordAssertGT(eg_queue_.read_available(), 0);
    return newest_event_group_id_;
  }

  // The caller needs to make sure that the queue is not empty when calling
  kvbc::BlockId oldestBlockId() {
    std::unique_lock<std::mutex> lock(mutex_);
    // Undefined behavior if the queue is empty
    ConcordAssertGT(queue_.read_available(), 0);
    return queue_.front().block_id;
  }

  // The caller needs to make sure that the queue is not empty when calling
  kvbc::EventGroupId oldestEventGroupId() {
    std::unique_lock<std::mutex> lock(eg_mutex_);
    // Undefined behavior if the queue is empty
    ConcordAssertGT(eg_queue_.read_available(), 0);
    return eg_queue_.front().event_group_id;
  }

  bool Empty() {
    std::unique_lock<std::mutex> lock(mutex_);
    return !queue_.read_available();
  }

  bool EmptyEventGroupQueue() {
    std::unique_lock<std::mutex> lock(eg_mutex_);
    return !eg_queue_.read_available();
  }

  bool Full() {
    std::unique_lock<std::mutex> lock(mutex_);
    return !queue_.write_available();
  }

  bool FullEventGroupQueue() {
    std::unique_lock<std::mutex> lock(eg_mutex_);
    return !eg_queue_.write_available();
  }

  // Return the number of elements in the queue
  size_t Size() {
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.read_available();
  }

  // Return the number of elements in the queue
  size_t SizeEventGroupQueue() {
    std::unique_lock<std::mutex> lock(eg_mutex_);
    return eg_queue_.read_available();
  }

 private:
  logging::Logger logger_;
  boost::lockfree::spsc_queue<SubUpdate> queue_;
  boost::lockfree::spsc_queue<SubEventGroupUpdate> eg_queue_;
  // lock used for updating the queue as well as the variables below
  std::mutex mutex_;
  std::condition_variable cv_;
  std::mutex eg_mutex_;
  std::condition_variable eg_cv_;

  // Indidcate whether the consumer doesn't read fast enough
  bool too_slow_;
  bool eg_too_slow_;
  uint64_t newest_block_id_;
  uint64_t newest_event_group_id_;
};

// Thread-safe list implementation which manages subscriber queues. You can
// think of this list as the list of subscribers whereby each subscriber is
// represented by its spsc queue. The presence or absence of a buffer determines
// whether a subscriber is subscribed or unsubscribed respectively.
class SubBufferList {
 public:
  SubBufferList() {}

  // Let's help ourselves and make sure we don't copy this list
  SubBufferList(const SubBufferList&) = delete;
  SubBufferList& operator=(const SubBufferList&) = delete;

  // Add a subscriber
  virtual bool addBuffer(std::shared_ptr<SubUpdateBuffer> elem) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto success = subscriber_.insert(elem).second;
    return success;
  }

  // Remove a subscriber
  virtual void removeBuffer(std::shared_ptr<SubUpdateBuffer> elem) {
    std::lock_guard<std::mutex> lock(mutex_);
    // If the assert fires then there is a logic error somewhere
    ConcordAssert(subscriber_.erase(elem) == 1);
  }

  // Populate updates to all subscribers
  // Note: This is potentially expensive depending on the update size and the
  // number of subscribers. If TRC/TRS stays then we might want to think about
  // an optimization.
  virtual void updateSubBuffers(SubUpdate& update) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& it : subscriber_) {
      it->Push(update);
    }
  }

  virtual void updateEventGroupSubBuffers(SubEventGroupUpdate& update) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& it : subscriber_) {
      it->PushEventGroup(update);
    }
  }

  // Current number of subscribers
  virtual size_t Size() {
    std::lock_guard<std::mutex> lock(mutex_);
    return subscriber_.size();
  }
  virtual ~SubBufferList() = default;

 protected:
  std::unordered_set<std::shared_ptr<SubUpdateBuffer>> subscriber_;
  std::mutex mutex_;
};

}  // namespace thin_replica
}  // namespace concord

#endif  // CONCORD_THIN_REPLICA_SUBSCRIPTION_BUFFER_HPP_
