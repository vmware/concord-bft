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

#include "sigData.hpp"
#include "MsgsCommunicator.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "MsgsCommunicator.hpp"
#include "Timers.hpp"
#include "communication/ICommunication.hpp"
#include <memory>

namespace utt {
class SigProcessor {
  /**
   * @brief The sigProcessor responsibility is to compute the partial UTT signature, collecting and combining the full
   * signature.
   *
   */
 public:
  /**
   * @brief JobCoordinatorCb is a callback for choosing the coordinator for a given signature job. The only requirement
   * is that it has to be deterministic among all replicas The default implementation is simply job_id % n
   */
  using JobCoordinatorCb = std::function<uint16_t(uint64_t)>;

  /**
   * @brief GenerateAppClientRequestCb is a callback for generating the content of ClientRequestMsg that will be
   * published by the replica who managed to collect the full signature. The output might be a platform specific (for
   * example, a composed ethereum request) and has to contain the full signature and its id. The default implementation
   * is simply the [sig_id, signature] serialized as a vector of bytes
   *
   */
  using GenerateAppClientRequestCb = std::function<std::vector<uint8_t>(uint64_t, const std::vector<uint8_t>&)>;

  /**
   * @brief Construct a new Sig Processor object
   *
   * @param repId replica id, has to be identical to the bft replica ID
   * @param n the number of replicas
   * @param threshold threshold for the number of required partial signatures (typically f+1)
   * @param timer_handler_timeout the time elapsed before the timer triggers.
   * @param msgsCommunicator a MsgsCommunicator object (should be taken from the bft layer)
   * @param msgHandlers a MsgHandlersRegistrator object (should be taken from the bft layer)
   * @param timers a Timers object (should be taken from the bft layer)
   */
  SigProcessor(uint16_t repId,
               uint16_t n,
               uint16_t threshold,
               uint64_t timer_handler_timeout,
               std::shared_ptr<bftEngine::impl::MsgsCommunicator> msgsCommunicator,
               std::shared_ptr<bftEngine::impl::MsgHandlersRegistrator> msgHandlers,
               concordUtil::Timers& timers);
  /**
   * @brief Set the Coordinator callback
   *
   * @param coordinator_cb
   */
  void setCoordinatorCb(const JobCoordinatorCb& coordinator_cb) { get_job_coordinator_cb = coordinator_cb; }

  /**
   * @brief Set the GenerateAppClientRequestCb callback
   *
   * @param gen_client_req_cb
   */
  void setGenerateClientRequestCb(const GenerateAppClientRequestCb& gen_client_req_cb) {
    generate_app_client_request_cb_ = gen_client_req_cb;
  }
  ~SigProcessor();

  /**
   * @brief Initiates the protocol for computing the full signature for a given UTT object. This is a thread safe method
   * and it can be called by any thread.
   *
   * @param sig_id a vector of identifiers for the signature computing jobs
   * @param sig the UTT partial signature (computed by the caller)
   * @param vcb a callback for validating other partial signatures for this job
   * @param job_timeout a timeout for this job(s). If the timeout passes, the replica will broadcast the partial
   * signature to all. The default is 1000 ms
   */
  void processSignature(uint64_t sig_id,
                        const std::vector<uint8_t>& sig,
                        const validationCb& vcb,
                        uint64_t job_timeout_ms = 1000);

 private:
  /**
   * @brief Handles the event of receiving new partial signature
   *
   * @param sig_id the sig ID
   * @param sig_source the sig creator
   * @param partial_sig the partial signature
   */
  void onReceivingNewPartialSig(uint64_t sig_id, uint16_t sig_source, const std::vector<uint8_t>& partial_sig);

  /**
   * @brief Handles the event of receiving a new valid full signature. The validation assumed to be done by the caller.
   *
   * @param sig_id the signature's job id
   */
  void onReceivingNewValidFullSig(uint64_t sig_id);

  /**
   * @brief Handles the event of timeout for a specific job
   *
   * @param entry the entry that represents the job
   */
  void onJobTimeout(const SigJobEntry& entry);

  std::shared_ptr<bftEngine::impl::MsgsCommunicator> msgs_communicator_;
  concordUtil::Timers& timers_;
  uint16_t repId_;
  uint16_t n_;
  uint16_t threshold_;
  uint64_t timer_handler_timeout_;
  JobCoordinatorCb get_job_coordinator_cb = [&](uint64_t sigId) { return sigId % n_; };
  GenerateAppClientRequestCb generate_app_client_request_cb_ = [](uint64_t sig_id, const std::vector<uint8_t>& sig) {
    std::vector<uint8_t> ret(sizeof(uint64_t) + sig.size(), 0);
    std::memcpy(ret.data(), &sig_id, sizeof(uint64_t));
    std::memcpy(ret.data() + sizeof(uint64_t), sig.data(), sig.size());
    return ret;
  };
  SigJobEntriesMap jobs_;
  concordUtil::Timers::Handle timeout_handler_;
};
}  // namespace utt