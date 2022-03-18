// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ShufflePrePrepareMsgStrategy.hpp"
#include "StrategyUtils.hpp"

#include "bftengine/ClientMsgs.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/ClientRequestMsg.hpp"

#include "Digest.hpp"

using concord::util::digest::DigestUtil;
namespace concord::kvbc::strategy {

using bftEngine::impl::MessageBase;
using bftEngine::impl::PrePrepareMsg;
using bftEngine::ClientRequestMsgHeader;

// This is a reference change method, which does the following changes to the message:
// It first tosses a coin
// If the coin toss is 1, it will take any 2 random consecutive client requests
// Changes the request and fill it with some ramdom string
// Swap the randomly chosen requests
// Update the digest and then send the shuffled message.
bool ShufflePrePrepareMsgStrategy::changeMessage(std::shared_ptr<MessageBase>& msg) {
  PrePrepareMsg& nmsg = static_cast<PrePrepareMsg&>(*(msg.get()));
  std::mt19937_64 eng{std::random_device{}()};
  std::uniform_int_distribution<> coin{0, 1};

  if ((nmsg.numberOfRequests() >= 2) && (coin(eng) == 1)) {  // Randomly appear good or bad.
    std::vector<std::tuple<char*, size_t, std::unique_ptr<char[]>>> clientMsgs;

    std::uniform_int_distribution<> index{0, static_cast<int32_t>(nmsg.numberOfRequests() - 2)};
    size_t swapIdx = nmsg.numberOfRequests() == 2 ? 0 : index(eng);
    size_t idx = 0;

    std::vector<std::string> sigOrDigestOfRequest(nmsg.numberOfRequests());
    auto it = RequestsIterator(&nmsg);
    char* requestBody = nullptr;

    while (it.getAndGoToNext(requestBody)) {
      ClientRequestMsg req((ClientRequestMsgHeader*)requestBody);
      if ((idx == swapIdx) || (idx == (swapIdx + 1))) {
        if (req.requestLength() > 0) {
          memcpy(req.requestBuf(),
                 StrategyUtils::getRandomStringOfLength(req.requestLength()).c_str(),
                 req.requestLength());
        }
        auto cloned_req = std::make_unique<char[]>(req.size());
        memcpy(cloned_req.get(), req.body(), req.size());
        clientMsgs.push_back(std::make_tuple(req.body(), req.size(), std::move(cloned_req)));
      }
      char* sig = req.requestSignature();
      if (sig != nullptr) {
        if (idx == swapIdx) {
          sigOrDigestOfRequest[idx + 1].append(sig, req.requestSignatureLength());
        } else if (idx == (swapIdx + 1)) {
          sigOrDigestOfRequest[idx - 1].append(sig, req.requestSignatureLength());
        } else {
          sigOrDigestOfRequest[idx].append(sig, req.requestSignatureLength());
        }
      } else {
        Digest d;
        DigestUtil::compute(req.body(), req.size(), reinterpret_cast<char*>(&d), sizeof(Digest));
        if (idx == swapIdx) {
          sigOrDigestOfRequest[idx + 1].append(d.content(), sizeof(Digest));
        } else if (idx == (swapIdx + 1)) {
          sigOrDigestOfRequest[idx - 1].append(d.content(), sizeof(Digest));
        } else {
          sigOrDigestOfRequest[idx].append(d.content(), sizeof(Digest));
        }
      }
      idx++;
    }
    memcpy(static_cast<void*>(std::get<0>(clientMsgs[0])),
           static_cast<void*>(std::get<2>(clientMsgs[1]).get()),
           std::get<1>(clientMsgs[1]));
    memcpy(static_cast<void*>(std::get<0>(clientMsgs[0]) + std::get<1>(clientMsgs[1])),
           static_cast<void*>(std::get<2>(clientMsgs[0]).get()),
           std::get<1>(clientMsgs[0]));

    std::string sigOrDig;
    for (const auto& sod : sigOrDigestOfRequest) {
      sigOrDig.append(sod);
    }

    Digest d;
    DigestUtil::compute(sigOrDig.c_str(), sigOrDig.size(), reinterpret_cast<char*>(&d), sizeof(Digest));
    nmsg.digestOfRequests() = d;
    LOG_INFO(logger_,
             "Finally the PrePrepare Message with correlation id : "
                 << nmsg.getCid() << " of seq num : " << nmsg.seqNumber() << " and number of requests : "
                 << nmsg.numberOfRequests() << " in the view : " << nmsg.viewNumber() << " is changed.");

    return true;
  }
  return false;
}
std::string ShufflePrePrepareMsgStrategy::getStrategyName() { return CLASSNAME(ShufflePrePrepareMsgStrategy); }
uint16_t ShufflePrePrepareMsgStrategy::getMessageCode() { return static_cast<uint16_t>(MsgCode::PrePrepare); }

}  // end of namespace concord::kvbc::strategy
