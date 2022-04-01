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

#include "MangledPreProcessResultMsgStrategy.hpp"
#include "StrategyUtils.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/PreProcessResultMsg.hpp"
#include "digest.hpp"
namespace concord::kvbc::strategy {

using bftEngine::impl::MessageBase;
using bftEngine::impl::PrePrepareMsg;
using bftEngine::ClientRequestMsgHeader;
using preprocessor::PreProcessResultMsg;
using concord::util::digest::DigestGenerator;

std::string concord::kvbc::strategy::MangledPreProcessResultMsgStrategy::getStrategyName() {
  return CLASSNAME(MangledPreProcessResultMsgStrategy);
}
uint16_t concord::kvbc::strategy::MangledPreProcessResultMsgStrategy::getMessageCode() { return MsgCode::PrePrepare; }

// Below function provides the strategy for changing of the message.
// It mangles all the PreProcessResult message by filling it with
// some ramdom string
// Update the digest and then send the mangled message.
bool concord::kvbc::strategy::MangledPreProcessResultMsgStrategy::changeMessage(std::shared_ptr<MessageBase>& msg) {
  PrePrepareMsg& nmsg = static_cast<PrePrepareMsg&>(*(msg.get()));

  std::vector<std::string> sigOrDigestOfRequest(nmsg.numberOfRequests());
  size_t idx = 0;
  auto it = RequestsIterator(&nmsg);
  char* requestBody = nullptr;
  bool ischanged = false;
  while (it.getAndGoToNext(requestBody)) {
    PreProcessResultMsg req((ClientRequestMsgHeader*)requestBody);
    if (req.type() == MsgCode::PreProcessResult) {
      if (req.requestLength() > 0) {
        memcpy(
            req.requestBuf(), StrategyUtils::getRandomStringOfLength(req.requestLength()).c_str(), req.requestLength());
        ischanged = true;
      }
    }
    char* sig = req.requestSignature();
    if (sig != nullptr) {
      sigOrDigestOfRequest[idx].append(sig, req.requestSignatureLength());
    } else {
      Digest d;
      DigestGenerator digestGenerator;
      digestGenerator.compute(req.body(), req.size(), reinterpret_cast<char*>(&d), sizeof(Digest));
      sigOrDigestOfRequest[idx].append(d.content(), sizeof(Digest));
    }
    idx++;
  }

  if (ischanged) {
    std::string sigOrDig;
    for (const auto& sod : sigOrDigestOfRequest) {
      sigOrDig.append(sod);
    }

    Digest d;
    DigestGenerator digestGenerator;
    digestGenerator.compute(sigOrDig.c_str(), sigOrDig.size(), reinterpret_cast<char*>(&d), sizeof(Digest));
    nmsg.digestOfRequests() = d;
    LOG_INFO(logger_,
             "Finally the PrePrepare Message with correlation id : "
                 << nmsg.getCid() << " of seq num : " << nmsg.seqNumber() << " and number of requests : "
                 << nmsg.numberOfRequests() << " in the view : " << nmsg.viewNumber() << " is changed.");
  }

  return ischanged;
}

}  // namespace concord::kvbc::strategy