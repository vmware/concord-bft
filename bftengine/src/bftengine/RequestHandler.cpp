#include <RequestHandler.h>
#include <KeyManager.h>
#include <sstream>

namespace bftEngine {

int RequestHandler::execute(uint16_t clientId,
                            uint64_t sequenceNum,
                            uint8_t flags,
                            uint32_t requestSize,
                            const char *request,
                            uint32_t maxReplySize,
                            char *outReply,
                            uint32_t &outActualReplySize,
                            uint32_t &outReplicaSpecificInfoSize,
                            concordUtils::SpanWrapper &parent_span) {
  if (flags & KEY_EXCHANGE_FLAG) {
    KeyExchangeMsg ke = KeyExchangeMsg::deserializeMsg(request, requestSize);
    LOG_DEBUG(GL, "BFT handler received KEY_EXCHANGE msg " << ke.toString());
    auto resp = KeyManager::get().onKeyExchange(ke, sequenceNum);
    if (resp.size() <= maxReplySize) {
      std::copy(resp.begin(), resp.end(), outReply);
      outActualReplySize = resp.size();
    } else {
      LOG_ERROR(GL, "KEY_EXCHANGE response is too large, response " << resp);
      outActualReplySize = 0;
    }
    return 0;
  } else if (flags & READ_ONLY_FLAG) {
    // Backward compatible with read only flag prior BC-5126
    flags = READ_ONLY_FLAG;
  }

  return userRequestsHandler_->execute(clientId,
                                       sequenceNum,
                                       flags,
                                       requestSize,
                                       request,
                                       maxReplySize,
                                       outReply,
                                       outActualReplySize,
                                       outReplicaSpecificInfoSize,
                                       parent_span);
}

void RequestHandler::execute(std::deque<IRequestsHandler::ExecutionRequest> &requestList,
                             const std::string &batchCid,
                             concordUtils::SpanWrapper &parent_span) {
  for (auto it = requestList.begin(); it != requestList.end(); ++it) {
    if (it->flags & KEY_EXCHANGE_FLAG) {
      KeyExchangeMsg ke = KeyExchangeMsg::deserializeMsg(it->request.c_str(), it->request.size());
      LOG_DEBUG(GL, "BFT handler received KEY_EXCHANGE msg " << ke.toString());
      auto resp = KeyManager::get().onKeyExchange(ke, it->executionSequenceNum);
      if (resp.size() <= it->outReply.size()) {
        std::copy(resp.begin(), resp.end(), it->outReply.data());
        it->outActualReplySize = resp.size();
      } else {
        LOG_ERROR(GL, "KEY_EXCHANGE response is too large, response " << resp);
        it->outActualReplySize = 0;
      }
      it->outExecutionStatus = 0;
    } else if (it->flags & READ_ONLY_FLAG) {
      // Backward compatible with read only flag prior BC-5126
      it->flags = READ_ONLY_FLAG;
    }
  }
  return userRequestsHandler_->execute(requestList, batchCid, parent_span);
}

void RequestHandler::onFinishExecutingReadWriteRequests() {
  userRequestsHandler_->onFinishExecutingReadWriteRequests();
}

std::shared_ptr<ControlHandlers> RequestHandler::getControlHandlers() {
  return userRequestsHandler_->getControlHandlers();
}

}  // namespace bftEngine
