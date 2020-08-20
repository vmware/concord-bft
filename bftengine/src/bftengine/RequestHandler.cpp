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

void RequestHandler::onFinishExecutingReadWriteRequests() {
  userRequestsHandler_->onFinishExecutingReadWriteRequests();
}

std::shared_ptr<ControlHandlers> RequestHandler::getControlHandlers() {
  return userRequestsHandler_->getControlHandlers();
}

}  // namespace bftEngine
