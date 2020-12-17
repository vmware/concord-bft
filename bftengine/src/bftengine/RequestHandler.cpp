#include <RequestHandler.h>
#include <KeyManager.h>
#include <sstream>

namespace bftEngine {

void RequestHandler::execute(IRequestsHandler::ExecutionRequestsQueue &requests,
                             const std::string &batchCid,
                             concordUtils::SpanWrapper &parent_span) {
  for (auto &req : requests) {
    if (req.flags & KEY_EXCHANGE_FLAG) {
      KeyExchangeMsg ke = KeyExchangeMsg::deserializeMsg(req.request, req.requestSize);
      LOG_DEBUG(GL, "BFT handler received KEY_EXCHANGE msg " << ke.toString());
      auto resp = KeyManager::get().onKeyExchange(ke, req.executionSequenceNum);
      if (resp.size() <= req.outReply.size()) {
        std::copy(resp.begin(), resp.end(), req.outReply.data());
        req.outActualReplySize = resp.size();
      } else {
        LOG_ERROR(GL, "KEY_EXCHANGE response is too large, response " << resp);
        req.outActualReplySize = 0;
      }
      req.outExecutionStatus = 0;
    } else if (req.flags & READ_ONLY_FLAG) {
      // Backward compatible with read only flag prior BC-5126
      req.flags = READ_ONLY_FLAG;
    }
  }
  return userRequestsHandler_->execute(requests, batchCid, parent_span);
}

void RequestHandler::onFinishExecutingReadWriteRequests() {
  userRequestsHandler_->onFinishExecutingReadWriteRequests();
}

std::shared_ptr<ControlHandlers> RequestHandler::getControlHandlers() {
  return userRequestsHandler_->getControlHandlers();
}

}  // namespace bftEngine
