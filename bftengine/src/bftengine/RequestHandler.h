#include <Replica.hpp>

#pragma once

namespace bftEngine {

class RequestHandler : public IRequestsHandler {
 public:
  RequestHandler(IRequestsHandler *userHdlr) : userRequestsHandler_(userHdlr) {}
  virtual int execute(uint16_t clientId,
                      uint64_t sequenceNum,
                      uint8_t flags,
                      uint32_t requestSize,
                      const char *request,
                      uint32_t maxReplySize,
                      char *outReply,
                      uint32_t &outActualReplySize,
                      uint32_t &outReplicaSpecificInfoSize,
                      concordUtils::SpanWrapper &parent_span) override;

  virtual void execute(std::deque<ExecutionRequest> &requestList,
                       const std::string &batchCid,
                       concordUtils::SpanWrapper &parent_span) override;

  virtual void onFinishExecutingReadWriteRequests() override;

  virtual std::shared_ptr<ControlHandlers> getControlHandlers() override;

 private:
  IRequestsHandler *const userRequestsHandler_ = nullptr;
};

}  // namespace bftEngine
