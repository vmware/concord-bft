#include <Replica.hpp>

#pragma once

namespace bftEngine {

class RequestHandler : public IRequestsHandler {
 public:
  RequestHandler(IRequestsHandler *userHdlr) : userRequestsHandler_(userHdlr) {}

  virtual void execute(ExecutionRequestsQueue &requests,
                       const std::string &batchCid,
                       concordUtils::SpanWrapper &parent_span) override;

  virtual void onFinishExecutingReadWriteRequests() override;

  virtual std::shared_ptr<ControlHandlers> getControlHandlers() override;

 private:
  IRequestsHandler *const userRequestsHandler_ = nullptr;
};

}  // namespace bftEngine
