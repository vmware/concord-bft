#pragma once
#include "status.hpp"
#include <chrono>
#include <string>

namespace bftEngine {

class IBasicClient {
 public:
  virtual ~IBasicClient() = default;
  virtual concordUtils::Status invokeCommandSynch(const char* request,
                                                  uint32_t requestSize,
                                                  uint8_t flags,
                                                  std::chrono::milliseconds timeout,
                                                  uint32_t replySize,
                                                  char* outReply,
                                                  uint32_t* outActualReplySize,
                                                  const std::string& cid = "",
                                                  const std::string& span_context = "") = 0;
};

}  // namespace bftEngine