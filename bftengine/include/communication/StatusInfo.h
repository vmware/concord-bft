// Copyright 2018 VMware, all rights reserved
//
// main hpp file for status related structs

#ifndef STATUSINFO_H
#define STATUSINFO_H

#include <functional>
#include <string>

enum class PeerInfoType {
  Connectivity
};

struct BasePeerStatus {
 public:
  int64_t peerId = 0;
  std::string peerIp;
  int16_t peerPort = 0;
  int64_t statusTime = 0;
};

enum class StatusType {
  Started,
  MessageReceived,
  MessageSent,
  Broken
};
struct PeerConnectivityStatus : public BasePeerStatus {
 public:
  StatusType statusType;
  std::string peerState;
};

typedef std::function<void(PeerConnectivityStatus)> UPDATE_CONNECTIVITY_FN;

#endif //STATUSINFO_H
