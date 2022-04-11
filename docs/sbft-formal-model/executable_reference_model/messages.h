#pragma once

#include "types.h"

namespace Messages {

struct PreparedCertificate;

struct ClientOperation {
  HostId sender;
  nat timestamp;
};
bool operator==(const ClientOperation& rhs, const ClientOperation& lhs);

struct Noop {};
bool operator==(const Noop& rhs, const Noop& lhs);

using OperationWrapper = variant<Noop, ClientOperation>;

struct PrePrepare {
  ViewNum view;
  SequenceID seqID;
  OperationWrapper operationWrapper;
};
bool operator==(const PrePrepare& lhs, const PrePrepare& rhs);
bool operator!=(const PrePrepare& lhs, const PrePrepare& rhs);

struct Prepare {
  ViewNum view;
  SequenceID seqID;
  OperationWrapper operationWrapper;
};
bool operator==(const Prepare& lhs, const Prepare& rhs);
bool operator!=(const Prepare& lhs, const Prepare& rhs);

struct Commit {
  ViewNum view;
  SequenceID seqID;
  OperationWrapper operationWrapper;
};
bool operator==(const Commit& lhs, const Commit& rhs);
bool operator!=(const Commit& lhs, const Commit& rhs);

struct ClientRequest {
  ClientOperation clientOp;
};
bool operator==(const ClientRequest& lhs, const ClientRequest& rhs);
bool operator!=(const ClientRequest& lhs, const ClientRequest& rhs);

struct NetworkMessage;
class Message;

struct PreparedCertificate {
  set<NetworkMessage> votes;
  Message prototype();
  bool WF();
  bool valid(nat quorumSize);
  bool empty();
};
bool operator==(const PreparedCertificate& lhs, const PreparedCertificate& rhs);

struct ViewChangeMsg {
  ViewNum newView;
  map<SequenceID, PreparedCertificate> certificates;
};

class Message : public variant<PrePrepare, Prepare, Commit, ClientRequest, ViewChangeMsg> {};

struct NetworkMessage {
  HostId sender;
  Message payload;
};
bool operator==(const NetworkMessage& lhs, const NetworkMessage& rhs);
bool operator!=(const NetworkMessage& lhs, const NetworkMessage& rhs);

bool operator==(const ViewChangeMsg& lhs, const ViewChangeMsg& rhs);
bool operator!=(const ViewChangeMsg& lhs, const ViewChangeMsg& rhs);

struct ViewChangeMsgsSelectedByPrimary {
  set<NetworkMessage> msgs;
  bool valid(ViewNum view, nat quorumSize);
};

struct NewViewMsg {
  ViewNum newView;
  ViewChangeMsgsSelectedByPrimary vcMsgs;
};
bool operator==(const NewViewMsg& lhs, const NewViewMsg& rhs);

}  // namespace Messages
