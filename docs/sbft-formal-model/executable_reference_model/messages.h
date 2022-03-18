#pragma once

#include "types.h"

namespace Messages {

struct PreparedCertificate;

struct ClientOperation {
  HostId sender;
  nat timestamp;
};
bool operator==(const ClientOperation& rhs, const ClientOperation& lhs) {
  return (rhs.sender == lhs.sender && rhs.timestamp == lhs.timestamp);
}

struct Noop {};
bool operator==(const Noop& rhs, const Noop& lhs) {
  return true;
}

using OperationWrapper = variant<Noop, ClientOperation>;

struct PrePrepare {
  ViewNum view;
  SequenceID seqID;
  OperationWrapper operationWrapper;
};
bool operator==(const PrePrepare& lhs, const PrePrepare& rhs) {
  return lhs.view == rhs.view && lhs.seqID == rhs.seqID && lhs.operationWrapper == rhs.operationWrapper;
}
bool operator!=(const PrePrepare& lhs, const PrePrepare& rhs) {
  return !(lhs == rhs);
}

struct Prepare {
  ViewNum view;
  SequenceID seqID;
  OperationWrapper operationWrapper;
};
bool operator==(const Prepare& lhs, const Prepare& rhs) {
  return lhs.view == rhs.view && lhs.seqID == rhs.seqID && lhs.operationWrapper == rhs.operationWrapper;
}
bool operator!=(const Prepare& lhs, const Prepare& rhs) {
  return !(lhs == rhs);
}

struct Commit {
  ViewNum view;
  SequenceID seqID;
  OperationWrapper operationWrapper;
};
bool operator==(const Commit& lhs, const Commit& rhs) {
  return lhs.view == rhs.view && lhs.seqID == rhs.seqID && lhs.operationWrapper == rhs.operationWrapper;
}
bool operator!=(const Commit& lhs, const Commit& rhs) {
  return !(lhs == rhs);
}

struct ClientRequest {
  ClientOperation clientOp;
};
bool operator==(const ClientRequest& lhs, const ClientRequest& rhs) {
  return lhs.clientOp == rhs.clientOp;
}
bool operator!=(const ClientRequest& lhs, const ClientRequest& rhs) {
  return !(lhs == rhs);
}

using Message = variant<PrePrepare, Prepare, Commit, ClientRequest/*, ViewChangeMsg*/>;

struct NetworkMessage {
  HostId sender;
  Message payload;
};
bool operator==(const NetworkMessage& lhs, const NetworkMessage& rhs) {
  return lhs.sender == rhs.sender && lhs.payload == rhs.payload;
}
bool operator!=(const NetworkMessage& lhs, const NetworkMessage& rhs) {
  return !(lhs == rhs);
}

struct PreparedCertificate {
  set<NetworkMessage> votes;
  Message prototype() {
    if(votes.size() > 0) {
      return votes.begin()->payload;
    }
    else {
      throw std::runtime_error("Accessed empty set in PreparedCertificate");
    } 
  }
  bool WF() {
    bool wf = true;
    for(const auto& i : votes) {
      const auto pval = std::get_if<Prepare>(&i.payload);
      if(!pval) {
        wf = false;
        break;
      }
    }
    return wf;
  }
  bool valid(nat quorumSize) {
    return empty() ||
           (votes.size() == quorumSize 
            && WF()
            && [this](){ 
              bool result = true; 
              for(const auto& v : votes) 
              {
                if(v.payload != prototype()) {
                  result = false;
                }
              }
              return result;
            }());
  }
  bool empty() { return votes.empty(); }
};
bool operator==(const PreparedCertificate& lhs, const PreparedCertificate& rhs) {
  return lhs.votes == rhs.votes;
}

struct ViewChangeMsg {
  ViewNum newView;
  map<SequenceID, PreparedCertificate> certificates;
};
bool operator==(const ViewChangeMsg& lhs, const ViewChangeMsg& rhs) {
  return lhs.newView == rhs.newView && lhs.certificates == rhs.certificates;
}
bool operator!=(const ViewChangeMsg& lhs, const ViewChangeMsg& rhs) {
  return !(lhs == rhs);
}

struct ViewChangeMsgsSelectedByPrimary {
  set<NetworkMessage> msgs;
  bool valid(ViewNum view, nat quorumSize) {
    return msgs.size() > 0
           && [this, view]() {
             bool result = true; 
             for(const auto& v : msgs) {
              //  const auto pval = std::get_if<ViewChangeMsg>(&v.payload);
              //  if(pval || pval->newView == view) {
              //    result = false;
              //    break;
              //  }
             }
             return result;
           }()
           && [this, quorumSize]() {
             set<HostId> quorumOfSenders;
             for(auto& v : msgs) {
               quorumOfSenders.insert(v.sender);
             }
             return quorumOfSenders.size() == quorumSize;
           }()
           && msgs.size() == quorumSize;
  }
};

struct NewViewMsg {};
bool operator==(const NewViewMsg& lhs, const NewViewMsg& rhs) {
  return true; // TODO: implement New View msg
}

}  // namespace Messages