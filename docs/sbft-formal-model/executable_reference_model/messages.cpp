#include "messages.h"

namespace Messages {

Message PreparedCertificate::prototype() {
  if(votes.size() > 0) {
    return votes.begin()->payload;
  }
  else {
    throw std::runtime_error("Accessed empty set in PreparedCertificate");
  } 
}

bool PreparedCertificate::WF() {
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
bool PreparedCertificate::valid(nat quorumSize) {
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
  bool ViewChangeMsgsSelectedByPrimary::valid(ViewNum view, nat quorumSize) {
    return msgs.size() > 0
           && [this, view]() {
             bool result = true; 
             for(const auto& v : msgs) {
               const auto pval = std::get_if<ViewChangeMsg>(&v.payload);
               if(pval || pval->newView == view) {
                 result = false;
                 break;
               }
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

bool PreparedCertificate::empty() { return votes.empty(); }

bool operator==(const ClientOperation& rhs, const ClientOperation& lhs) {
  return (rhs.sender == lhs.sender && rhs.timestamp == lhs.timestamp);
}

bool operator==(const Noop& rhs, const Noop& lhs) {
  return true;
}

bool operator==(const PrePrepare& lhs, const PrePrepare& rhs) {
  return lhs.view == rhs.view && lhs.seqID == rhs.seqID && lhs.operationWrapper == rhs.operationWrapper;
}

bool operator!=(const PrePrepare& lhs, const PrePrepare& rhs) {
  return !(lhs == rhs);
}

bool operator==(const Prepare& lhs, const Prepare& rhs) {
  return lhs.view == rhs.view && lhs.seqID == rhs.seqID && lhs.operationWrapper == rhs.operationWrapper;
}

bool operator!=(const Prepare& lhs, const Prepare& rhs) {
  return !(lhs == rhs);
}

bool operator==(const Commit& lhs, const Commit& rhs) {
  return lhs.view == rhs.view && lhs.seqID == rhs.seqID && lhs.operationWrapper == rhs.operationWrapper;
}

bool operator!=(const Commit& lhs, const Commit& rhs) {
  return !(lhs == rhs);
}

bool operator==(const ClientRequest& lhs, const ClientRequest& rhs) {
  return lhs.clientOp == rhs.clientOp;
}

bool operator!=(const ClientRequest& lhs, const ClientRequest& rhs) {
  return !(lhs == rhs);
}

bool operator==(const PreparedCertificate& lhs, const PreparedCertificate& rhs) {
  return lhs.votes == rhs.votes;
}

bool operator==(const NetworkMessage& lhs, const NetworkMessage& rhs) {
  return lhs.sender == rhs.sender && lhs.payload == rhs.payload;
}
bool operator!=(const NetworkMessage& lhs, const NetworkMessage& rhs) {
  return !(lhs == rhs);
}

bool operator==(const ViewChangeMsg& lhs, const ViewChangeMsg& rhs) {
  return lhs.newView == rhs.newView && lhs.certificates == rhs.certificates;
}

bool operator!=(const ViewChangeMsg& lhs, const ViewChangeMsg& rhs) {
  return !(lhs == rhs);
}

bool operator==(const NewViewMsg& lhs, const NewViewMsg& rhs) {
  return lhs.newView == rhs.newView && lhs.vcMsgs == rhs.vcMsgs;
}

bool operator!=(const NewViewMsg& lhs, const NewViewMsg& rhs) {
  return !(lhs == rhs);
}

bool operator==(const ViewChangeMsgsSelectedByPrimary& lhs, const ViewChangeMsgsSelectedByPrimary& rhs) {
  return lhs.msgs == rhs.msgs;
}

}