#pragma once

#include <utt/Comm.h>
#include <utt/IBE.h>
#include <utt/PolyCrypto.h>
#include <utt/RandSig.h>

namespace libutt {
class AddrSK;
class RegAuthPK;
class RegAuthSharePK;
class RegAuthShareSK;
}  // namespace libutt

std::ostream& operator<<(std::ostream& out, const libutt::RegAuthPK& rpk);
std::istream& operator>>(std::istream& in, libutt::RegAuthPK& rpk);

std::ostream& operator<<(std::ostream& out, const libutt::RegAuthSharePK& rpk);
std::istream& operator>>(std::istream& in, libutt::RegAuthSharePK& rpk);

std::ostream& operator<<(std::ostream& out, const libutt::RegAuthShareSK& rsk);
std::istream& operator>>(std::istream& in, libutt::RegAuthShareSK& rsk);
namespace libutt {

class RegAuthPK {
 public:
  RandSigPK vk;  // re-randomizable signature PK, needed to verify registration commitments

  IBE::MPK mpk;  // IBE MPK, need to derive encryption PKs for users by TXN creators

 public:
  bool operator==(const RegAuthPK& o) const { return vk == o.vk && mpk == o.mpk; }

  bool operator!=(const RegAuthPK& o) const { return !operator==(o); }
};

class RegAuthSharePK {
 public:
  RandSigSharePK vk;  // re-randomizable signature PK, needed to verify registration commitments

  IBE::MPK mpk;  // IBE MPK, need to derive encryption PKs for users by TXN creators

 public:
  bool operator==(const RegAuthSharePK& o) const { return vk == o.vk && mpk == o.mpk; }

  bool operator!=(const RegAuthSharePK& o) const { return !operator==(o); }
};

class RegAuthSK {
 public:
  CommKey ck_reg;  // registration commitment key, needed to create registration commitments
  RandSigSK sk;    // re-randomizable signature SK, needed to sign registration commitments

  IBE::Params p;
  IBE::MSK msk;  // IBE MSK, needed to issue encryption SKs to users
  std::vector<RegAuthShareSK> shares;

 public:
  RegAuthSK() {}

 public:
  static RegAuthSK random(const CommKey& ck_reg, const IBE::Params& p);
  static RegAuthSK generateKeyAndShares(CommKey& baseComm, size_t t, size_t n, const IBE::Params& p);

 public:
  // AddrSK randomUser() const;
  AddrSK registerRandomUser(const std::string& pid) const;

  RegAuthPK toPK() const {
    RegAuthPK pk;
    pk.vk = sk.toPK();
    pk.mpk = msk.toMPK(p);
    return pk;
  }
};

class RegAuthShareSK {
 public:
  CommKey ck_reg;     // registration commitment key, needed to create registration commitments
  RandSigShareSK sk;  // re-randomizable signature shared SK, needed to sign registration commitments

  IBE::Params p;
  IBE::MSK msk;  // IBE MSK, needed to issue encryption SKs to users

 public:
  RegAuthShareSK() {}

  RegAuthSharePK toPK() const {
    RegAuthSharePK pk;
    pk.vk = sk.toPK();
    pk.mpk = msk.toMPK(p);
    return pk;
  }
};

}  // end of namespace libutt
