#pragma once

#include <utt/Comm.h>
#include <utt/IBE.h>
#include <utt/PolyCrypto.h>
#include <utt/RandSig.h>

namespace libutt {
class AddrSK;
class RegAuthPK;
}  // namespace libutt

std::ostream& operator<<(std::ostream& out, const libutt::RegAuthPK& rpk);
std::istream& operator>>(std::istream& in, libutt::RegAuthPK& rpk);

namespace libutt {

class RegAuthPK {
 public:
  RandSigPK vk;  // re-randomizable signature PK, needed to verify registration commitments

  IBE::MPK mpk;  // IBE MPK, need to derive encryption PKs for users by TXN creators

 public:
  bool operator==(const RegAuthPK& o) const { return vk == o.vk && mpk == o.mpk; }

  bool operator!=(const RegAuthPK& o) const { return !operator==(o); }
};

class RegAuthSK {
 public:
  CommKey ck_reg;  // registration commitment key, needed to create registration commitments
  RandSigSK sk;    // re-randomizable signature SK, needed to sign registration commitments

  IBE::Params p;
  IBE::MSK msk;  // IBE MSK, needed to issue encryption SKs to users

 public:
  RegAuthSK() {}

 public:
  static RegAuthSK random(const CommKey& ck_reg, const IBE::Params& p);

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

}  // end of namespace libutt
